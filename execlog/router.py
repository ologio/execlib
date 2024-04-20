import time
import asyncio
import logging
import inspect
import traceback 
import threading
from pathlib import Path
from typing import Callable
from functools import partial
from colorama import Fore, Style
from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


Event = namedtuple(
    'Event',
    ['endpoint', 'name', 'action'],
    defaults=[None, None, None],
)

class Router: 
    '''
    Route events to registered callbacks

    Generalized registration includes an endpoint (the origin of an event), a pattern (to
    filter events at the endpoint), and a callback (to be executed if pattern is matched).

    The Router _routes_ events to affiliated callbacks in a multi-threaded fashion. A
    thread pool handles these jobs as events are submitted, typically by a composing
    Listener. The Listener "hears" an event, and passes it on through to a Router to
    further filter and delegate any matching follow-up jobs.

    This base Router implements most of the registry and filter model. When events are
    submitted for propagation, they are checked for matching routes. Events specify an
    origin endpoint, which is used as the filter for attached routes. The event is then
    subjected to the `filter` method, which checks if the event matches the registered
    `pattern` under the originated `endpoint`. If so, the callback is scheduled for
    execution, and the matching event is passed as its sole argument.

    Subclasses are expected to implement (at least) the `filter` method. This function is
    responsible for wrapping up the task-specific logic needed to determine if an event,
    originating from a known endpoint, matches the callback-specific pattern. This method
    needn't handle any other filter logic, like checking if the event originates from the
    provided endpoint, as this is already handled by the outer look in `matching_routes`.

    `get_listener` is a convenience method that instantiates and populates an affiliated
    Listener over the register paths found in the Router. Listeners require a Router upon
    instantiation so events can be propagated to available targets when they occur.
    `get_listener()` is the recommended way to attain a Listener.

    Note: on debouncing events
        Previously, debouncing was handled by listeners. This logic has been generalized
        and moved to this class, as it's general enough to be desired across various
        Listener types. We also need unique, identifying info only available with a
        `(endpoint, callback, pattern)` triple in order to debounce events in accordance
        with their intended target.

    Note: tracking events and serializing callback frames
        Although not part of the original implementation, we now track which events have a
        callback chain actively being executed, and prevent the same chain from being
        started concurrently. If the callback chain is actively running for an event, and
        that same event is submitted before this chain finishes, the request is simply
        enqueued. The `clear_event` method is attached as a "done callback" to each job
        future, and will re-submit the event once the active chain finishes.

        While this could be interpreted as a harsh design choice, it helps prevent many
        many thread conflicts (race conditions, writing to the same resources, etc) when
        the same function is executed concurrently, many times over. Without waiting
        completely for an event to be fully handled, later jobs may complete before
        earlier ones, or interact with intermediate disk states (raw file writes, DB
        inserts, etc), before the earliest call has had a chance to clean up.
    '''
    def __init__(self, loop=None, workers=None, listener_cls=None):
        '''
        Parameters:
            loop:
            workers:
            listener_cls:
        '''
        self.loop         = loop
        self.workers      = workers
        self.listener_cls = listener_cls

        self.routemap        : dict[str, list[tuple]] = defaultdict(list)
        self.post_callbacks = []

        # track running jobs by event
        self.running_events = defaultdict(set)

        # debounce tracker
        self.next_allowed_time = defaultdict(int)

        # store prepped (e.g., delayed) callbacks
        self.callback_registry = {}

        self._thread_pool = None
        self._route_lock  = threading.Lock()

    @property
    def thread_pool(self):
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(max_workers=self.workers)
        return self._thread_pool

    def register(
        self,
        endpoint,
        callback,
        pattern,
        debounce=200,
        delay=10,
        **listener_kwargs,
    ):
        '''
        Register a route. To be defined by an inheriting class, typically taking a pattern
        and a callback.

        Note: Listener arguments
            Notice how listener_kwargs are accumulated instead of uniquely assigned to an
            endpoint. This is generally acceptable as some listeners may allow various
            configurations for the same endpoint. Note, however, for something like the
            PathListener, this will have no effect. Registering the same endpoint multiple
            times won't cause any errors, but the configuration options will only remain
            for the last registered group.

            (Update) The above remark about PathListener's is no longer, and likely never
            was. Varying flag sets under the same endpoint do in fact have a cumulative
            effect, and we need to be able disentangle events accordingly through
            submitted event's `action` value.

        Parameters:
            pattern: hashable object to be used when filtering event (passed to inherited
                     `filter(...)`)
            callback: callable accepting an event to be executed if when a matching event
                      is received
        '''
        route_tuple = (callback, pattern, debounce, delay, listener_kwargs)
        self.routemap[endpoint].append(route_tuple)

    def submit(self, events, callbacks=None):
        '''
        Handle a list of events. Each event is matched against the registered callbacks,
        and those callbacks are ran concurrently (be it via a thread pool or an asyncio
        loop).
        '''
        if type(events) is not list:
            events = [events]

        futures = []
        for event in events:
            future = self.submit_callback(self.submit_event, event, callbacks=callbacks)
            future.add_done_callback(lambda f: self.clear_event(event, f))
            futures.append(future)

        return futures

    def submit_event(self, event, callbacks=None):
        '''
        Group up and submit all matching callbacks for `event`. All callbacks are ran
        concurrently in their own threads, and this method blocks until all are completed.

        In the outer `submit` context, this blocking method is itself ran in its own
        thread, and the registered post-callbacks are attached to the completion of this
        function, i.e., the finishing of all callbacks matching provided event.

        Note that there are no checks for empty callback lists, where we could exit early.
        Here we simply rely on methods doing the right thing: `wait_on_futures` would
        simply receive an empty list, for example. Nevertheless, once an event is
        submitted with this method, it gets at least a few moments where that event is
        considered "running," and will be later popped out by `clear_events` (almost
        immediately if there is in fact nothing to do). An early exit would simply have to
        come after indexing the event in `running_events`
        '''
        if callbacks is None:
            # ensure same thread gets all matching routes & sets debounce updates; else
            # this may be split across threads mid-check, preventing one thread from
            # handling the blocking of the entire group
            with self._route_lock:
                callbacks = self.matching_routes(event)

        # stop early if no work to do
        if len(callbacks) == 0:
            return []

        # enqueue requested/matched callbacks and exit if running
        event_idx = self.event_index(event) 
        if event_idx in self.running_events:
            self.queue_callbacks(event_idx, callbacks)
            return []

        # callbacks now computed, flush the running event
        # note: a separate thread could queue valid callbacks since the running check;
        # o/w we know the running index is empty
        self.running_events[event_idx] = self.running_events[event_idx]

        # submit matching callbacks and wait for them to complete
        future_results = self.wait_on_callbacks(callbacks, event)

        # finally call post event-group callbacks (only if some event callbacks were
        # submitted), wait for them to complete
        if future_results:
            self.wait_on_futures([
                self.submit_callback(post_callback, event, future_results)
                for post_callback in self.post_callbacks
            ])

        return future_results

    def submit_callback(self, callback, *args, **kwargs):
        '''
        Note: this method is expected to return a future. Perform any event-based
        filtering before submitting a callback with this method.
        '''
        if inspect.iscoroutinefunction(callback):
            if self.loop is None:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

            #loop.run_in_executor(executor, loop.create_task, callback(event))
            #future = self.loop.call_soon_threadsafe(
            #    self.loop.create_task,
            future = asyncio.run_coroutine_threadsafe(
                callback(*args, **kwargs),
                self.loop,
            )
        else:
            future = self.thread_pool.submit(
                callback, *args, **kwargs
            )
            future.add_done_callback(handle_exception)

        return future

    def matching_routes(self, event, event_time=None):
        '''
        Return eligible matching routes for the provided event.

        Note that we wait as late as possible before enqueuing matches if the event is in
        fact already active in a frame. If this method were start filtering results while
        the frame is active, and the frame were to finish before all matching callbacks
        were determined, we would be perfectly happy to return all matches, and allow the
        outer `submit_event` context to run them right away in a newly constructed frame.
        The _very_ next thing that gets done is adding this event to the active event
        tracker. Otherwise, matching is performed as usual, and eligible callbacks are
        simply enqueued for the next event frame, which will be checked in the "done"
        callback of the active frame. The logic here should mostly "seal up" any real
        opportunities for error, e.g., a frame ending and popping off elements from
        `running_events` half-way through their inserting at the end of this method, or
        multiple threads checking for matching routes for the same event, and both coming
        away with a non-empty set of matches to run. That last example highlights
        precisely how the single event-frame model works: many threads might be running
        this method at the same time, for the same event (which has fired rapidly), but
        only one should be able to "secure the frame" and begin running the matching
        callbacks. Making the "active frame check" both as late as possible and as close
        to the event blocking stage in the tracker (in `submit_event`), we make the
        ambiguity gap as small as possible (and almost certainly smaller than any
        realistic I/O-bound event duplication).

        Note: on event actions
            The debounce reset is now only set if the event is successfully filtered. This
            allows some middle ground when trying to depend on event actions: if the
            action passes through, we block the whole range of actions until the debounce
            window completes. Otherwise, the event remains open, only to be blocked by the
            debounce on the first matching action.
        '''
        matches    = []
        endpoint   = event.endpoint
        name       = event.name
        #action     = tuple(event.action) # should be more general
        event_time = time.time()*1000 if event_time is None else event_time

        for (callback, pattern, debounce, delay, listen_kwargs) in self.routemap[endpoint]:
            #index = (endpoint, name, action, callback, pattern, debounce, delay)
            index = (endpoint, name, callback, pattern, debounce, delay)

            if event_time < self.next_allowed_time[index]:
                # reject event
                continue

            if self.filter(event, pattern, **listen_kwargs):
                # note that delayed callbacks are added
                matches.append(self.get_delayed_callback(callback, delay, index))

                # set next debounce 
                self.next_allowed_time[index] = event_time + debounce

                match_text = Style.BRIGHT + Fore.GREEN + 'matched'

                callback_name = str(callback)
                if hasattr(callback, '__name__'):
                    callback_name = callback.__name__

                logger.info(
                    f'Event [{name}] {match_text} [{pattern}] under [{endpoint}] for [{callback_name}]'
                )
            else:
                match_text = Style.BRIGHT + Fore.RED + 'rejected'
                logger.debug(
                    f'Event [{name}] {match_text} against [{pattern}] under [{endpoint}] for [{callback.__name__}]'
                )

        return matches

    def get_delayed_callback(self, callback, delay, index):
        '''
        Parameters:
            callback: function to wrap  
            delay: delay in ms
        '''
        if index not in self.callback_registry:
            async def async_wrap(callback, *args, **kwargs):
                await asyncio.sleep(delay/1000)
                return await callback(*args, **kwargs) 

            def sync_wrap(callback, *args, **kwargs):
                time.sleep(delay/1000)
                return callback(*args, **kwargs) 

            wrapper = None
            if inspect.iscoroutinefunction(callback): wrapper = async_wrap
            else:                                     wrapper = sync_wrap

            self.callback_registry[index] = partial(wrapper, callback)

        return self.callback_registry[index]

    def wait_on_futures(self, futures):
        '''
        Block until all futures in `futures` are complete. Return collected results as a
        list, and log warnings when a future fails.
        '''
        future_results = []
        for future in as_completed(futures):
            try:
                future_results.append(future.result())
            except Exception as e:
                logger.warning(f"Router callback job failed with exception {e}")

        return future_results

    def wait_on_callbacks(self, callbacks, event, *args, **kwargs):
        '''
        Overridable by inheriting classes based on callback structure
        '''
        return self.wait_on_futures([
            self.submit_callback(callback, event, *args, **kwargs)
            for callback in callbacks
        ])

    def queue_callbacks(self, event_idx, callbacks):
        '''
        Overridable by inheriting classes based on callback structure
        '''
        self.running_events[event_idx].update(callbacks)

    def filter(self, event, pattern, **listen_kwargs) -> bool:
        '''
        Parameters:
            listen_kwargs_list: 
        '''
        raise NotImplementedError

    def add_post_callback(self, callback: Callable):
        self.post_callbacks.append(callback)

    def get_listener(self, listener_cls=None):
        '''
        Create a new Listener to manage watched routes and their callbacks.
        '''
        if listener_cls is None:
            listener_cls = self.listener_cls
        if listener_cls is None:
            raise ValueError('No Listener class provided')

        listener = listener_cls(self)
        return self.extend_listener(listener)

    def extend_listener(self, listener):
        '''
        Extend a provided Listener object with the Router instance's `listener_kwargs`.
        '''
        for endpoint, route_tuples in self.routemap.items():
            for route_tuple in route_tuples:
                listen_kwargs = route_tuple[-1]
                listener.listen(endpoint, **listen_kwargs)
        return listener

    def stop_event(self, event):
        '''
        Pop event out of the running events tracker and return it.
        '''
        event_idx = self.event_index(event)
        return self.running_events.pop(event_idx, None)

    def clear_event(self, event, future):
        '''
        Clear an event. Pops the passed event out of `running_events`, and the request
        counter is >0, the event is re-submitted.

        This method is attached as a "done" callback to the main event wrapping job
        `submit_event`. The `future` given to this method is one to which it was
        attached as this "done" callback. This method should only be called when that
        `future` is finished running (or failed). If any jobs were submitted in the
        wrapper task, the future results here should be non-empty. We use this fact to
        filter out non-work threads that call this method. Because even the
        `matching_routes` check is threaded, we can't wait to see an event has no work to
        schedule, and thus can't prevent this method being attached as a "done" callback.
        The check for results from the passed future allows us to know when in fact a
        valid frame has finished, and a resubmission may be on the table.
        '''
        if not future.result(): return
        queued_callbacks = self.stop_event(event)

        # resubmit event if some queued work
        if queued_callbacks and len(queued_callbacks) > 0:
            logger.debug(
                f'Event [{event.name}] resubmitted with [{len(queued_callbacks)}] queued callbacks'
            )
            self.submit(event, callbacks=queued_callbacks)

    def event_index(self, event):
        return event[:2]


class ChainRouter(Router):
    '''
    Routes events to registered callbacks
    '''
    def __init__(self, ordered_routers):
        super().__init__()

        self.ordered_routers = []
        for router in ordered_routers:
            self.add_router(router)

        self.running_events = defaultdict(lambda: defaultdict(set))

    def add_router(self, router):
        '''
        TODO: allow positional insertion in ordered list
        
        Note: the `routemap` extensions here shouldn't be necessary, since 1) route maps
        show up only in `matching_routes`, and 2) `matching_routes` is only invoked in
        `submit_event`, which is totally overwritten for the ChainRouter type. All events
        are routed through to individual Routers, and which point their route maps are
        used.
        '''
        self.ordered_routers.append(router)
        for endpoint, routelist in router.routemap.items():
            self.routemap[endpoint].extend(routelist)

    def matching_routes(self, event, event_time=None):
        '''
        Colloquial `callbacks` now used as a dict of lists of callbacks, indexed by
        router, and only having keys for routers with non-empty callback lists.
        '''
        if event_time is None:
            event_time = time.time()*1000

        route_map = {}
        for router in self.ordered_routers:
            router_matches = router.matching_routes(event, event_time)
            if router_matches:
                route_map[router] = router_matches

        return route_map

    def wait_on_callbacks(self, callbacks, event, *args, **kwargs):
        '''
        Note: relies on order of callbacks dict matching that of `ordered_routers`, which
        should happen in `matching_routes`
        '''
        results = {}
        for router, callback_list in callbacks.items():
            router_results = router.submit_event(event, callbacks=callback_list)
            results[router] = router_results

        return results

    def queue_callbacks(self, event_idx, callbacks):
        for router, callback_list in callbacks.items():
            self.running_events[event_idx][router].update(callback_list)

    def stop_event(self, event):
        '''
        Sub-routers do not get a "done" callback for their `submit_event` jobs, as they
        would if they handled their own event submissions. They will, however, set the
        submitted event as "running." We can't rely on sub-routers' "done" callbacks to
        "unset" the running event, because the disconnect between the thread completing
        and execution of that callback may take too long. 

        Instead, we explicitly unset the running event for each of the constituent
        sub-routers at the _same time_ we handle the ChainRouter's notion of event's
        ending.
        '''
        event_idx = self.event_index(event)
        for router in self.ordered_routers:
            rq_callbacks = router.running_events.pop(event_idx, [])
            assert len(rq_callbacks) == 0

        return self.running_events.pop(event_idx, None)

    def get_listener(self, listener_cls=None):
        if listener_cls is None:
            for router in self.ordered_routers:
                if router.listener_cls is not None:
                    listener_cls = router.listener_cls
                    break

        listener = super().get_listener(listener_cls)
        for router in self.ordered_routers:
            router.extend_listener(listener)
        return listener


def handle_exception(future):
    try:
        future.result()
    except Exception as e:
        print(f"Exception occurred: {e}")
        traceback.print_exc()
