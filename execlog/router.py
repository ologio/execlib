'''
Router
'''
import time
import asyncio
import logging
import inspect
import traceback 
import threading
from pathlib import Path
from typing import Any, Callable
from collections import defaultdict
from colorama import Fore, Back, Style
from functools import partial, update_wrapper
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

from tqdm.auto import tqdm

from execlog.event import Event
from execlog.listener import Listener
from execlog.util.generic import color_text


logger = logging.getLogger(__name__)

class Router[E: Event]:
    '''
    Route events to registered callbacks

    .. note::

        Generalized registration includes an endpoint (the origin of an event), a pattern (to
        filter events at the endpoint), and a callback (to be executed if pattern is matched).

        The Router _routes_ events to affiliated callbacks in a multi-threaded fashion. A
        thread pool handles these jobs as events are submitted, typically by a composing
        Listener. The Listener "hears" an event, and passes it on through to a Router to
        further filter and delegate any matching follow-up jobs.

        This base Router implements most of the registry and filter model. When events are
        submitted for propagation, they are checked for matching routes. Events specify an
        origin endpoint, which is used as the filter for attached routes. The event is then
        subjected to the ``filter`` method, which checks if the event matches the registered
        ``pattern`` under the originated ``endpoint``. If so, the callback is scheduled for
        execution, and the matching event is passed as its sole argument.

        Subclasses are expected to implement (at least) the ``filter`` method. This function is
        responsible for wrapping up the task-specific logic needed to determine if an event,
        originating from a known endpoint, matches the callback-specific pattern. This method
        needn't handle any other filter logic, like checking if the event originates from the
        provided endpoint, as this is already handled by the outer look in ``matching_routes``.

        ``get_listener`` is a convenience method that instantiates and populates an affiliated
        Listener over the register paths found in the Router. Listeners require a Router upon
        instantiation so events can be propagated to available targets when they occur.
        ``get_listener()`` is the recommended way to attain a Listener.

    .. admonition:: on debouncing events

        Previously, debouncing was handled by listeners. This logic has been generalized
        and moved to this class, as it's general enough to be desired across various
        Listener types. We also need unique, identifying info only available with a
        ``(endpoint, callback, pattern)`` triple in order to debounce events in accordance
        with their intended target.

    .. admonition:: tracking events and serializing callback frames

        Although not part of the original implementation, we now track which events have a
        callback chain actively being executed, and prevent the same chain from being
        started concurrently. If the callback chain is actively running for an event, and
        that same event is submitted before this chain finishes, the request is simply
        enqueued. The ``clear_event`` method is attached as a "done callback" to each job
        future, and will re-submit the event once the active chain finishes.

        While this could be interpreted as a harsh design choice, it helps prevent many
        many thread conflicts (race conditions, writing to the same resources, etc) when
        the same function is executed concurrently, many times over. Without waiting
        completely for an event to be fully handled, later jobs may complete before
        earlier ones, or interact with intermediate disk states (raw file writes, DB
        inserts, etc), before the earliest call has had a chance to clean up.
    '''
    listener_cls = Listener[E]

    def __init__(self, loop=None, workers=None):
        '''
        Parameters:
            loop:
            workers: number of workers to assign the thread pool when the event loop is
                     started. Defaults to ``None``, which, when passed to
                     ThreadPoolExecutor, will by default use 5x the number of available
                     processors on the machine (which the docs claim is a reasonable
                     assumption given threads are more commonly leveraged for I/O work
                     rather than intense CPU operations). Given the intended context for
                     this class, this assumption aligns appropriately.
        '''
        self.loop         = loop
        self.workers      = workers

        self.routemap : dict[str, list[tuple]] = defaultdict(list)
        self.post_callbacks = []

        # track running jobs by event
        self.running_events = defaultdict(set)

        # debounce tracker
        self.next_allowed_time = defaultdict(int)

        # store prepped (e.g., delayed) callbacks
        self.callback_registry = {}

        # track event history
        self.event_log = []

        # shutdown flag, mostly for callbacks
        self.should_exit = False
        self._active_futures = set()

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
        callback: Callable,
        pattern,
        debounce=200,
        delay=10,
        **listener_kwargs,
    ):
        '''
        Register a route. 

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
            submitted event's ``action`` value.

        Parameters:
            endpoint:
            callback: callable accepting an event to be executed if when a matching event
                      is received
            pattern: hashable object to be used when filtering event (passed to inherited
                     ``filter(...)``)
            debounce:
            delay:
        '''
        route_tuple = (callback, pattern, debounce, delay, listener_kwargs)
        self.routemap[endpoint].append(route_tuple)

    def submit(self, events: E | list[E], callbacks: list[Callable] | None = None):
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

    def submit_event(self, event: E, callbacks: list[Callable] | None = None):
        '''
        Group up and submit all matching callbacks for ``event``. All callbacks are ran
        concurrently in their own threads, and this method blocks until all are completed.

        In the outer ``submit`` context, this blocking method is itself ran in its own
        thread, and the registered post-callbacks are attached to the completion of this
        function, i.e., the finishing of all callbacks matching provided event.

        Note that an event may not match any routes, in which case the method exits early.
        An empty list is returned, and this shows up as the outer future's result. In this
        case, the event is never considered "running," and the non-result picked up in
        ``clear_event`` will ensure it exits right away (not even attempting to pop the
        event from the running list, and for now not tracking it in the event log).
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

    def submit_callback(self, callback: Callable, *args, **kwargs):
        '''
        Note: this method is expected to return a future. Perform any event-based
        filtering before submitting a callback with this method.
        '''
        # exit immediately if exit flag is set
        # if self.should_exit:
        #     return
        callback = self.wrap_safe_callback(callback)

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
            self._active_futures.add(future)
            future.add_done_callback(self.general_task_done)

        return future

    def matching_routes(self, event: E, event_time=None):
        '''
        Return eligible matching routes for the provided event.

        Note that we wait as late as possible before enqueuing matches if the event is in
        fact already active in a frame. If this method were start filtering results while
        the frame is active, and the frame were to finish before all matching callbacks
        were determined, we would be perfectly happy to return all matches, and allow the
        outer ``submit_event`` context to run them right away in a newly constructed frame.
        The _very_ next thing that gets done is adding this event to the active event
        tracker. Otherwise, matching is performed as usual, and eligible callbacks are
        simply enqueued for the next event frame, which will be checked in the "done"
        callback of the active frame. The logic here should mostly "seal up" any real
        opportunities for error, e.g., a frame ending and popping off elements from
        ``running_events`` half-way through their inserting at the end of this method, or
        multiple threads checking for matching routes for the same event, and both coming
        away with a non-empty set of matches to run. That last example highlights
        precisely how the single event-frame model works: many threads might be running
        this method at the same time, for the same event (which has fired rapidly), but
        only one should be able to "secure the frame" and begin running the matching
        callbacks. Making the "active frame check" both as late as possible and as close
        to the event blocking stage in the tracker (in ``submit_event``), we make the
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

            callback_name = str(callback)
            if hasattr(callback, '__name__'):
                callback_name = callback.__name__

            name_text     = color_text(name,               Fore.BLUE)
            pattern_text  = color_text(pattern,            Fore.BLUE)
            endpoint_text = color_text(endpoint,           Fore.BLUE)
            callback_text = color_text(callback_name[:50], Fore.BLUE)

            if self.filter(event, pattern, **listen_kwargs):
                # note that delayed callbacks are added
                matches.append(self.get_delayed_callback(callback, delay, index))

                # set next debounce 
                self.next_allowed_time[index] = event_time + debounce

                match_text = color_text('matched', Style.BRIGHT, Fore.GREEN)
                logger.info(
                    f'Event [{name_text}] {match_text} [{pattern_text}] under [{endpoint_text}] for [{callback_text}]'
                )
            else:
                match_text = color_text('rejected', Style.BRIGHT, Fore.RED)
                logger.debug(
                    f'Event [{name_text}] {match_text} against [{pattern_text}] under [{endpoint_text}] for [{callback_text}]'
                )

        return matches

    def get_delayed_callback(self, callback: Callable, delay: int|float, index):
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
        Block until all futures in ``futures`` are complete. Return collected results as a
        list, and log warnings when a future fails.
        '''
        future_results = []
        for future in as_completed(futures):
            try:
                if not future.cancelled():
                    future_results.append(future.result())
            except Exception as e:
                logger.warning(f"Router callback job failed with exception \"{e}\"")

        return future_results

    def wait_on_callbacks(self, callbacks: list[Callable], event: E, *args, **kwargs):
        '''
        Overridable by inheriting classes based on callback structure
        '''
        return self.wait_on_futures([
            self.submit_callback(callback, event, *args, **kwargs)
            for callback in callbacks
        ])

    def queue_callbacks(self, event_idx, callbacks: list[Callable]):
        '''
        Overridable by inheriting classes based on callback structure
        '''
        self.running_events[event_idx].update(callbacks)

    def wrap_safe_callback(self, callback: Callable):
        '''
        Check for shutdown flag and exit before running the callbacks. 

        Applies primarily to jobs enqueued by the ThreadPoolExecutor but not started when
        an interrupt is received.
        '''
        def safe_callback(callback, *args, **kwargs):
            if self.should_exit:
                logger.debug('Exiting early from queued callback')
                return

            return callback(*args, **kwargs) 

        return partial(safe_callback, callback)

    def filter(self, event: E, pattern, **listen_kwargs) -> bool:
        '''
        Determine if a given event matches the provided pattern

        Parameters:
            event:
            pattern:
            listen_kwargs: 
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
        Extend a provided Listener object with the Router instance's ``listener_kwargs``.
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

    def clear_event(self, event: E, future):
        '''
        Clear an event. Pops the passed event out of ``running_events``, and the request
        counter is >0, the event is re-submitted.

        This method is attached as a "done" callback to the main event wrapping job
        ``submit_event``. The ``future`` given to this method is one to which it was
        attached as this "done" callback. This method should only be called when that
        ``future`` is finished running (or failed). If any jobs were submitted in the
        wrapper task, the future results here should be non-empty. We use this fact to
        filter out non-work threads that call this method. Because even the
        ``matching_routes`` check is threaded, we can't wait to see an event has no work to
        schedule, and thus can't prevent this method being attached as a "done" callback.
        The check for results from the passed future allows us to know when in fact a
        valid frame has finished, and a resubmission may be on the table.
        '''
        result = None
        if not future.cancelled():
            result = future.result()
        else:
            return None

        # result should be *something* if work was scheduled
        if not result:
            return None

        self.event_log.append((event, result))
        queued_callbacks = self.stop_event(event)

        # resubmit event if some queued work remains
        if queued_callbacks and len(queued_callbacks) > 0:
            logger.debug(
                f'Event [{event.name}] resubmitted with [{len(queued_callbacks)}] queued callbacks'
            )
            self.submit(event, callbacks=queued_callbacks)

    def event_index(self, event):
        return event[:2]

    def shutdown(self):
        logger.info(color_text('Router shutdown received', Fore.BLACK, Back.RED))

        self.should_exit = True
        for future in tqdm(
            list(self._active_futures),
            desc=color_text('Cancelling active futures...', Fore.BLACK, Back.RED),
            colour='red',
        ):
            future.cancel()

        if self.thread_pool is not None:
            self.thread_pool.shutdown(wait=False)

    def general_task_done(self, future):
        self._active_futures.remove(future)
        try:
            if not future.cancelled():
                future.result()
        except Exception as e:
            logger.error(f"Exception occurred in threaded task: '{e}'")
            #traceback.print_exc()


class ChainRouter[E: Event](Router[E]):
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
        
        .. note::

            the ``routemap`` extensions here shouldn't be necessary, since 1) route maps
            show up only in ``matching_routes``, and 2) ``matching_routes`` is only
            invoked in ``submit_event``, which is totally overwritten for the ChainRouter
            type. All events are routed through to individual Routers, and which point
            their route maps are used.
        '''
        self.ordered_routers.append(router)
        for endpoint, routelist in router.routemap.items():
            self.routemap[endpoint].extend(routelist)

    def matching_routes(self, event: E, event_time=None):
        '''
        Colloquial ``callbacks`` now used as a dict of lists of callbacks, indexed by
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

    def wait_on_callbacks(self, callbacks, event: E, *args, **kwargs):
        '''
        Note: relies on order of callbacks dict matching that of ``ordered_routers``, which
        should happen in ``matching_routes``
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
        Sub-routers do not get a "done" callback for their ``submit_event`` jobs, as they
        would if they handled their own event submissions. They will, however, set the
        submitted event as "running." We can't rely on sub-routers' "done" callbacks to
        "unset" the running event, because the disconnect between the thread completing
        and execution of that callback may take too long. 

        Instead, we explicitly unset the running event for each of the constituent
        sub-routers at the *same time* we handle the ChainRouter's notion of event's
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


# RouterBuilder
def route(router, route_group, **route_kwargs):
    def decorator(f):
        f._route_data = (router, route_group, route_kwargs)
        return f

    return decorator

class RouteRegistryMeta(type):
    '''
    Metaclass handling route registry at the class level.
    '''
    def __new__(cls, name, bases, attrs):
        route_registry = defaultdict(lambda: defaultdict(list))

        def register_route(method):
            nonlocal route_registry

            if hasattr(method, '_route_data'):
                router, route_group, route_kwargs = method._route_data
                route_registry[router][route_group].append((method, route_kwargs))

        # add registered superclass methods; iterate over bases (usually just one), then
        # that base's chain down (reversed), then methods from each subclass
        for base in bases:
            for _class in reversed(base.mro()):
                methods = inspect.getmembers(_class, predicate=inspect.isfunction)
                for _, method in methods:
                    register_route(method)

        # add final registered formats for the current class, overwriting any found in
        # superclass chain
        for attr_name, attr_value in attrs.items():
            register_route(attr_value)

        attrs['route_registry'] = route_registry

        return super().__new__(cls, name, bases, attrs)

class RouterBuilder(ChainRouter, metaclass=RouteRegistryMeta):
    '''
    Builds a (Chain)Router using attached methods and passed options.

    This class can be subtyped and desired router methods attached using the provided
    ``route`` decorator. This facilitates two separate grouping mechanisms:

    1. Group methods by frame (i.e., attach to the same router in a chain router)
    2. Group by registry equivalence (i.e, within a frame, registered with the same
       parameters)

    These groups are indicated by the following collation syntax:

    .. code-block:: python

        @route('<router>/<frame>', '<route-group>', **route_kwargs)
        def method(...):
            ...

    and the following is a specific example:

    .. code-block:: python

        @route(router='convert', route_group='file', debounce=500)
        def file_convert_1(self, event):
            ...

    which will attach the method to the "convert" router (or "frame" in a chain router
    context) using parameters (endpoint, pattern, and other keyword args) associated with
    the "file" route group (as indexed by the ``register_map`` provided on instantiation)
    with the ``debounce`` route keyword (which will override the same keyword values if
    set in the route group). Note that the exact same ``@route`` signature can be used for
    an arbitrary number of methods to be handled in parallel by the associated Router.
    
    Note that there is one reserved route group keyword: "post," for post callbacks.
    Multiple post-callbacks for a particular router can be specified with the same ID
    syntax above.

    .. admonition:: Map structures

        The following is a more intuitive breakdown of the maps involved, provided and
        computed on instantiation:

        .. code-block:: python
            
            # provided
            register_map[<router-name>] -> ( Router, { <type>: ( ( endpoint, pattern ), **kwargs ) } )

            # computed
            routers[<router-name>][<type>] -> [... <methods> ...]

    .. admonition:: TODO
        
        Consider "flattening" the ``register_map`` to be indexed only by ``<type>``,
        effectively forcing the 2nd grouping mechanism to be provided here (while the 1st
        is handled by the method registration within the body of the class). This properly
        separates the group mechanisms and is a bit more elegant, but reduces the
        flexibility a bit (possibly in a good way, though).
    '''
    def __init__(
        self,
        register_map: dict[str, tuple[Router, dict[str, tuple[tuple[str, str], dict[str, Any]]]]],
    ):
        self.register_map = register_map
        routers = []

        # register
        for router_name, (router, router_options) in self.register_map.items():
            routers.append(router)
            for route_group, method_arg_list in self.route_registry[router_name].items():
                # get post-callbacks for reserved key "post"
                # assumed no kwargs for passthrough
                if route_group == 'post':
                    for method, _ in method_arg_list:
                        router.add_post_callback(
                            update_wrapper(partial(method, self), method),
                        )
                    continue

                group_options = router_options.get(route_group)
                if group_options is None:
                    continue

                # "group_route_kwargs" are route kwargs provided @ group level
                # "method_route_kwargs" are route kwargs provided @ method level 
                # |-> considered more specific and will override group kwargs
                (endpoint, pattern), group_route_kwargs = group_options
                for method, method_route_kwargs in method_arg_list:
                    router.register(
                        endpoint,
                        update_wrapper(partial(method, self), method),
                        pattern,
                        **{
                            **group_route_kwargs,
                            **method_route_kwargs
                        }
                    )

        super().__init__(routers)

    # -- disabling for now to inherit from ChainRouter directly. Require the order to
    # -- simply be specified by the order of the router keys in the register_map
    # def get_router(self, router_key_list: list[str]):
    #     return ChainRouter([self.register_map[k][0] for k in router_key_list])

