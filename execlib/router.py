'''
Router
'''
import time
import asyncio
import logging
import inspect
import traceback
import threading
import concurrent 
from enum import Enum
from pathlib import Path
from typing import Any, Callable
from collections import defaultdict
from colorama import Fore, Back, Style
from functools import partial, update_wrapper
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

from tqdm.auto import tqdm

from execlib.event import Event
from execlib.listener import Listener
from execlib.util.generic import color_text, get_func_name


logger = logging.getLogger(__name__)

class FrameDirective(Enum):
    '''
    Indicates frame-level behavior when a callback fails.
    '''
    CONTINUE_WITHOUT = 1
    CANCEL_FRAME = 2

class CallbackTimeoutError(Exception):
    ...

class CancelledFrameError(Exception):
    ...

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

    .. admonition:: Details behind the threaded model and future management

        Routers kick off execution in response to *events*. These events are received via
        ``.submit``, and the following process is kick-started:

        1. Each event is wrapped in its own ``.submit_event`` call and submitted as a task
           to the *primary* thread pool. Let $E$ be the set of events, and $|E|$ be the
           number of events. ``.submit`` exits as soon as these $|E|$ tasks are enqueued,
           not waiting for the completion of the corresponding worker threads. There are
           now $|E|$ tier-I tasks waiting to be started by the router's primary thread
           pool, with states set to "pending."
        2. The primary thread pool begins running the enqueued ``.submit_event`` calls
           concurrently using allocated resources (e.g., four threads). Each
           ``.submit_event`` call matches the associated event $e$ to $c_e$ callbacks,
           according to the registered routes. These callbacks are each individually
           submitted to the *secondary* thread pool as tier-II tasks and waited upon
           *within* the ``.submit_event`` call. This thread pool separation prevents
           deadlocks which would otherwise be an issue if submitting both tier-I and
           tier-II tasks to the same thread pool. Tier-I tasks that have begun this
           process are in the "running" state, and the submitted tier-II futures are now
           "pending."
        3. Once the $c_e$ callbacks for event $e$ are completed (which are tier-II tasks
           being waited upon within a tier-I task), their done-callbacks will be invoked
           within "a thread belonging to the process that added them" (with
           ``wait_on_event_callbacks`` calling through to ``submit_callback``, which
           attaches ``general_task_done``). Where these done callbacks are executed
           varies based on a few conditions. See

           https://stackoverflow.com/a/26021772/4573915

           for a great breakdown on this. The gist is the following: 

           a. If the callback is attached to a future that is already cancelled or
              completed, it will be invoked immediately in the current thread doing the
              attaching.
           b. If the future is queued/pending and successfully cancelled, the thread
              doing the cancelling will immediately invoke all of the future's callbacks.
           c. Otherwise, the thread that executes the future's task (could produce either
              a successful result or an exception) will invoke the callbacks.

           So if a task completes (i.e., is not cancelled, producing either a result or an
           exception), the thread that ran the task will also handle the associated
           callbacks. If the task is successfully cancelled (which means it was never
           running and never allocated a thread), the cancelling context will handle the
           callbacks, and this happens here only in ``.shutdown()`` with the call
           ``thread_pool.shutdown(cancel_futures=True)``.

           The results of these futures are made available in this ``submit_event``
           context. Note that these results are dictated by the logic in
           ``wait_on_futures``. If a future was successfully cancelled or raised and
           exception during execution, it will not have a result to add to this list.

           The *router-level* post-callbacks are then submitted to the secondary thread
           pool and awaited in a similar fashion to the individual $c_e$ callbacks. The
           results obtained from the event callbacks are passed through to these
           "post-callbacks" for possible centralized processing.
        4. Once all post-callbacks have completed (along with *their* attached
           "done-callbacks," which are just ``.general_task_done`` checks handled in the
           executor thread that ran the post-callback), finally the tier-I
           ``.submit_event`` future can be marked completed (either with a successfully
           attached result or an exception), and its attached "done-callbacks" will be
           ran the in the same tier-I thread that handled the task (which is
           ``general_task_done`` and ``clear_event``, in that order).

        - General behaviors and additional remarks:
          * Thread pool futures can only be cancelled prior to "running" or "done" states.
            Both successful cancellation or completion trigger a future's done-callbacks,
            which will be executed in one a few possible contexts depending several
            conditions, as detailed above.
          * Tier-I tasks have ``clear_event`` attached as a done-callback, which tracks
            the task result and resubmits the event if valid (successfully debounced)
            repeat requests were received while the event has been handled.
          * Both tier-I and tier-II callbacks have a ``.general_task_done`` callback, which
            attempts to retrieve the future result if it wasn't cancelled (if it was, this
            retrieval would raise a ``CancelledError``). If it wasn't cancelled but an
            exception was raised during execution, this same exception will be re-raised
            and re-caught, logged as an error, and exit "cleanly" (since job failures
            shouldn't throw off the entire process). A successful result retrieval will
            have no effect.

        - On interrupts, exception handling, and future cancellation:
          * 
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
        self.callback_start_times = {}

        # track event history
        self.event_log = []

        # shutdown flag, mostly for callbacks
        self.should_exit = False
        self._active_futures = set()

        self._thread_pool_1 = None
        self._thread_pool_2 = None
        self._route_lock  = threading.Lock()

    @property
    def primary_thread_pool(self):
        '''Handle tier-I futures.'''
        if self._thread_pool_1 is None:
            self._thread_pool_1 = ThreadPoolExecutor(max_workers=self.workers)
        return self._thread_pool_1
    
    @property
    def secondary_thread_pool(self):
        '''Handle tier-II futures.'''
        if self._thread_pool_2 is None:
            self._thread_pool_2 = ThreadPoolExecutor(max_workers=self.workers)
        return self._thread_pool_2

    def register(
        self,
        endpoint,
        callback: Callable,
        pattern,
        debounce=200,
        delay=10,
        callback_timeout=None,
        condition=FrameDirective.CONTINUE_WITHOUT,
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
            callback_timeout: timeout for waiting 
        '''
        route_tuple = (
            callback,
            pattern,
            debounce,
            delay,
            callback_timeout,
            condition,
            listener_kwargs
        )
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

    def submit_event(
        self,
        event      : E,
        callbacks  : list[Callable] | None       = None,
        timeouts   : list[int|float] | None      = None,
        conditions : list[FrameDirective] | None = None,
    ) -> list:
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
                callbacks, timeouts, conditions = self.matching_routes(event)

        # stop early if no work to do
        if len(callbacks) == 0:
            return []

        # enqueue requested/matched callbacks and exit if running
        event_idx = self.event_index(event) 
        if event_idx in self.running_events:
            self.queue_callbacks(event_idx, callbacks)
            return []

        # TODO: Chesterton's fence
        # callbacks now computed, flush the running event
        # note: a separate thread could queue valid callbacks since the running check;
        # o/w we know the running index is empty
        # self.running_events[event_idx] = self.running_events[event_idx]

        # submit matching callbacks and wait for them to complete
        # *may* raise a FrameCancelledError, which we let propagate upward
        completed_futures = self.wait_on_event_callbacks(
            event,
            callbacks,
            timeouts=timeouts,
            conditions=conditions,
        )

        # finally call post event-group callbacks (only if some event callbacks were
        # submitted), wait for them to complete
        if completed_futures:
            wait([
                self.submit_event_callback(post_callback, event, completed_futures)[0]
                for post_callback in self.post_callbacks
            ])

        return completed_futures

    def _submit_with_thread_pool(self, thread_pool, callback: Callable, *args, **kwargs):
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
            future = thread_pool.submit(
                callback, *args, **kwargs
            )

        return future

    def submit_callback(self, callback: Callable, *args, **kwargs):
        future = self._submit_with_thread_pool(
            self.primary_thread_pool,
            callback,
            *args, **kwargs
        )
        self._active_futures.add(future)

        return future

    def submit_event_callback(self, callback: Callable, event: E, *args, **kwargs):
        '''
        Note: this method is expected to return a future. Perform any event-based
        filtering before submitting a callback with this method.
        '''
        # exit immediately if exit flag is set
        # if self.should_exit:
        #     return
        submitted_time = time.time()
        callback = self.wrap_timed_callback(callback, submitted_time)

        future = self._submit_with_thread_pool(
            self.secondary_thread_pool,
            callback,
            event,
            *args, **kwargs
        )
        future.add_done_callback(self.general_callback)

        return future, submitted_time

    def matching_routes(
        self,
        event: E,
        event_time = None
    ) -> tuple[list[Callable], list[int|float]]:
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
        timeouts   = []
        conditions = []
        endpoint   = event.endpoint
        name       = event.name
        #action     = tuple(event.action) # should be more general
        event_time = time.time()*1000 if event_time is None else event_time

        for (callback, pattern, debounce, delay, ctimeout, condition, listen_kwargs) in self.routemap[endpoint]:
            #index = (endpoint, name, action, callback, pattern, debounce, delay)
            index = (endpoint, name, callback, pattern, debounce, delay)

            if event_time < self.next_allowed_time[index]:
                # reject event
                continue

            callback_name = get_func_name(callback)

            name_text     = color_text(name,               Fore.BLUE)
            pattern_text  = color_text(pattern,            Fore.BLUE)
            endpoint_text = color_text(endpoint,           Fore.BLUE)
            callback_text = color_text(callback_name[:50], Fore.BLUE)

            if self.filter(event, pattern, **listen_kwargs):
                # note that delayed callbacks are added
                matches.append(self.get_delayed_callback(callback, delay, index))
                timeouts.append(ctimeout)
                conditions.append(condition)

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

        return matches, timeouts, conditions

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

    def wait_on_event_callbacks(
        self,
        event      : E,
        callbacks  : list[Callable],
        timeouts   : list[int | float | None] | None    = None,
        conditions : list[FrameDirective | None] | None = None,
    ): #, *args, **kwargs):
        '''
        Waits for event-associated callbacks to complete.

        Submits each callback in ``callbacks`` to the thread pool (via
        ``submit_callback``), passing ``event`` as the only parameter to each. Once
        started, the future for callback ``callbacks[i]`` will have ``timeouts[i]``
        seconds to complete. If it has not completed in this time, the future's result is
        set to the Timeout exception

        Overridable by inheriting classes based on callback structure
        '''
        if timeouts is None:
            timeouts = [None]*len(callbacks)

        if conditions is None:
            conditions = [FrameDirective.CONTINUE_WITHOUT]*len(callbacks)

        future_map = {}
        timed_futures = set()
        for callback, timeout, on_err in zip(callbacks, timeouts, conditions):
            # "raw" callback here is the reference that will be indexed in `.callback_start_times`
            future, submitted_time = self.submit_event_callback(callback, event) #*args, **kwargs)
            future_map[future] = (
                callback,
                get_func_name(callback),
                future,
                submitted_time,
                timeout,
                on_err
            )

            if timeout is not None:
                timed_futures.add(future)

        completed_futures = []
        # iterate while there are some futures not completed (cancelled, or finished w/
        # exception or return value). When completed, a future is removed from `futures`.
        while future_map:
            min_timeout = float('inf')
            expired_futures = [] # actively running but expired

            if timed_futures:
                time_now = time.time()
                for future in list(timed_futures):
                    callback, cback_name, future, submitted_time, timeout, on_err = future_map[future]
                    future_tuple = (future, cback_name, on_err)
                    callback_id = (callback, submitted_time)

                    if future.done():
                        timed_futures.remove(future)
                        continue

                    if future.running() and callback_id in self.callback_start_times:
                        time_running = time_now - self.callback_start_times[callback_id]
                        time_left = timeout - time_running

                        # track running futures that have timed out
                        if time_left < 0:
                            expired_futures.append(future_tuple)
                            continue
                    else:
                        # running w/o a start time, or queued. Set time-left to the timeout
                        time_left = timeout

                    # track running future w/ smallest non-zero time left before timeout
                    min_timeout = min(min_timeout, time_left)

            done_futures = [] 
            for future in list(future_map.keys()):
                _, cback_name, future, _, _, on_err = future_map[future]
                future_tuple = (future, cback_name, on_err)

                if future.done():
                    # done futures have a set result or exception and now immutable
                    done_futures.append(future_tuple) # local
                    completed_futures.append(future) # global
                    future_map.pop(future)

            done_with_exception = [
                ftuple
                for ftuple in done_futures
                if ftuple[0].exception() is not None
            ]
            frame_cancellable = expired_futures + done_with_exception

            cancel_frame = False
            # set a timeout exception for all expired futures, and determine
            # if any of these timeouts should result in a frame cancellation
            for _, _, on_err in frame_cancellable:
                if on_err == FrameDirective.CANCEL_FRAME:
                    cancel_frame = True
                    break

            # set exceptions on running expired futures
            for expired_future, cback_name, _ in expired_futures:
                # results are immutable once set; double check doneness
                if not expired_future.done():
                    expired_future.set_exception(
                        CallbackTimeoutError(
                            f'Event callback {cback_name} timed out with condition {on_err}'
                        )
                    )

            if cancel_frame:
                # cancel the active frame. Expired futures have already been handled, and
                # we now need to handle running futures (not cancellable) and pending
                # futures (cancellable)
 
                # explicitly cancel queued futures before looping through active futures
                for future in list(future_map.keys()):
                    # cancellable futures not yet started
                    # can't really be more specific here, sets a CancelledError 
                    future.cancel()

                for future in list(future_map.keys()):
                    _, cback_name, future, _, _, _ = future_map[future]

                    if future.done():
                        # includes the expired futures whose exceptions were just set &
                        # those cancelled, although managing `future_map` isn't needed at
                        # this stage
                        future_map.pop(future)
                    elif future.running():
                        # possible memory leak
                        future.set_exception(
                            CancelledFrameError(
                                f'Indirect frame cancellation for event callback {cback_name}'
                            )
                        )
                        # attempt to communicate with threaded processes for graceful shutdown
                        # -> set shutdown flags
                        ...

                # finally, raise an exception indicating the frame was cancelled
                raise CancelledFrameError

            # if no expired futures (or they can be ignored with CONTINUE_WITHOUT), carry
            # on. Wait for the future with the least time remaining; we shouldn't need to
            # do any of the above future parsing until at least min-timeout more seconds.
            timeout = None
            if min_timeout < float('inf'):
                timeout = min_timeout

            # if any remaining futures can produce CANCELLED FRAMEs, wait only on them.
            # This lets us be maximally dynamic: 
            # -> We respond before the timeout only if the newly completed might CANCEL
            #    the FRAME, which we want to do as soon as possible.
            # -> If not responding early, we wait at most `timeout` seconds to be back and
            #    checking if the *earliest possible timeout violation* has occurred. If
            #    so, we want to handle it immediately no matter what so I can set the
            #    desired Timeout exception as *early as possible*. If I don't, the future
            #    might finish and set its own result, shutting me out when I would've
            #    liked to capture the timeout violation.
            # -> When CANCEL FRAME futures are available, I don't wait on all pending
            #    futures as I *don't mind* being late to them. I only need to check
            #    immediately on CANCEL FRAMEs, and otherwise be back no later than
            #    `timeout`. If there's no `timeout` I'll wait for the first CANCEL FRAME,
            #    if no CANCEL FRAME I'll wait up to timeout on all futures, and if neither
            #    I'll simply wait until all futures complete.
            cancel_frame_futures = [
                future
                for future, ftuple in future_map.items()
                if ftuple[-1] == FrameDirective.CANCEL_FRAME
            ]

            return_when = concurrent.futures.ALL_COMPLETED
            wait_futures = future_map.keys()
            if cancel_frame_futures:
                return_when = concurrent.futures.FIRST_COMPLETED
                wait_futures = cancel_frame_futures

            # if no active future has a timeout
            done, _ = wait(
                wait_futures,
                timeout=timeout,
                return_when=return_when
            )

        # add any trailing completed futures
        completed_futures.extend(done)

        return completed_futures

    def queue_callbacks(self, event_idx, callbacks: list[Callable]):
        '''
        Overridable by inheriting classes based on callback structure
        '''
        self.running_events[event_idx].update(callbacks)

    def wrap_timed_callback(self, callback: Callable, submitted_time):
        '''
        Check for shutdown flag and exit before running the callbacks. 

        Applies primarily to jobs enqueued by the ThreadPoolExecutor but not started when
        an interrupt is received.
        '''
        def safe_callback(callback, *args, **kwargs):
            # track when this task actually begins
            self.callback_start_times[(callback, submitted_time)] = time.time()

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
        Clear an event. Pops the passed event out of ``running_events``, and if the
        request counter is >0, the event is re-submitted.

        This method is attached as a "done" callback to the main event wrapping job
        ``submit_event``. The ``future`` given to this method is one to which it was
        attached as this "done" callback. This method should only be called when that
        ``future`` is finished running (or failed). If any jobs were submitted in the
        wrapper task, the future results here should be non-empty (even if the methods
        don't return anything; we'll at least have ``[None,...]`` if we scheduled at least
        one callback). We use this fact to filter out non-work threads that call this
        method. Because even the ``matching_routes`` check is threaded, we can't wait to
        see an event has no work to schedule, and thus can't prevent this method being
        attached as a "done" callback. The check for results from the passed future allows
        us to know when in fact a valid frame has finished, and a resubmission may be on
        the table.

        .. admonition:: Why we don't need to worry about resubmission w/ empty results

            Note that, even though we can't check for whether there will be any matching
            routes prior to calling ``submit_event`` (we have to wait until we're in that
            method, at which point this method will already be attached as a callback),
            the event will never be marked as "running" and added to ``running_events``.
            This means that we won't queue up any callbacks for the same event while
            waiting on it, and then exit early here, never to re-submit. The worry was
            that a event response might match 0 callbacks (e.g., due to debouncing) and
            return ``[]`` from ``submit_event``, but *while waiting for this to complete*,
            the same event is submitted and matches (e.g., debouncing timers now allowing
            the event through). This could mean that the repeat event gets queued behind
            the event in ``running_events`` and should be resubmitted here as a "queued
            callback," but *won't do so* because we exit early if no results are obtained.
            This is **not an issue** because the original event (that didn't match any
            callbacks) will never be marked as running, and thus never prevent the second
            event from itself running if in fact in matches a non-empty callback set. This
            means that an empty future result set seen here indicates both 1) that no work
            took place, and 2) no conflicting events were prevented from running, and we
            can exit early here.
        '''
        self._active_futures.remove(future)

        # result should be *something* if work was scheduled, since `submit_event` wraps
        # up futures in a list. If no result, event was never marked active, and don't
        # want to resubmit as any duplicates were allowed to start. Attempt to get result,
        # returning if it's (None or []) or raised an Exception (possibly a
        # FrameCancelledError if there's a frame issue, for a CancelledError if the tier-I
        # task was successfully cancelled following a `.shutdown()` call)
        try:
            if not future.result(): return
        except concurrent.futures.CancelledError as e:
            #logger.error(f'Tier-I future cancelled')
            # do not re-raise; outer context can handle cancellations
            return
        except CancelledFrameError as e:
            # do not re-raise; outer context will manage frame cancellations
            return
        except Exception as e:
            # print traceback for unexpected exception
            logger.error(f'Unexpected exception in tier-I future: "{e}"')
            traceback.print_exc()
            return

        self.event_log.append((event, future))
        queued_callbacks = self.stop_event(event)

        # resubmit event if some queued work remains
        if queued_callbacks and len(queued_callbacks) > 0:
            logger.debug(
                f'Event [{event.name}] resubmitted with [{len(queued_callbacks)}] queued callbacks'
            )
            self.submit(event, callbacks=queued_callbacks)

    def event_index(self, event):
        return event[:2]

    def general_callback(self, future):
        try:
            future.result()
        except concurrent.futures.CancelledError as e:
            logger.error(f'Tier-II future cancelled; "{e}"')
        except CancelledFrameError as e:
            logger.error(f'Tier-II frame cancelled; "{e}"')
        except Exception as e:
            logger.warning(f'Tier-II job failed with unknown exception "{e}"')

    def shutdown(self):
        logger.info(color_text('Router shutdown received', Fore.BLACK, Back.RED))
        self.should_exit = True

        # manually track and cancel pending futures b/c `.shutdown(cancel_futures=True)`
        # is misleading, and will cause an outer `as_completed` loop to hang
        if self._active_futures:
            for future in tqdm(
                list(self._active_futures),
                desc=color_text(
                    f'Cancelling {len(self._active_futures)} pending futures...',
                    Fore.BLACK, Back.RED),
                colour='red',
            ):
                future.cancel()

        if self._thread_pool_2 is not None:
            # cancel pending futures (i.e., those not started)
            self.secondary_thread_pool.shutdown(wait=False)

        if self._thread_pool_1 is not None:
            # cancel pending futures (i.e., those not started)
            self.primary_thread_pool.shutdown(wait=False)


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

    def matching_routes(
        self,
        event: E,
        event_time=None
    ):
        '''
        Colloquial ``callbacks`` now used as a dict of lists of callbacks, indexed by
        router, and only having keys for routers with non-empty callback lists.
        '''
        if event_time is None:
            event_time = time.time()*1000

        callback_map  = {}
        timeout_map   = {}
        condition_map = {}
        for router in self.ordered_routers:
            matches, timeouts, conditions = router.matching_routes(event, event_time)
            if matches:
                callback_map[router]  = matches
                timeout_map[router]   = timeouts
                condition_map[router] = conditions

        return callback_map, timeout_map, condition_map

    def wait_on_event_callbacks(
        self,
        event      : E,
        callbacks  : dict[Router, list[Callable]],
        timeouts   : dict[Router, list[int | float | None]] | None    = None,
        conditions : dict[Router, list[FrameDirective | None]] | None = None,
    ): #, *args, **kwargs):
        '''
        Returns a dictionary mapping from 

        Note: relies on order of callback-associated dicts matching that of
        ``ordered_routers``, which should happen in ``matching_routes``.

        This method blurs the OOP lines a bit, as we're actually passing dicts rather than
        the lists expected by the super class. The parent ``submit_event`` is carefully
        designed to be indifferent; use caution when making changes.
        '''
        if timeouts is not None:
            timeouts = {}

        if conditions is not None:
            conditions = {}

        futures = {}
        for router, callback_list in callbacks.items():
            futures[router] = router.submit_event(
                event,
                callback_list,
                timeouts=timeouts.get(router),
                conditions=conditions.get(router),
            )

        return futures

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

    def shutdown(self):
        super().shutdown()

        # for router in self.ordered_routers:
        #     router.shutdown()


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

