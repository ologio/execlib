import logging
from pathlib import Path
from typing import Callable

from execlog.router import Router
from execlog.event import FileEvent
from execlog.util.path import glob_match
from execlog.listeners.path import PathListener


logger = logging.getLogger(__name__)

class PathRouter(Router[FileEvent]):
    listener_cls = PathListener

    def register(
        self,
        path     : Path,
        func     : Callable,
        glob     : str       = '**/!(.*|*.tmp|*~)', # recursive, non-temp
        debounce : int|float = 200,
        delay    : int|float = 30,
        **listener_kwargs,
    ):
        '''
        Parameters:
            path:  Path (directory) to watch with ``inotify``
            func:  Callback to run if FS event target matches glob
            glob:  Relative glob pattern to match files in provided path. The FS event's
                   filename must match this pattern for the callback to queued. (Default:
                   "*"; matching all files in path).
            debounce:
            delay:
            listener_kwargs: Additional params for associated listener "listen" routes.
                             See ``PathListener.listen``.
        '''
        super().register(
            #endpoint=Path(path),
            endpoint=path,
            callback=func,
            pattern=glob,
            debounce=debounce,
            delay=delay,
            **listener_kwargs
        )

    def filter(self, event, glob, **listen_kwargs) -> bool:
        '''
        Filter path events based on the provided glob pattern and listen arguments.

        This method is needed due to the lack of granularity when you have separate router
        callbacks that listen to the same directory (or overlap on some nested directory
        therein) with *different listen flags*. The overlapping path in question will only
        ever be assigned a single watch descriptor by iNotify, but will (or at least appears
        to) add (via bitwise OR) new flags if the same path is registered. Thus, an event
        fired by iNotify cannot be automatically propagated to the registered callbacks,
        as the file event "action" may apply only to a subset of those functions. This is
        the place for that final delineation, ensuring the exact action is matched before
        callback execution. This has the benefit of being a suitable proxy for the actual
        iNotify filtering that takes place when submitting synthetic events to the router
        by hand. 

        **Bigger picture, and why we have to reproduce the work already done by an
        event-based mechanism like iNotify**: Realistically, such a method is needed
        regardless if we hope to connect to the threaded router model as we do not
        canonically store callback associations at the listener level. If our responses
        could be tied one-to-one to the sensitivities of iNotify events, then they could
        be called directly in response to them. But they are not: we want to support
        glob-based filtering, need to delineate by flags as explained above, and can have
        separate endpoints for the same path. These are conditions *not* collapsed at the
        iNotify level, and thus need to be fully re-implemented for callback matching.
        (For example, imagine we had callback uniqueness on just endpoint and glob, i.e.,
        no sensitivity to flags, then the flag-matching conditions implemented here would
        not be needed to rightly pass iNotify events to their callbacks. In such a case,
        we could rely fully on iNotify's flag response model to implicitly handle this
        aspect of the filtering process. If the same could be said the remaining
        constraints, then as mentioned, we could simply associate callbacks one-to-one and
        avoid the auxiliary filtering altogether.)

        Parameters:
            event: Event instance
            glob:  Single string or tuple of glob patterns to check against event endpoint
        '''
        not_tmp_glob = '**/!(.*|*.tmp|*~)'
        if not glob_match(Path(event.name), not_tmp_glob):
            return False

        listen_flags = listen_kwargs.get('flags')
        # only filter by flags if explicitly specified on registry
        # (o/w route likely just wanting to use defaults)
        if listen_flags is not None:
            # negative filter if action not one of the listened flags
            if not any(flag & listen_flags for flag in event.action):
                logger.debug(
                    f'Event [{event.name}] caught in flag filter under [{glob}] for action [{event.action}]'
                )
                return False

        return glob_match(Path(event.name), glob)
