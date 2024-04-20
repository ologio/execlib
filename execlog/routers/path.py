import logging
from pathlib import Path

from execlog.router import Router
from execlog.listeners.path import PathListener
from execlog.util.path import glob_match


logger = logging.getLogger(__name__)

class PathRouter(Router):
    def __init__(self, loop=None, workers=None, listener_cls=PathListener):
        '''
        Parameters:
            workers: number of workers to assign the thread pool when the event loop is
                     started. Defaults to `None`, which, when passed to
                     ThreadPoolExecutor, will by default use 5x the number of available
                     processors on the machine (which the docs claim is a reasonable
                     assumption given threads are more commonly leveraged for I/O work
                     rather than intense CPU operations). Given the intended context for
                     this class, this assumption aligns appropriately.
        '''
        super().__init__(loop=loop, workers=workers, listener_cls=listener_cls)
    
    def register(
        self,
        path,
        func,
        glob='**/!(.*|*.tmp|*~)', # recursive, non-temp
        debounce=200,
        delay=30,
        **listener_kwargs,
    ):
        '''
        Parameters:
            path:  Path (directory) to watch with `inotify`
            func:  Callback to run if FS event target matches glob
            glob:  Relative glob pattern to match files in provided path. The FS event's
                   filename must match this pattern for the callback to queued. (Default:
                   "*"; matching all files in path).
            listener_kwargs: Additional params for associated listener "listen" routes.
                             See `PathListener.listen`.
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
        Note:
            If `handle_events` is called externally, note that this loop will block in the
            calling thread until the jobs have been submitted. It will _not_ block until
            jobs have completed, however, as a list of futures is returned. The calling
            Watcher instance may have already been started, in which case `run()` will
            already be executing in a separate thread. Calling this method externally will
            not interfere with this loop insofar as it adds jobs to the same thread pool.

            Because this method only submits jobs associated with the provided `events`,
            the calling thread can await the returned list of futures and be confident
            that top-level callbacks associated with these file events have completed. Do
            note that, if the Watcher has already been started, any propagating file
            events will be picked up and possibly process simultaneously (although their
            associated callbacks will have nothing to do with the return list of futures).

        Parameters:
            event : Event instance
            glob  : Single string or tuple of glob patterns to check against event endpoint
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
