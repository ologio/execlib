'''
Implements a file system watcher.

See also:
    - https://inotify-simple.readthedocs.io/en/latest/#gracefully-exit-a-blocking-read
'''
import threading


class Listener(threading.Thread):
    def __init__(self, router):
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
        super().__init__()

        self.router = router

    def listen(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
