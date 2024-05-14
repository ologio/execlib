'''
See also:

- https://inotify-simple.readthedocs.io/en/latest/#gracefully-exit-a-blocking-read
'''
import threading

from execlog.event import Event


class Listener[E: Event](threading.Thread):
    '''
    Implements a file system watcher.
    '''
    def __init__(self, router: 'Router[E]'):
        '''
        Parameters:
            router: associated Router instance that events should be passed to
        '''
        super().__init__()

        self.router = router

    def listen(self):
        '''
        Register a new listener endpoint
        '''
        raise NotImplementedError

    def run(self):
        '''
        Begin listening for events. Typically a blocking loop that passes events to
        attached Router.
        '''
        raise NotImplementedError

    def stop(self):
        '''
        Begin listening for events. Typically a blocking loop that passes events to
        attached Router.
        '''
        raise NotImplementedError
