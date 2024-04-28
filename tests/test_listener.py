import time
import logging
from pathlib import Path
from functools import partial

from execlog import util
from execlog import ChainRouter, Event
from execlog.routers import PathRouter
from execlog.listeners import PathListener


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(util.generic.TqdmLoggingHandler())

# router setup
router1 = PathRouter()
router2 = PathRouter()
router3 = PathRouter()

chain_router = ChainRouter([router1, router2, router3])

# router-1
router1.register('tests/endpoint_proxy', partial(print, 'R1-1 ::'))

# router-2
router2.register('tests/endpoint_proxy', partial(print, 'R2-1 ::'))
router2.register('tests/endpoint_proxy', partial(print, 'R2-2 ::'))

# router-3
router3.register('tests/endpoint_proxy', partial(print, 'R3-1 ::'))
router3.register('tests/endpoint_proxy', partial(print, 'R3-2 ::'))
router3.register('tests/endpoint_proxy', partial(print, 'R3-3 ::'))


def test_single_path_listener():
    '''
    1. Get listener for a single router
    2. Start listening for file events
    3. Create a few files under the registered path
    4. Wait a second for inotify to pick up on the events, allow jobs to be submitted to
       the router's thread pool
    5. Shutdown the listener; any lingering jobs will be finished if not done already
    '''
    listener = router1.get_listener()

    # listener starts in new thread
    listener.start()

    file_a = Path('tests/endpoint_proxy/fileA')
    file_a.write_text('test text')
    file_a.unlink()

    file_b = Path('tests/endpoint_proxy/fileB')
    file_b.write_text('test text')

    # allow I/O to propagate 
    time.sleep(1)

    listener.stop()

    assert True

def test_chain_path_listener():
    listener = chain_router.get_listener()

    # listener starts in new thread
    listener.start()

    file_a = Path('tests/endpoint_proxy/fileA')
    file_a.write_text('test text')
    file_a.unlink()

    file_b = Path('tests/endpoint_proxy/fileB')
    file_b.write_text('test text')

    # allow I/O to propagate 
    time.sleep(1)

    listener.stop()

    assert True
