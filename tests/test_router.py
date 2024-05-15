import logging
from pathlib import Path
from functools import partial
from concurrent.futures import wait

from execlib import util
from execlib import ChainRouter, Event
from execlib.routers import PathRouter
from execlib.listeners import PathListener


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(util.generic.TqdmLoggingHandler())

# router setup
router1 = PathRouter()
router2 = PathRouter()
router3 = PathRouter()

chain_router = ChainRouter([router1, router2, router3])

def test_route_registry():
    # router-1
    router1.register('endpoint_proxy', partial(print, 'R1-1 ::'))

    # router-2
    router2.register('endpoint_proxy', partial(print, 'R2-1 ::'))
    router2.register('endpoint_proxy', partial(print, 'R2-2 ::'))

    # router-3
    router3.register('endpoint_proxy', partial(print, 'R3-1 ::'))
    router3.register('endpoint_proxy', partial(print, 'R3-2 ::'))
    router3.register('endpoint_proxy', partial(print, 'R3-3 ::'))

    assert True

def test_single_router_submission():
    events = [
        Event(endpoint='endpoint_proxy', name='file1'),
        Event(endpoint='endpoint_proxy', name='file2'),
        Event(endpoint='endpoint_proxy', name='file3'),
    ]
    futures = router2.submit(events)
    wait(futures)
    
    assert True

def test_chain_router_submission():
    events = [
        Event(endpoint='endpoint_proxy', name='file1'),
        Event(endpoint='endpoint_proxy', name='file2'),
        Event(endpoint='endpoint_proxy', name='file3'),
    ]
    futures = chain_router.submit(events)
    wait(futures)

    assert True

