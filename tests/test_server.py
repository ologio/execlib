import time
import multiprocessing as mp
import threading
import logging
from pathlib import Path

from execlog import Server
from execlog.routers import PathRouter


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def threaded_start_then_join(server):
    thread = threading.Thread(target=server.start)

    # start the server; is a blocking call in that thread
    thread.start()

    # short wait here in main thread for some startup procedures
    time.sleep(1)

    # call shutdown from this thread
    server.shutdown()

    # join the thread back to main thread; if successfully started but shutdown failed,
    # joining back would cause indefinite blockage
    thread.join()

    # doesn't appear to be a more formal way to check if server is officially running;
    # done a lot of digging here. No flags, state; I imagine it's actually difficult to
    # know if the process is actually stopped. In any case, the above logic is good enough
    # for my use case as far as I'm concerned.
    return True


def test_server_creation():
    server = Server(
        host='localhost',
        port=8778,
        root='.'
    )

    assert threaded_start_then_join(server)

def test_server_static():
    server = Server(
        host='localhost',
        port=8778,
        root='.',
        static=True
    )

    assert threaded_start_then_join(server)

def test_server_livereload():
    server = Server(
        host='localhost',
        port=8778,
        root='.',
        livereload=True,
    )

    assert threaded_start_then_join(server)

def test_server_with_listeners():
    router1 = PathRouter()
    router1.register('tests/endpoint_proxy', lambda _: 'router1 job success')

    router2 = PathRouter()
    router2.register('tests/endpoint_proxy', lambda _: 'router2 job success')

    listeners = [router1.get_listener(), router2.get_listener()]

    server = Server(
        host='localhost',
        port=8778,
        root='.',
        managed_listeners=listeners,
    )
    thread = threading.Thread(target=server.start)
    thread.start()

    # write a file to a registered endpoint
    file_a = Path('tests/endpoint_proxy/server_file')
    file_a.write_text('test text')
    file_a.unlink()

    # wait a sec
    time.sleep(2)

    # attempt to shutdown the server, join the thread back
    # successful if not blocking
    server.shutdown()
    thread.join()

    # finally check the router event logs
    assert router1.event_log[0][1] == ['router1 job success']
    assert router2.event_log[0][1] == ['router2 job success']

