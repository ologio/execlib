'''
Server

Central management object for both file serving systems (static server, live
reloading) and job execution (routing and listening). Routers and Listeners can
be started and managed independently, but a single Server instance can house,
start, and shutdown listeners in one place.

.. admonition:: TODO

    As it stands, the Server requires address and port details, effectively
    needing one of the HTTP items (static file serving or livereloading) to be
    initialized appropriately. But there is a clear use case for just managing
    disparate Routers and their associated Listeners. Should perhaps separate
    this "grouped listener" into another object, or just make the Server
    definition more flexible.
'''
import re
import asyncio
import logging
import threading
from functools import partial
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from inotify_simple import flags
from fastapi.staticfiles import StaticFiles

from execlib.handler import Handler as LREndpoint
from execlib.routers.path import PathRouter

logger = logging.getLogger(__name__)

class Server:
    '''
    Wraps up a development static file server and live reloader.
    '''
    def __init__(self) -> None:
        self.server = None

        # MT/MP server implementations can check this variable for graceful shutdowns
        self.should_shutdown = False
        self.started = False

        # used to isolate server creation logic, when applicable
        self._init_server()

    def _init_server(self) -> None:
        pass

    def start(self):
        self.started = True

    def shutdown(self):
        self.should_shutdown = True
        logger.debug("Server shutdown request received")

class ProcessServer(Server):
    '''
    general subprocess start, shutdown, output catching logic
    '''
    pass

class HTTPServer(Server):
    def __init__(
        self,
        host,
        port,
        root,
        loop = None,
    ):
        '''
        Parameters:
            host: host server address (either 0.0.0.0, 127.0.0.1, localhost)
            port: port at which to start the server
        '''
        self.host = host
        self.port = port
        self.loop = loop

        self.userver = None

        super().__init__()

    def _init_server(self) -> None:
        '''
        Set up the FastAPI server and Uvicorn hook.

        Only a single server instance is used here, optionally mounting the
        static route (if static serving enabled) and providing a websocket
        endpoint (if livereload enabled).

        Note that, when present, the livereload endpoint is registered first,
        as the order in which routes are defined matters for FastAPI apps. This
        allows ``/livereload`` to behave appropriately, even when remounting
        the root if serving static files (which, if done in the opposite order,
        would "eat up" the ``/livereload`` endpoint).
        '''
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # enable propagation and clear handlers for uvicorn internal loggers;
        # allows logging messages to propagate to my root logger
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config['loggers']['uvicorn']['propagate'] = True
        log_config['loggers']['uvicorn']['handlers'] = []
        log_config['loggers']['uvicorn.access']['propagate'] = True
        log_config['loggers']['uvicorn.access']['handlers'] = []
        log_config['loggers']['uvicorn.error']['propagate'] = False
        log_config['loggers']['uvicorn.error']['handlers'] = []

        server_args               = {}
        server_args['log_config'] = log_config
        server_args['host']       = self.host
        server_args['port']       = self.port

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            yield
            self.shutdown()

        self.server = FastAPI(lifespan=lifespan)

        uconfig = uvicorn.Config(app=self.server, loop=self.loop, **self.server_args)
        self.userver = uvicorn.Server(config=uconfig)

    def start(self) -> None:
        '''
        Start the server.

        .. admonition:: Design

            This method takes on some extra complexity in order to ensure the
            blocking Watcher and FastAPI's event loop play nicely together. The
            Watcher's ``start()`` method runs a blocking call to INotify's
            ``read()``, which obviously cannot be started directly here in the
            main thread. Here we have a few viable options:

            1. Simply wrap the Watcher's ``start`` call in a separate thread,
               e.g.,

               .. code-block:: python

                   watcher_start = partial(self.watcher.start, loop=loop)
                   threading.Thread(
                       target=self.watcher.start,
                       kwargs={'loop': loop}
                   ).start()

               This works just fine, and the watcher's registered async
               callbacks can still use the passed event loop to get messages
               sent back to open WebSocket clients.

            2. Run the Watcher's ``start`` inside a thread managed by event
               loop via ``loop.run_in_executor``:

               .. code-block:: python

                  loop.run_in_executor(
                      None,
                      partial(self.watcher.start, loop=loop)
                  )

               Given that this just runs the target method in a separate
               thread, it's very similar to option #1. It doesn't even make the
               outer loop context available to the Watcher, meaning we still
               have to pass this loop explicitly to the ``start`` method. The
               only benefit here (I think? there may actually be no difference)
               is that it keeps things under one loop, which can be beneficial
               for shutdown.

               See related discussions:

               - https://stackoverflow.com/questions/55027940/is-run-in-executor-optimized-for-running-in-a-loop-with-coroutines
               - https://stackoverflow.com/questions/70459437/how-gil-affects-python-asyncio-run-in-executor-with-i-o-bound-tasks

            Once the watcher is started, we can kick off the FastAPI server
            (which may be serving static files, handling livereload WS
             connections, or both). We provide ``uvicorn`` access to the
            manually created ``asyncio`` loop used to the run the Watcher (in a
            thread, that is), since that loop is made available to the
            ``Watcher._event_loop`` method. This ultimately allows async
            methods to be registered as callbacks to the Watcher and be ran in
            a managed loop. In this case, that loop is managed by FastAPI,
            which keeps things consistent: the Watcher can call
            ``loop.call_soon_threadsafe`` to queue up a FastAPI-based response
            _in the same FastAPI event loop_, despite the trigger for that
            response having originated from a separate thread (i.e., where the
            watcher is started). This works smoothly, and keeps the primary
            server's event loop from being blocked.

            Note that, due to the delicate Watcher behavior, we must perform a
            shutdown explicitly in order for things to be handled gracefully.
            This is done in the server setup step, where we ensure FastAPI
            calls ``watcher.stop()`` during its shutdown process.

        .. admonition:: on event loop management

            The uvicorn server is ran with ``run_until_complete``, intended as
            a long-running process to eventually be interrupted or manually
            disrupted with a call to ``shutdown()``. The ``shutdown`` call
            attempts to gracefully shutdown the uvicorn process by setting a
            ``should_exit`` flag. Upon successful shutdown, the server task
            will be considered complete, and we can then manually close the
            loop following the interruption. So a shutdown call (which is also
            attached as a lifespan shutdown callback for the FastAPI object)
            will disable listeners and shut down their thread pools, gracefully
            close up the Uvicorn server and allow the serve coroutine to
            complete, and finally close down the event loop.
        '''
        super().start()

        logger.info(f'Starting HTTP server @ http://{self.host}:{self.port}')

        self.loop.run_until_complete(self.userver.serve())
        self.loop.close()

    def shutdown(self) -> None:
        """
        Additional shutdown handling after the FastAPI event loop receives an
        interrupt.

        .. admonition:: Usage

            This is attached as a "shutdown" callback when creating the FastAPI
            instance, which generally appears to hear interrupts and propagate
            them through.

            This method can also be invoked programmatically, such as from a
            thread not handling the main event loop. Note that either of the
            following shutdown approaches of the Uvicorn server do not appear
            to work well in this case; they both stall the calling thread
            indefinitely (in the second case, when waiting on the shutdown
            result), or simply don't shutdown the server (in the first). Only
            setting ``should_exit`` and allowing for a graceful internal
            shutdown appears to both 1) handle this gracefully, and 2) shut
            down the server at all.

            .. code-block:: python

                self.loop.call_soon_threadsafe(self.userver.shutdown)

                # OR #

                future = asyncio.run_coroutine_threadsafe(
                    self.userver.shutdown(), self.loop
                )

                # and wait for shutdown
                future.result()

            The shutdown process goes as follows:

            1. Stop any managed listeners: close out listener loops and/or
               thread pools by calling ``stop()`` on each of the managed
               listeners. This prioritizes their closure so that no events can
               make their way into the queue.
            2. Gracefully shut down the wrapper Uvicorn server. This is the
               process that starts the FastAPI server instance; set the
               ``should_exit`` flag.

            If this completes successfully, in the thread where Uvicorn was
            started the server task should be considered "completed," at which
            point the event loop can be closed successfully.
        """
        # stop FastAPI server if started
        if self.userver is not None:
            def set_should_exit():
                self.userver.should_exit = True

            self.loop.call_soon_threadsafe(set_should_exit)

class StaticHTTPServer(Server):
    def __init__(
        self,
        root,
        *args,
        **kwargs
    ) -> None:
        '''
        Parameters:
            root: base path for static files _and_ where router bases are attached (i.e.,
                  when files at this path change, a reload event will be propagated to a
                  corresponding client page)
        '''
        self.root = root

        super().__init__(*args, **kwargs)

    def _init_server(self):
        super()._init_server()

        self.server.mount(
            "/",
            StaticFiles(directory=self.root),
            name="static"
        )

class LiveReloadHTTPServer(Server):
    def _init_server(self) -> None:
        super()._init_server()

        self.server.websocket_route('/livereload')(LREndpoint)

class ListenerServer(Server):
    '''
    Server abstraction to handle disparate listeners.
    '''
    def __init__(
        self,
        managed_listeners : list | None = None,
    ) -> None:
        '''
        Parameters:
            managed_listeners: auxiliary listeners to "attach" to the server process, and to
                               propagate the shutdown signal to when the server receives an
                               interrupt.
        '''
        super().__init__()

        if managed_listeners is None:
            managed_listeners = []

        self.managed_listeners = managed_listeners

    def _listener_setup(self):
        '''
        flags.MODIFY okay since we don't need to reload non-existent pages
        '''
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        #self.listener = listener.WatchFS(loop=self.loop)
        self.router = PathRouter(loop=self.loop)
        self.router.register(
            path=str(self.root), 
            func=LREndpoint.reload_clients,
            delay=100, 
            flags=flags.MODIFY,
        )

        self.listener = self.router.get_listener()
        self.managed_listeners.append(self.listener)

    def start(self):
        super().start()

        for listener in self.managed_listeners:
            #loop.run_in_executor(None, partial(self.listener.start, loop=loop))
            if not listener.started:
                listener.start()

        for listener in self.managed_listeners:
            listener.join()

    def shutdown(self):
        super().shutdown()

        # stop attached auxiliary listeners, both internal & external
        if self.managed_listeners:
            logger.info(f"Stopping {len(self.managed_listeners)} listeners...")

            for listener in self.managed_listeners:
                listener.stop()
