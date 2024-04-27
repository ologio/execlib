'''
Server

Central management object for both file serving systems (static server, live reloading)
and job execution (routing and listening). Routers and Listeners can be started and
managed independently, but a single Server instance can house, start, and shutdown
listeners in one place.

TODO: as it stands, the Server requires address and port details, effectively needing one
of the HTTP items (static file serving or livereloading) to be initialized appropriately.
But there is a clear use case for just managing disparate Routers and their associated
Listeners. Should perhaps separate this "grouped listener" into another object, or just
make the Server definition more flexible.
'''
import re
import asyncio
import logging
import threading
from functools import partial

import uvicorn
from inotify_simple import flags
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles

from execlog.handler import Handler as LREndpoint


logger = logging.getLogger(__name__)

class Server:
    '''
    Server class. Wraps up a development static file server and live reloader.
    '''
    def __init__(
        self,
        host,
        port,
        root,
        static            : bool        = False,
        livereload        : bool        = False,
        managed_listeners : list | None = None,
    ):
        '''
        Parameters:
            host:              host server address (either 0.0.0.0, 127.0.0.1, localhost)
            port:              port at which to start the server
            root:              base path for static files _and_ where router bases are attached (i.e.,
                               when files at this path change, a reload event will be
                               propagated to a corresponding client page)
            static:            whether or not to start a static file server
            livereload:        whether or not to start a livereload server
            managed_listeners: auxiliary listeners to "attach" to the server process, and to
                               propagate the shutdown signal to when the server receives an
                               interrupt.
        '''
        self.host       = host
        self.port       = port
        self.root       = root
        self.static     = static
        self.livereload = livereload

        if managed_listeners is None:
            managed_listeners = []
        self.managed_listeners = managed_listeners

        self.listener = None
        self.server   = None
        self.server_text = ''
        self.server_args = {}

        self.loop = None
        self._server_setup()

    def _wrap_static(self):
        self.server.mount("/", StaticFiles(directory=self.root), name="static")

    def _wrap_livereload(self):
        self.server.websocket_route('/livereload')(LREndpoint)
        #self.server.add_api_websocket_route('/livereload', LREndpoint)

    def _server_setup(self):
        '''
        Set up the FastAPI server. Only a single server instance is used here, optionally
        mounting the static route (if static serving enabled) and providing a websocket
        endpoint (if livereload enabled).

        Note that, when present, the livereload endpoint is registered first, as the order
        in which routes are defined matters for FastAPI apps. This allows `/livereload` to
        behave appropriately, even when remounting the root if serving static files
        (which, if done in the opposite order, would "eat up" the `/livereload` endpoint).
        '''
        # enable propagation and clear handlers for uvicorn internal loggers;
        # allows logging messages to propagate to my root logger
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config['loggers']['uvicorn']['propagate'] = True
        log_config['loggers']['uvicorn']['handlers'] = []
        log_config['loggers']['uvicorn.access']['propagate'] = True
        log_config['loggers']['uvicorn.access']['handlers'] = []
        log_config['loggers']['uvicorn.error']['propagate'] = False
        log_config['loggers']['uvicorn.error']['handlers'] = []

        self.server_args['log_config'] = log_config
        self.server_args['host'] = self.host
        self.server_args['port'] = self.port

        if self.static or self.livereload:
            self.server = FastAPI()
            self.server.on_event('shutdown')(self.shutdown)

        if self.livereload:
            self._wrap_livereload()
            self._listener_setup()
            self.server_text += '+reload'

        if self.static:
            self._wrap_static()
            self.server_text += '+static'

    def _listener_setup(self):
        '''
        flags.MODIFY okay since we don't need to reload non-existent pages
        '''
        from execlog.reloader.router import PathRouter

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
        '''
        Start the server.

        Note: Design
            This method takes on some extra complexity in order to ensure the blocking
            Watcher and FastAPI's event loop play nicely together. The Watcher's `start()`
            method runs a blocking call to INotify's `read()`, which obviously cannot be
            started directly here in the main thread. Here we have a few viable options:

            1. Simply wrap the Watcher's `start` call in a separate thread, e.g.,

               ```py
               watcher_start = partial(self.watcher.start, loop=loop)
               threading.Thread(target=self.watcher.start, kwargs={'loop': loop}).start()
               ```

               This works just fine, and the watcher's registered async callbacks can
               still use the passed event loop to get messages sent back to open WebSocket
               clients.
            2. Run the Watcher's `start` inside a thread managed by event loop via
               `loop.run_in_executor`:

               ```py
               loop.run_in_executor(None, partial(self.watcher.start, loop=loop))
               ```

               Given that this just runs the target method in a separate thread, it's very
               similar to option #1. It doesn't even make the outer loop context available
               to the Watcher, meaning we still have to pass this loop explicitly to the
               `start` method. The only benefit here (I think? there may actually be no
               difference) is that it keeps things under one loop, which can be beneficial
               for shutdown.

               See related discussions:

               - https://stackoverflow.com/questions/55027940/is-run-in-executor-optimized-for-running-in-a-loop-with-coroutines
               - https://stackoverflow.com/questions/70459437/how-gil-affects-python-asyncio-run-in-executor-with-i-o-bound-tasks

            Once the watcher is started, we can kick off the FastAPI server (which may be
            serving static files, handling livereload WS connections, or both). We
            provide `uvicorn` access to the manually created `asyncio` loop used to the
            run the Watcher (in a thread, that is), since that loop is made available to
            the `Watcher._event_loop` method. This ultimately allows async methods to be
            registered as callbacks to the Watcher and be ran in a managed loop. In this
            case, that loop is managed by FastAPI, which keeps things consistent: the
            Watcher can call `loop.call_soon_threadsafe` to queue up a FastAPI-based
            response _in the same FastAPI event loop_, despite the trigger for that
            response having originated from a separate thread (i.e., where the watcher is
            started). This works smoothly, and keeps the primary server's event loop from
            being blocked.

            Note that, due to the delicate Watcher behavior, we must perform a shutdown
            explicitly in order for things to be handled gracefully. This is done in the
            server setup step, where we ensure FastAPI calls `watcher.stop()` during its
            shutdown process.
        '''
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        for listener in self.managed_listeners:
            #loop.run_in_executor(None, partial(self.listener.start, loop=loop))
            if not listener.started:
                listener.start()

        if self.server:
            logger.info(f'Server{self.server_text} @ http://{self.host}:{self.port}')

            uconfig = uvicorn.Config(app=self.server, loop=self.loop, **self.server_args)
            userver = uvicorn.Server(config=uconfig)
            self.loop.run_until_complete(userver.serve())

    def shutdown(self):
        '''
        Additional shutdown handling after the FastAPI event loop receives an interrupt.

        Currently this 
        '''
        logger.info("Shutting down server...")

        # stop attached auxiliary listeners, both internal & external
        for listener in self.managed_listeners:
            listener.stop()
