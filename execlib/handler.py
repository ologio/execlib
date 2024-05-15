'''
Handler

Websocket endpoint subclass intended to route websocket connections in a Server context. 

Note: the current Handler class is very specific, tailored entirely to handling a
supported live-reload handshake. This should likely be made more general, but until
separate handshakes or endpoints are needed, it's fine as is.
'''
import re
import logging
from pathlib import Path

from inotify_simple import flags
from starlette.endpoints import WebSocketEndpoint


logger = logging.getLogger(__name__)

#page_re = re.compile(r'https?:\/\/.*?\/(.*?)(?:\?|\.html|$)')
page_re = re.compile(r'https?:\/\/.*?\/(.*?)$')

def client_reload_wrap(reloaded_file):
    rpath = Path(reloaded_file)
    static_extensions = ['.js', '.css']

    if rpath.suffix in static_extensions:
        return lambda _: True
    else:
        return lambda c: Path(c).with_suffix('.html') == rpath
    

class Handler(WebSocketEndpoint):
    '''
    Subclasses WebSocketEndpoint to be attached to live reload endpoints.

    .. admonition:: Reload model

        - Served HTML files are generated from templates that include livereload JS and the
          target livereload server (port manually set prior to site build).
        - When pages are visited (be they served from NGINX or via the development
          server), the livereload.js attempts to connect to the known livereload WS
          endpoint.
        - FastAPI routes the request to _this_ endpoint, and ``on_connect`` is called.
        - Upon successful connection, the livereload JS client sends a "hello" message.
          This is picked up as the first post-acceptance message, and captured by the
          ``on_receive`` method.
        - ``on_receive`` subsequently initiates a formal handshake, sending back a "hello"
          command and waiting the "info" command from the client.
        - If the "info" command is received successfully and contains the requesting
          page's URL, the handshake completes and the websocket is added to the class'
          ``live_clients`` tracker.
        - Later, when a file in a watch path of the server's watcher is _modified_,
          ``reload_clients`` will be called from within the originating server's event loop,
          and pass in the FS event associated with the file change. ``client_reload_wrap``
          is used to wrap a boolean checker method for whether or not to reload clients
          given the FS event.

          TODO: flesh out the reload wrapper to incorporate more complex filters and/or
          transformations when determining when to reload certain clients.
    '''
    encoding = 'json'
    live_clients = {}

    async def on_connect(self, websocket):
        await websocket.accept()

    async def on_receive(self, websocket, data):
        '''
        .. admonition:: On page names

            When websockets connect, they simply communicate the exact URL from the origin
            page. The websocket is then indexed to possibly variable page names (often
            without an ``.html`` suffix, but occasionally with). The ``client_reload_wrap`` is
            then responsible for taking this client page name and normalizing it to be
            matched with full file names (i.e., suffix always included).
        '''
        url = await self._lr_handshake(websocket, data)

        if url is None:
            logger.warning('Client handshake failed, ignoring')
            return

        origin_m = page_re.search(url)
        if origin_m is not None:
            origin_page = origin_m.group(1) 

            # assume index.html if connected to empty name
            if origin_page == '':
                origin_page = 'index.html'
        else:
            origin_page = '<unidentified>.null'

        self.live_clients[origin_page] = websocket
        logger.info(f'Reloader connected to [{origin_page}] ({len(self.live_clients)} live clients)')

    async def on_disconnect(self, websocket, close_code):
        remove_page = None
        for page, ws in self.live_clients.items():
            if ws == websocket:
                remove_page = page

        if remove_page is not None:
            logger.info(f'Client for [{remove_page}] disconnected, removing')
            self.live_clients.pop(remove_page)

    @classmethod
    async def reload_clients(cls, event):
        '''
        Method targeted as a watcher callback. This async method is scheduled in a
        thread-safe manner by the watcher to be ran in the FastAPI event loop.
        '''
        logger.info(f'> [{event.name}] changed on disk')
        should_reload = client_reload_wrap(event.name)

        for page, ws in cls.live_clients.items():
            if should_reload(page):
                logger.info(f'>> Reloading client for [{page}]')
                await ws.send_json({
                    'command' : 'reload',
                    'path'    : page,
                    'liveCSS' : True,
                    'liveImg' : True,
                })
        
    @staticmethod
    async def _lr_handshake(websocket, hello):
        '''
        Handshake with livereload.js

        1. client send 'hello'
        2. server reply 'hello'
        3. client send 'info'
        '''
        # 1. await client hello after accept
        #hello = await websocket.receive_json()

        if hello.get('command') != 'hello':
            logger.warning('Client handshake failed at "hello" stage')
            return 
        
        # 2. send hello to client
        await websocket.send_json({
            'command': 'hello',
            'protocols': [
                'http://livereload.com/protocols/official-7',
            ],
            'serverName': 'livereload-tornado',
        })

        # 3. await info response
        info = await websocket.receive_json()

        if info.get('command') != 'info':
            logger.warning('Client handshake failed at "info" stage')
            return None
        elif 'url' not in info:
            logger.warning('Info received from client, but no URL provided')
            return None

        return info['url']
