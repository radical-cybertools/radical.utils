
import zmq
import threading as mt

from ..serialize import to_msgpack, from_msgpack
from ..logger    import Logger

from .utils import zmq_bind

MODE_PUSH = 'push'
MODE_PULL = 'pull'


# ------------------------------------------------------------------------------
#
class Pipe(object):
    '''
    The `Pipe` class provides simple and direct n-to-m connectivity without
    load balancing or caching.  The class is expected to be used in the
    following way:

      - *one* endpoint (either push or pull) establishes the pipe:
        p = Pipe()
        p.connect_push()
        print(p.url)

      - all other endpoints (either push or pull) connect to the same pipe by
        specifying the URL from the first EP:
        p1 = Pipe()
        p1.connect_pull(url)

      - data should only be sent once at least one receiving EP is established
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, mode, url=None, log=None) -> None:
        '''
        Create a `Pipe` instance which can be used for either sending (`put()`)
        or receiving (`get()` / `get_nowait()`) data. according to the specified
        mode (`MODE_PUSH` or `MODE_PULL`).

        An URL can be specified for one end of the pipe - that end will then be
        in listening mode.  The other end of the pipe MUST use the connection
        URL provided by the listening end (`Pipe.url`).
        '''

        self._context  = zmq.Context.instance()
        self._mode     = mode
        self._url      = None
        self._log      = log
        self._sock     = None
        self._poller   = zmq.Poller()
        self._cbs      = list()
        self._listener = None

        if mode == MODE_PUSH:
            self._connect_push(url)

        elif mode == MODE_PULL:
            self._connect_pull(url)

        else:
            raise ValueError('unsupported pipe mode [%s]' % mode)

        if not self._log:

            self._log = Logger('radical.utils.pipe')


    # --------------------------------------------------------------------------
    #
    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def _connect_push(self, url):
        '''
        Establish this pipe instance as sending endpoint.
        '''

        if self._sock:
            raise RuntimeError('already connected at %s' % self._url)

        self._sock = self._context.socket(zmq.PUSH)

        if url:
            self._sock.connect(url)
            self._url = url
        else:
            self._url = zmq_bind(self._sock)

        self._url = self._sock.getsockopt(zmq.LAST_ENDPOINT)


    # --------------------------------------------------------------------------
    #
    def _connect_pull(self, url):
        '''
        Establish this Pipe as receiving endpoint.
        '''

        if self._sock:
            raise RuntimeError('already connected at %s' % self._url)

        self._sock = self._context.socket(zmq.PULL)

        if url:
            self._sock.connect(url)
            self._url = url
        else:
            self._url = zmq_bind(self._sock)

        self._poller.register(self._sock, zmq.POLLIN)


    # --------------------------------------------------------------------------
    #
    def register_cb(self, cb):
        '''
        Register a callback for incoming messages.  The callback will be called
        with the message as argument.

        Only a pipe in pull mode can have callbacks registered.  Note that once
        a callback is registered, the `get()` and `get_nowait()` methods must
        not be used anymore.
        '''

        assert self._mode == MODE_PULL

        self._cbs.append(cb)

        if not self._listener:
            self._listener = mt.Thread(target=self._listen)
            self._listener.daemon = True
            self._listener.start()


    # --------------------------------------------------------------------------
    #
    def _listen(self):
        '''
        Listen for incoming messages, and call registered callbacks.
        '''

        while True:

            socks = dict(self._poller.poll(timeout=10))

            if self._sock in socks:
                msg = from_msgpack(self._sock.recv())

                for cb in self._cbs:
                    try:
                        cb(msg)
                    except:
                        self._log.exception('callback failed')


    # --------------------------------------------------------------------------
    #
    def put(self, msg):
        '''
        Send a message - if receiving endpoints are connected, exactly one of
        them will be able to receive that message.
        '''

        assert self._mode == MODE_PUSH
        self._sock.send(to_msgpack(msg))


    # --------------------------------------------------------------------------
    #
    def get(self):
        '''
        Receive a message.  This call blocks until a message is available.
        '''

        assert self._mode == MODE_PULL
        assert not self._cbs

        return from_msgpack(self._sock.recv())


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout: float = 0):
        '''
        Receive a message.  This call blocks for `timeout` seconds
        until a message is available.  If no message is available after timeout,
        `None` is returned.
        '''

        assert self._mode == MODE_PULL
        assert not self._cbs

        # zmq timeouts are in milliseconds
        socks = dict(self._poller.poll(timeout=int(timeout * 1000)))

        if self._sock in socks:
            return from_msgpack(self._sock.recv())


# ------------------------------------------------------------------------------

