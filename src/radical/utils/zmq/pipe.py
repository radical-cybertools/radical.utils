
import zmq
import msgpack


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
    def __init__(self) -> None:
        '''
        Create a `Pipe` instance which can be used for either sending (`put()`)
        or receiving (`get()` / `get_nowait()`) data.  The communication mode is
        selected by calling *either* `Pipe.connect_push()` *or*
        `Pipe.connect_pull()`.
        '''

        self._context = zmq.Context()
        self._push    = None
        self._pull    = None
        self._poller  = zmq.Poller()
        self._url     = None


    # --------------------------------------------------------------------------
    #
    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def connect_push(self, url: str = None):
        '''
        Establish this pipe instance as sending endpoint.
        '''

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        if url:
            bind = False
        else:
            bind = True
            url  = 'tcp://*:*'

        self._push = self._context.socket(zmq.PUSH)

        if bind: self._push.bind(url)
        else   : self._push.connect(url)

        self._url = self._push.getsockopt(zmq.LAST_ENDPOINT)


    # --------------------------------------------------------------------------
    #
    def connect_pull(self, url: str = None):
        '''
        Establish this Pipe as receiving endpoint.
        '''

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        if url:
            bind = False
        else:
            bind = True
            url  = 'tcp://*:*'

        self._pull = self._context.socket(zmq.PULL)

        if bind: self._pull.bind(url)
        else   : self._pull.connect(url)

        self._url = self._pull.getsockopt(zmq.LAST_ENDPOINT)
        self._poller.register(self._pull, zmq.POLLIN)


    # --------------------------------------------------------------------------
    #
    def put(self, msg):
        '''
        Send a message - if receiving endpoints are connected, exactly one of
        them will be able to receive that message.
        '''

        self._push.send(msgpack.packb(msg))


    # --------------------------------------------------------------------------
    #
    def get(self):
        '''
        Receive a message.  This call blocks until a message is available.
        '''

        return msgpack.unpackb(self._pull.recv())


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout: float = 0):
        '''
        Receive a message.  This call blocks for `timeout` seconds
        until a message is available.  If no message is available after timeout,
        `None` is returned.
        '''

        # zmq timeouts are in milliseconds
        socks = dict(self._poller.poll(timeout=(timeout * 1000)))

        if self._pull in socks:
            return msgpack.unpackb(self._pull.recv())


# ------------------------------------------------------------------------------

