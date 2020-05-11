
import zmq
import msgpack

import threading as mt

from ..atfork  import atfork
from ..config  import Config
from ..ids     import generate_id, ID_CUSTOM
from ..url     import Url
from ..misc    import get_hostip, is_string, as_string, as_bytes, as_list, noop
from ..logger  import Logger
from ..profile import Profiler

from .bridge   import Bridge
from .utils    import no_intr, log_bulk


# ------------------------------------------------------------------------------
#
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping
                          # 0:  infinite


# ------------------------------------------------------------------------------
#
def _atfork_child():
    Subscriber._callbacks = dict()                                        # noqa


atfork(noop, noop, _atfork_child)


# ------------------------------------------------------------------------------
#
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class PubSub(Bridge):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, channel=None):

        if cfg and not channel and is_string(cfg):
            # allow construction with only channel name
            channel = cfg
            cfg     = None

        if   cfg    : cfg = Config(cfg=cfg)
        elif channel: cfg = Config(cfg={'channel': channel})
        else: raise RuntimeError('PubSub needs cfg or channel parameter')

        if not cfg.channel:
            raise ValueError('no channel name provided for pubsub')

        if not cfg.uid:
            cfg.uid = generate_id('%s.bridge.%%(counter)04d' % cfg.channel,
                                  ID_CUSTOM)

        super(PubSub, self).__init__(cfg)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def type_in(self):
        return 'pub'

    @property
    def type_out(self):
        return 'sub'

    @property
    def addr_in(self):
        # protocol independent addr query
        return self.addr_pub

    @property
    def addr_out(self):
        # protocol independent addr query
        return self.addr_sub

    @property
    def addr_pub(self):
        return self._addr_pub

    @property
    def addr_sub(self):
        return self._addr_sub

    def addr(self, spec):
        if spec.lower() == self.type_in : return self.addr_put
        if spec.lower() == self.type_out: return self.addr_get


    # --------------------------------------------------------------------------
    #
    def _bridge_initialize(self):

        self._log.info('initialize bridge %s', self._uid)

        self._url        = 'tcp://*:*'
        self._lock       = mt.Lock()

        self._ctx        = zmq.Context()  # rely on GC for destruction
        self._pub        = self._ctx.socket(zmq.XSUB)
        self._pub.linger = _LINGER_TIMEOUT
        self._pub.hwm    = _HIGH_WATER_MARK
        self._pub.bind(self._url)

        self._sub        = self._ctx.socket(zmq.XPUB)
        self._sub.linger = _LINGER_TIMEOUT
        self._sub.hwm    = _HIGH_WATER_MARK
        self._sub.bind(self._url)

        # communicate the bridge ports to the parent process
        _addr_pub = as_string(self._pub.getsockopt(zmq.LAST_ENDPOINT))
        _addr_sub = as_string(self._sub.getsockopt(zmq.LAST_ENDPOINT))

        # store addresses
        self._addr_pub = Url(_addr_pub)
        self._addr_sub = Url(_addr_sub)

        # use the local hostip for bridge addresses
        self._addr_pub.host = get_hostip()
        self._addr_sub.host = get_hostip()

        self._log.info('bridge pub on  %s: %s'  % (self._uid, self._addr_pub))
        self._log.info('       sub on  %s: %s'  % (self._uid, self._addr_sub))

        # start polling for messages
        self._poll = zmq.Poller()
        self._poll.register(self._pub, zmq.POLLIN)
        self._poll.register(self._sub, zmq.POLLIN)


    # --------------------------------------------------------------------------
    #
    def _bridge_work(self):

        # we could use a zmq proxy - but we rather code it directly to have
        # proper logging, timing, etc.  But the code for the proxy would be:
        #
        #     zmq.proxy(socket_pub, socket_sub)
        #
        # That's the equivalent of the code below.

        while not self._term.is_set():

            # timeout in ms
            socks = dict(self._poll.poll(timeout=500))

            if self._sub in socks:

                # if the sub socket signals a message, it's likely
                # a topic subscription.  Forward that to the pub
                # channel, so the bridge subscribes for the respective
                # message topic.
                msg = self._sub.recv()
                self._pub.send(msg)

                self._prof.prof('subscribe', uid=self._uid, msg=msg)
              # log_bulk(self._log, msg, '~~ %s' % self.channel)


            if self._pub in socks:

                # if the pub socket signals a message, get the message
                # and forward it to the sub channel, no questions asked.
                msg = self._pub.recv()
                self._sub.send(msg)

              # self._prof.prof('msg_fwd', uid=self._uid, msg=msg)
              # log_bulk(self._log, msg, '<> %s' % self.channel)


# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url, log=None, prof=None):

        self._channel  = channel
        self._url      = as_string(url)
        self._log      = log
        self._prof     = prof
        self._lock     = mt.Lock()

        # FIXME: no uid ns
        self._uid      = generate_id('%s.pub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        if not log:
            self._log  = Logger(name=self._uid, ns='radical.utils.zmq')

        if not prof:
            self._prof = Profiler(name=self._uid, ns='radical.utils.zmq')
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect pub to %s: %s'  % (self._channel, self._url))

        self._ctx           = zmq.Context()  # rely on GC for destruction
        self._socket        = self._ctx.socket(zmq.PUB)
        self._socket.linger = _LINGER_TIMEOUT
        self._socket.hwm    = _HIGH_WATER_MARK
        self._socket.connect(self._url)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):

        assert(isinstance(topic, str )), 'invalid topic type'
        assert(isinstance(msg,   dict)), 'invalid message type'

      # self._log.debug('=== put %s : %s: %s', topic, self.channel, msg)
      # self._prof.prof('put', uid=self._uid, msg=msg)
      # log_bulk(self._log, msg, '-> %s' % self.channel)

        btopic = as_bytes(topic.replace(' ', '_'))
        bmsg   = msgpack.packb(msg)
        data   = btopic + b' ' + bmsg

        self._socket.send(data)


# ------------------------------------------------------------------------------
#
class Subscriber(object):

    # instead of creating a new listener thread for each endpoint which then, on
    # incoming messages, calls a subscriber callback, we only create *one*
    # listening thread per ZMQ endpoint address and call *all* registered
    # callbacks in that thread.  We hold those endpoints in a class dict, so
    # that all class instances share that information
    _callbacks = dict()


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_nowait(socket, lock, timeout, log, prof):

        # FIXME: add logging

        if socket.poll(flags=zmq.POLLIN, timeout=timeout):

            raw         = no_intr(socket.recv, flags=zmq.NOBLOCK)
            topic, data = raw.split(b' ', 1)
            msg         = msgpack.unpackb(data)

            return [as_string(topic), as_string(msg)]

        return None, None


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _listener(url, log, prof):

      # assert(url in Subscriber._callbacks)

        try:
            uid    = Subscriber._callbacks.get(url, {}).get('uid')
            lock   = Subscriber._callbacks.get(url, {}).get('lock')
            term   = Subscriber._callbacks.get(url, {}).get('term')
            socket = Subscriber._callbacks.get(url, {}).get('socket')

            while not term.is_set():

                # this list is dynamic
                callbacks  = Subscriber._callbacks[url]['callbacks']
                topic, msg = Subscriber._get_nowait(socket, lock, 500, log, prof)

                if topic:
                    t = as_string(topic)
                    for m in as_list(msg):
                        m = as_string(m)
                        for cb, _lock in callbacks:
                          # prof.prof('call_cb', uid=uid, msg=cb.__name__)
                            if _lock:
                                with _lock:
                                    cb(t, m)
                            else:
                                cb(t, m)
        except:
            log.exception('listener died')


    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url, topic=None, cb=None, log=None, prof=None):
        '''
        If a `topic` is given, the channel will subscribe to that topic
        immediately.

        When a callback `cb` is specified, then the Subscriber c'tor will spawn
        a separate thread which continues to listen on the channel, and the cb
        is invoked on any incoming message.  The topic will be the first, the
        message will be the second argument to the cb.
        '''

        self._channel  = channel
        self._url      = as_string(url)
        self._topic    = as_list(topic)
        self._cb       = cb
        self._log      = log
        self._prof     = prof
        self._uid      = generate_id('%s.sub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        if not self._log:
            self._log = Logger(name=self._uid, ns='radical.utils.zmq')

        if not self._prof:
            self._prof = Profiler(name=self._uid, ns='radical.utils.zmq')
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect sub to %s: %s'  % (self._channel, self._url))

        self._lock     = mt.Lock()
        self._ctx      = zmq.Context()  # rely on GC for destruction

        if url not in Subscriber._callbacks:

            s        = self._ctx.socket(zmq.SUB)
            s.linger = _LINGER_TIMEOUT
            s.hwm    = _HIGH_WATER_MARK
            s.connect(self._url)

            Subscriber._callbacks[url] = {'uid'      : self._uid,
                                          'socket'   : s,
                                          'channel'  : channel,
                                          'lock'     : mt.Lock(),
                                          'term'     : mt.Event(),
                                          'thread'   : None,
                                          'callbacks': list()}

        # only allow `get()` and `get_nowait()`
        self._interactive = True

        if topic and cb:
            self.subscribe(topic, cb)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def _start_listener(self):

        # only start if needed
        if Subscriber._callbacks[self._url]['thread']:
            return

        t = mt.Thread(target=Subscriber._listener,
                      args=[self._url, self._log, self._prof])
        t.daemon = True
        t.start()

        Subscriber._callbacks[self._url]['thread'] = t


    # --------------------------------------------------------------------------
    #
    def _stop_listener(self, force=False):

        # only stop listener if no callbacks remain registered (unless forced)
        if force or not Subscriber._callbacks[self._url]['callbacks']:
            if  Subscriber._callbacks[self._url]['thread']:
                Subscriber._callbacks[self._url]['term'  ].set()
                Subscriber._callbacks[self._url]['thread'].join()
                Subscriber._callbacks[self._url]['term'  ].unset()
                Subscriber._callbacks[self._url]['thread'] = None


    # --------------------------------------------------------------------------
    #
    def subscribe(self, topic, cb=None, lock=None):

        # if we need to serve callbacks, then open a thread to watch the socket
        # and register the callbacks.  If a thread is already runnning on that
        # channel, just register the callback.
        #
        # Note that once a thread is watching a socket, we cannot allow to use
        # `get()` and `get_nowait()` anymore, as those will interfere with the
        # thread consuming the messages,
        #
        # The given lock (if any) is used to shield concurrent cb invokations.

        if cb:

            self._interactive = False
            self._start_listener()
            Subscriber._callbacks[self._url]['callbacks'].append([cb, lock])

        sock  = Subscriber._callbacks[self._url]['socket']
        topic = topic.replace(' ', '_')
        log_bulk(self._log, topic, '~~ %s' % self.channel)

        with self._lock:
            no_intr(sock.setsockopt, zmq.SUBSCRIBE, as_bytes(topic))


    # --------------------------------------------------------------------------
    #
    def unsubscribe(self, cb):

        if self._url in Subscriber._callbacks:
            for _cb, _lock in Subscriber._callbacks[self._url]['callbacks']:
                if cb == _cb:
                    Subscriber._callbacks[self._url]['callbacks'].remove([_cb, _lock])
                    break

        self._stop_listener()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._stop_listener(force=True)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._interactive:
            raise RuntimeError('invalid get(): callbacks are registered')


        # FIXME: add timeout to allow for graceful termination
        #
        sock = Subscriber._callbacks[self._url]['socket']

        with self._lock:
            raw = no_intr(sock.recv)

        topic, data = raw.split(b' ', 1)
        msg = msgpack.unpackb(data)

        log_bulk(self._log, msg, '<- %s' % self.channel)

        return [as_string(topic), as_string(msg)]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):

        # FIXME:  does this duplicate _get_nowait? why / why not?

        if not self._interactive:
            raise RuntimeError('invalid get_nowait(): callbacks are registered')


        sock = Subscriber._callbacks[self._url]['socket']

        if no_intr(sock.poll, flags=zmq.POLLIN, timeout=timeout):

            with self._lock:
                raw = no_intr(sock.recv, flags=zmq.NOBLOCK)

            topic, data = raw.split(b' ', 1)
            msg = msgpack.unpackb(data)

            log_bulk(self._log, msg, '<- %s' % self.channel)

            return [as_string(topic), as_string(msg)]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

