# pylint: disable=protected-access

import zmq
import time

import threading as mt

from typing      import Optional

from ..atfork    import atfork
from ..config    import Config
from ..ids       import generate_id, ID_CUSTOM
from ..url       import Url
from ..misc      import as_string, as_bytes, as_list, noop
from ..host      import get_hostip
from ..logger    import Logger
from ..profile   import Profiler
from ..serialize import to_msgpack, from_msgpack

from .bridge     import Bridge
from .utils      import zmq_bind, no_intr, log_bulk, LOG_ENABLED


# ------------------------------------------------------------------------------
#
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping
                          # 0:  infinite


# ------------------------------------------------------------------------------
#
def _atfork_child():
    for subscriber in Subscriber._instances:
        subscriber._callbacks = list()                                    # noqa


atfork(noop, noop, _atfork_child)


# ------------------------------------------------------------------------------
#
class PubSub(Bridge):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel: str, cfg: Optional[dict] = None, log=None):

        if cfg:
            # create deep copy
            cfg = Config(cfg=cfg)
        else:
            cfg = Config()

        if not cfg.uid:
            cfg.uid = generate_id('%s.bridge.%%(counter)04d' % channel,
                                  ID_CUSTOM)

        super().__init__(cfg, log=log)


    # --------------------------------------------------------------------------
    #
    # protocol independent addr query
    @property
    def type_in(self):
        return 'pub'

    @property
    def type_out(self):
        return 'sub'

    @property
    def addr_in(self):
        return self.addr_pub

    @property
    def addr_out(self):
        return self.addr_sub

    # protocol dependent addr query
    @property
    def addr_pub(self):
        return self._addr_pub

    @property
    def addr_sub(self):
        return self._addr_sub


    # --------------------------------------------------------------------------
    #
    def _bridge_initialize(self):

        self._log.info('initialize bridge %s', self._uid)

        self._lock        = mt.Lock()

        self._ctx         = zmq.Context.instance()  # rely on GC for destruction
        self._xpub        = self._ctx.socket(zmq.XSUB)
        self._xpub.linger = _LINGER_TIMEOUT
        self._xpub.hwm    = _HIGH_WATER_MARK
        self._addr_pub    = zmq_bind(self._xpub)

        self._xsub        = self._ctx.socket(zmq.XPUB)
        self._xsub.linger = _LINGER_TIMEOUT
        self._xsub.hwm    = _HIGH_WATER_MARK
        self._addr_sub    = zmq_bind(self._xsub)

        self._log.info('bridge pub on  %s: %s', self._uid, self._addr_pub)
        self._log.info('       sub on  %s: %s', self._uid, self._addr_sub)

        # make sure bind is active
        time.sleep(0.1)

        # start polling for messages
        self._poll = zmq.Poller()
        self._poll.register(self._xpub, zmq.POLLIN)
        self._poll.register(self._xsub, zmq.POLLIN)


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
            socks = dict(self._poll.poll(timeout=10))

            if self._xsub in socks:

                # if the sub socket signals a message, it's likely
                # a topic subscription.  Forward that to the pub
                # channel, so the bridge subscribes for the respective
                # message topic.
                msg = self._xsub.recv()
                self._xpub.send(msg)

                self._prof.prof('subscribe', uid=self._uid, msg=msg)
                log_bulk(self._log, '~~1 %s' % self.uid, [msg])


            if self._xpub in socks:

                # if the pub socket signals a message, get the message
                # and forward it to the sub channel, no questions asked.
                msg = self._xpub.recv()
                self._xsub.send(msg)

              # self._prof.prof('msg_fwd', uid=self._uid, msg=msg)
                log_bulk(self._log, '<> %s' % self.uid, [msg])


# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url=None, log=None, prof=None, path=None):

        self._channel  = channel
        self._url      = as_string(url)
        self._log      = log
        self._prof     = prof
        self._lock     = mt.Lock()

        # FIXME: no uid ns
        self._uid = generate_id('%s.pub.%s' % (self._channel,
                                               '%(counter)04d'), ID_CUSTOM)

        if not self._url:
            self._url = Bridge.get_config(channel, path).pub

        if not log:
            if LOG_ENABLED: level = 'DEBUG_9'
            else          : level = 'ERROR'
            self._log = Logger(name=self._uid, ns='radical.utils.zmq',
                               level=level, path=path)

        if not prof:
            self._prof = Profiler(name=self._uid, ns='radical.utils.zmq',
                                  path=path)
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect pub to %s: %s', self._channel, self._url)

        self._ctx           = zmq.Context.instance()  # rely on GC for destruction
        self._socket        = self._ctx.socket(zmq.PUB)
        self._socket.linger = _LINGER_TIMEOUT
        self._socket.hwm    = _HIGH_WATER_MARK
        self._socket.connect(self._url)

        time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def url(self):
        return self._url

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):

        assert isinstance(topic, str), 'invalid topic type'

        self._log.debug_9('=== put %s : %s: %s', topic, self.channel, msg)
      # self._log.debug_9('=== put %s: %s', msg, get_stacktrace())
      # self._prof.prof('put', uid=self._uid, msg=msg)
        log_bulk(self._log, '-> %s' % topic, [msg])

        btopic = as_bytes(topic.replace(' ', '_'))
        bmsg   = to_msgpack(msg)
        data   = btopic + b' ' + bmsg

        self._socket.send(data)


# ------------------------------------------------------------------------------
#
class Subscriber(object):

    # We need to clean out some data structures on fork to avoid invalid sockets
    # and deadlock.  For that purpose we keep a list of Subscriber instances
    # around
    _instances = list()


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_nowait(socket, lock, timeout, log, prof):

        # FIXME: add logging

        if socket.poll(flags=zmq.POLLIN, timeout=timeout):

            data        = no_intr(socket.recv, flags=zmq.NOBLOCK)
            topic, bmsg = data.split(b' ', 1)
            msg         = from_msgpack(bmsg)

            log.debug_9(' <- %s: %s', topic, msg)

            return [as_string(topic), as_string(msg)]

        return None, None


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _listener(sock, lock, term, callbacks, log, prof):

        try:
            while not term.is_set():

                # this list is dynamic
                topic, msg = Subscriber._get_nowait(sock, lock, 500, log, prof)

                log.debug_9(' <- %s: %s', topic, msg)

                if topic:
                    for cb, _lock in callbacks:
                      # prof.prof('call_cb', uid=uid, msg=cb.__name__)
                        try:
                            if _lock:
                                with _lock:
                                    cb(topic, msg)
                            else:
                                cb(topic, msg)
                        except SystemExit:
                            log.info('callback called sys.exit')
                            term.set()
                            break

                        except:
                            log.exception('callback error')
        except:
            log.exception('listener died')


    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url=None, topic=None, cb=None,
                                log=None, prof=None, path=None):
        '''
        If a `topic` is given, the channel will subscribe to that topic
        immediately.

        When a callback `cb` is specified, then the Subscriber c'tor will spawn
        a separate thread which continues to listen on the channel, and the cb
        is invoked on any incoming message.  The topic will be the first, the
        message will be the second argument to the cb.
        '''

        Subscriber._instances.append(self)

        self._channel   = channel
        self._url       = as_string(url)
        self._topics    = as_list(topic)
        self._log       = log
        self._prof      = prof

        self._lock      = mt.Lock()
        self._term      = mt.Event()
        self._callbacks = list()
        self._thread    = None
        self._uid       = generate_id('%s.sub.%s' % (self._channel,
                                                    '%(counter)04d'), ID_CUSTOM)

        if not self._topics:
            self._topics = []

        if not self._url:
            self._url = Bridge.get_config(channel, path).sub

        if not self._url:
            raise ValueError('no contact url specified, no config found')

        if not self._log:
            if LOG_ENABLED: level = 'DEBUG_9'
            else          : level = 'ERROR'
            self._log = Logger(name=self._uid, ns='radical.utils.zmq',
                               level=level)

        if not self._prof:
            self._prof = Profiler(name=self._uid, ns='radical.utils.zmq')
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect sub to %s: %s', self._channel, self._url)

        self._ctx         = zmq.Context.instance()  # rely on GC for destruction
        self._sock        = self._ctx.socket(zmq.SUB)
        self._sock.linger = _LINGER_TIMEOUT
        self._sock.hwm    = _HIGH_WATER_MARK
        self._sock.connect(self._url)

        time.sleep(0.1)

        # only allow `get()` and `get_nowait()`
        self._interactive = True

        for topic in self._topics:
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
    def url(self):
        return self._url

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def _start_listener(self):

        # only start if needed
        if self._thread:
            return

        lock      = self._lock
        term      = self._term
        callbacks = self._callbacks

        self._log.info('start listener for %s', self._channel)

        t = mt.Thread(target=Subscriber._listener,
                      args=[self._sock, lock, term, callbacks,
                            self._log, self._prof])
        t.daemon = True
        t.start()

        self._thread = t


    # --------------------------------------------------------------------------
    #
    def _stop_listener(self, force=False):

        # only stop listener if no callbacks remain registered (unless forced)
        if force or not self._callbacks:
            if  self._thread:
                self._term.set()
                self._thread.join()
                self._term.clear()
                self._thread = None


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
            self._callbacks.append([cb, lock])

        topic = str(topic).replace(' ', '_')
        log_bulk(self._log, '~~2 %s' % topic, [topic])

        with self._lock:
            self._log.debug_9('==== subscribe for %s', topic)
            no_intr(self._sock.setsockopt, zmq.SUBSCRIBE, as_bytes(topic))

        if topic not in self._topics:
            self._topics.append(topic)


    # --------------------------------------------------------------------------
    #
    def unsubscribe(self, cb):

        for _cb, _lock in self._callbacks:
            if cb == _cb:
                self._callbacks.remove([_cb, _lock])
                break

        if not self._callbacks:
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
        with self._lock:
            data = no_intr(self._sock.recv)

        topic, bmsg = data.split(b' ', 1)
        msg = from_msgpack(bmsg)

        log_bulk(self._log, '<- %s' % topic, [msg])

        return [as_string(topic), as_string(msg)]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):

        # FIXME:  does this duplicate _get_nowait? why / why not?

        if not self._interactive:
            raise RuntimeError('invalid get_nowait(): callbacks are registered')

        if no_intr(self._sock.poll, flags=zmq.POLLIN, timeout=timeout):

            with self._lock:
                data = no_intr(self._sock.recv, flags=zmq.NOBLOCK)

            topic, bmsg = data.split(b' ', 1)
            msg = from_msgpack(bmsg)

            log_bulk(self._log, '<- %s' % topic, [msg])

            return [as_string(topic), as_string(msg)]

        else:
            return [None, None]


# ------------------------------------------------------------------------------
# pylint: disable=unreachable
def test_pubsub(channel, addr_pub, addr_sub):

    topic = 'test'

    c_a  = 1
    c_b  = 2
    data = dict()

    for i in 'ABCD':
        data[i] = dict()
        for j in 'AB':
            data[i][j] = 0

    def cb(uid, topic, msg):
        if 'idx' not in msg:
            return
        if msg['idx'] is None:
            return False
        data[uid][msg['src']] += 1

    cb_C = lambda t,m: cb('C', t, m)
    cb_D = lambda t,m: cb('D', t, m)

    Subscriber(channel=channel, url=addr_sub, topic=topic, cb=cb_C)
    Subscriber(channel=channel, url=addr_sub, topic=topic, cb=cb_D)

    # --------------------------------------------------------------------------
    def work_pub(uid, n, delay):

        pub = Publisher(channel=channel, url=addr_pub)
        idx = 0

        while idx < n:
            time.sleep(delay)
            pub.put(topic, {'src': uid, 'idx': idx})
            idx += 1
            data[uid][uid] += 1

        # send EOF
        pub.put(topic, {'src': uid, 'idx': None})
    # --------------------------------------------------------------------------

    t_a = mt.Thread(target=work_pub, args=['A', c_a, 0.001])
    t_b = mt.Thread(target=work_pub, args=['B', c_b, 0.001])

    t_a.start()
    t_b.start()

    t_a.join()
    t_b.join()

    time.sleep(0.1)

    assert data['A']['A'] == c_a
    assert data['B']['B'] == c_b

    assert data['C']['A'] + data['C']['B'] + \
           data['D']['A'] + data['D']['B'] == 2 * (c_a + c_b)

  # print('==== %.1f %s [%s]' % (time.time(), channel, get_caller_name()))

    return data


# ------------------------------------------------------------------------------

