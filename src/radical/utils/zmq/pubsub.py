
import zmq
import msgpack

import threading as mt

from .bridge  import Bridge, no_intr, log_bulk

from ..ids    import generate_id, ID_CUSTOM
from ..url    import Url
from ..misc   import get_hostip, as_string, as_bytes, as_list
from ..config import Config
from ..logger import Logger


# ------------------------------------------------------------------------------
#
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping


# ------------------------------------------------------------------------------
#
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class PubSub(Bridge):

    def __init__(self, cfg):

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
        return self.addr_pub

    @property
    def addr_out(self):
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
        _addr_pub = as_string(self._pub.getsockopt (zmq.LAST_ENDPOINT))
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

        self._log.info('initialized bridge %s', self._uid)


    # --------------------------------------------------------------------------
    #
    def _bridge_work(self):

        # we could use a zmq proxy - but we rather code it directly to have
        # proper logging, timing, etc.  But the code for the proxy would be:
        #
        #     zmq.proxy(socket_pub, socket_sub)
        #
        # That's the equivalent of the code below.

        try:

            while not self._term.is_set():

                # timeout in ms
                _socks = dict(no_intr(self._poll.poll, timeout=500))

                if self._sub in _socks:

                    # if the sub socket signals a message, it's likely a topic
                    # subscription.  Forward that to the pub channel, so the
                    # bridge subscribes for the respective message topic.
                    with self._lock:
                        msg = no_intr(self._sub.recv_multipart)
                        no_intr(self._pub.send_multipart, msg)

                    self._log.debug('~~ %s: %s', self.channel, msg)
                  # log_bulk(self._log, msg, '~~ %s' % self.channel)


                if self._pub in _socks:

                    # if the pub socket signals a message, get the message and
                    # forward it to the sub channel, no questions asked.
                    # TODO: check topic filtering
                    with self._lock:
                        msg = no_intr(self._pub.recv_multipart,
                                                flags=zmq.NOBLOCK)
                        no_intr(self._sub.send_multipart, msg)

                    self._log.debug('<> %s: %s', self.channel, msg)
                  # log_bulk(self._log, msg, '<> %s' % self.channel)

        except:
            self._log.exception('bridge failed')


# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url, log=None):

        self._channel  = channel
        self._url      = url
        self._log      = log
        self._lock     = mt.Lock()

        # FIXME: no uid ns
        self._uid      = generate_id('%s.pub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        if not log:
            self._log  = Logger(name=self._uid, ns='radical.utils')

        self._log.info('connect pub to %s: %s'  % (self._channel, self._url))

        self._ctx      = zmq.Context()  # rely on GC for destruction
        self._q        = self._ctx.socket(zmq.PUB)
        self._q.linger = _LINGER_TIMEOUT
        self._q.hwm    = _HIGH_WATER_MARK
        self._q.connect(self._url)


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

        self._log.debug('-> %s: %s', self.channel, msg)
        log_bulk(self._log, msg, '-> %s' % self.channel)

        topic = topic.replace(' ', '_')
        data  = as_bytes([topic, msgpack.packb(msg)])

        with self._lock:
            no_intr(self._q.send_multipart, data)


# ------------------------------------------------------------------------------
#
class Subscriber(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url, topic=None, cb=None, log=None):
        '''
        If a `topic` is given, the channel will subscribe to that topic
        immediately.

        When a callback `cb` is specified, then the Subscriber c'tor will spawn
        a separate thread which continues to listen on the channel, and the cb
        is invoked on any incoming message.  The topic will be the first, the
        message will be the second argument to the cb.
        '''

        self._channel  = channel
        self._url      = url
        self._topic    = topic
        self._cb       = cb
        self._log      = log
        self._uid      = generate_id('%s.sub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        if not self._log:
            self._log = Logger(name=self._uid, ns='radical.utils')

        self._log.info('connect sub to %s: %s'  % (self._channel, self._url))

        self._lock     = mt.Lock()
        self._ctx      = zmq.Context()  # rely on GC for destruction
        self._q        = self._ctx.socket(zmq.SUB)
        self._q.linger = _LINGER_TIMEOUT
        self._q.hwm    = _HIGH_WATER_MARK
        self._q.connect(self._url)

        if self._cb:

            assert(self._topic), 'callback without topics?'

            for t in as_list(self._topic):
                self.subscribe(t)

            # ------------------------------------------------------------------
            def listener():

                try:
                    while True:

                        topic, msg = self.get_nowait(500)

                        if topic:
                            for m in as_list(msg):
                                self._cb(as_string(topic), as_string(m))
                except:
                    self._log.exception('subscriber failed')
            # ------------------------------------------------------------------

            self._cb_thread = mt.Thread(target=listener)
            self._cb_thread.daemon = True
            self._cb_thread.start()


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
    def subscribe(self, topic):

        topic = topic.replace(' ', '_')

        log_bulk(self._log, topic, '~~ %s' % self.channel)
        with self._lock:
            no_intr(self._q.setsockopt, zmq.SUBSCRIBE, as_bytes(topic))


    # --------------------------------------------------------------------------
    #
    def get(self):

        # FIXME: add timeout to allow for graceful termination

        with self._lock:
            topic, data = no_intr(self._q.recv_multipart)

        msg = msgpack.unpackb(data)

        log_bulk(self._log, msg, '<- %s' % self.channel)

        return [topic, msg]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        if no_intr(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            with self._lock:
                topic, data = no_intr(self._q.recv_multipart,
                                                              flags=zmq.NOBLOCK)
            msg = msgpack.unpackb(data)

            log_bulk(self._log, msg, '<- %s' % self.channel)

            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

