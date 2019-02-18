
import zmq
import time
import errno
import msgpack

import threading as mt

from .bridge  import Bridge

from ..ids    import generate_id, ID_CUSTOM
from ..url    import Url
from ..misc   import get_hostip
from ..logger import Logger


# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping


def log_bulk(log, bulk, token):

    if hasattr(bulk, 'read'):
        bulk = msgpack.unpack(bulk)

    if not isinstance(bulk, list):
        bulk = [bulk]

    if 'arg' in bulk[0]:
        bulk = [e['arg'] for e in bulk]

    if 'uid' in bulk[0]:
        for e in bulk:
            log.debug("%s: %s [%s]", token, e['uid'], e.get('state'))
    else:
        for e in bulk:
          # log.debug("%s: %s", str(token), unicode(e)[0:32])
            log.debug("%s: ?", str(token))


# --------------------------------------------------------------------------
#
# zmq will (rightly) barf at interrupted system calls.  We are able to rerun
# those calls.
#
# FIXME: how does that behave wrt. tomeouts?  We probably should include
#        an explicit timeout parameter.
#
# kudos: https://gist.github.com/minrk/5258909
#
def _uninterruptible(f, *args, **kwargs):
    cnt = 0
    while True:
        cnt += 1
        try:
            return f(*args, **kwargs)
        except zmq.ContextTerminated as e:
            return None
        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                if cnt > 10:
                    raise
                # interrupted, try again
                continue
            else:
                # real error, raise it
                raise


# ------------------------------------------------------------------------------
#
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class PubSub(Bridge):

    def __init__(self, cfg):

        super(PubSub, self).__init__(cfg)

        self._initialize_bridge()


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

    @property
    def addr_in(self):
        return self._addr_in

    @property
    def addr_out(self):
        return self._addr_out

    @property
    def type_in(self):
        return 'PUB'

    @property
    def type_out(self):
        return 'SUB'


    # --------------------------------------------------------------------------
    # 
    def _initialize_bridge(self):

        self._log.info('start bridge %s', self._uid)

        self._url        = 'tcp://*:*'

        self._ctx        = zmq.Context()  # rely on GC for destruction
        self._in         = self._ctx.socket(zmq.XSUB)
        self._in.linger  = _LINGER_TIMEOUT
        self._in.hwm     = _HIGH_WATER_MARK
        self._in.bind(self._url)

        self._out        = self._ctx.socket(zmq.XPUB)
        self._out.linger = _LINGER_TIMEOUT
        self._out.hwm    = _HIGH_WATER_MARK
        self._out.bind(self._url)

        # communicate the bridge ports to the parent process
        _addr_in  = self._in.getsockopt (zmq.LAST_ENDPOINT)
        _addr_out = self._out.getsockopt(zmq.LAST_ENDPOINT)

        # store addresses
        self._addr_in  = Url(_addr_in)
        self._addr_out = Url(_addr_out)

        # use the local hostip for bridge addresses
        self._addr_in.host  = get_hostip()
        self._addr_out.host = get_hostip()

        self._log.info('bridge in  on  %s: %s'  % (self._uid, self._addr_in ))
        self._log.info('       out on  %s: %s'  % (self._uid, self._addr_out))

        # start polling for messages
        self._poll = zmq.Poller()
        self._poll.register(self._in,  zmq.POLLIN)
        self._poll.register(self._out, zmq.POLLIN)

        # the bridge runs in a daemon thread, so that the main process will not
        # wait for it.  But, give Python's thread performance (or lack thereof),
        # this means that the user of this class should create a separate
        # process instance to host the bridge thread.
        self._bridge_thread = mt.Thread(target=self._bridge_work)
        self._bridge_thread.daemon = True
        self._bridge_thread.start()


    # --------------------------------------------------------------------------
    # 
    def wait(self, timeout=None):
        '''
        join negates the daemon thread settings, in that it stops us from
        killing the parent process w/o hanging it.  So we do a slow pull on the
        thread state.
        '''

        start = time.time()

        while True:

            if not self._bridge_thread.is_alive():
                return True

            if  timeout is not None and \
                timeout < time.time() - start:
                return False

            time.sleep(0.1)


    # --------------------------------------------------------------------------
    # 
    def _bridge_work(self):

        # we could use a zmq proxy - but we rather code it directly to have
        # proper logging, timing, etc.  But the code for the proxy would be:
        #
        #     zmq.proxy(socket_in, socket_out)
        #
        # That's the equivalent of the code below.

        try:

            while True:

                _socks = dict(_uninterruptible(self._poll.poll, timeout=1000))
                # timeout in ms

                if self._in in _socks:

                    # if any incoming socket signals a message, get the
                    # message on the subscriber channel, and forward it
                    # to the publishing channel, no questions asked.
                    msg = _uninterruptible(self._in.recv_multipart, flags=zmq.NOBLOCK)
                    _uninterruptible(self._out.send_multipart, msg)
                    log_bulk(self._log, msg, '>> %s' % self.channel)

                if self._out in _socks:
                    # if any outgoing socket signals a message, it's
                    # likely a topic subscription.  We forward that on
                    # the incoming channels to subscribe for the
                    # respective messages.
                    msg = _uninterruptible(self._out.recv_multipart)
                    _uninterruptible(self._in.send_multipart, msg)
                    log_bulk(self._log, msg, '<< %s' % self.channel)

        except Exception:
            self._log.exception('bridge failed')

        # thread ends here


# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url):

        self._channel  = channel
        self._url      = url

        self._uid      = generate_id('%s.pub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        self._log      = Logger(name=self._uid, ns='radical.utils')
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

        assert(isinstance(msg,dict)), 'invalide message type'

        topic = topic.replace(' ', '_')
        data  = msgpack.packb(msg) 

        log_bulk(self._log, msg, '-> %s' % self.channel)

        msg = [topic, data]
        _uninterruptible(self._q.send_multipart, msg)


# ------------------------------------------------------------------------------
#
class Subscriber(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url):

        self._channel  = channel
        self._url      = url

        self._uid      = generate_id('%s.sub.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        self._log      = Logger(name=self._uid, ns='radical.utils')
        self._log.info('connect sub to %s: %s'  % (self._channel, self._url))

        self._ctx      = zmq.Context()  # rely on GC for destruction
        self._q        = self._ctx.socket(zmq.SUB)
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
    def subscribe(self, topic):

        topic = topic.replace(' ', '_')

        log_bulk(self._log, topic, '~~ %s' % self.channel)
        _uninterruptible(self._q.setsockopt, zmq.SUBSCRIBE, topic)


    # --------------------------------------------------------------------------
    #
    def get(self):

        # FIXME: add timeout to allow for graceful termination

        topic, data = _uninterruptible(self._q.recv_multipart)
        msg         = msgpack.unpackb(data) 

        log_bulk(self._log, msg, '<- %s' % self.channel)

        return [topic, msg]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            data  = _uninterruptible(self._q.recv_multipart, flags=zmq.NOBLOCK)
            topic = None

            # we run into an intermitant failure mode where the topic element of
            # the multipart message is duplicated, but only when a *previous*
            # receive got interrupted.  We correct this here
            # FIXME: understand *why* this happens
            if isinstance(data, list):
                if len(data) == 2:
                    topic, data = data[0], data[1]
                elif len(data) == 3 and data[0] == data[1]:
                    topic, data = data[1], data[2]

            if not topic or not data:
                return [None, None]

            msg = msgpack.unpackb(data)

            log_bulk(self._log, msg, '<- %s' % self.channel)

            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

