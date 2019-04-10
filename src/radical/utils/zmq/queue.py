
import os
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

    if not bulk:
      # log.debug("%s: None", token)
        return

    if not isinstance(bulk, list):
        bulk = [bulk]

    if isinstance(bulk[0], dict) and 'arg' in bulk[0]:
        bulk = [e['arg'] for e in bulk]

    if isinstance(bulk[0], dict) and 'uid' in bulk[0]:
        for e in bulk:
            log.debug("%s: %s [%s]", token, e['uid'], e.get('state'))
    else:
        for e in bulk:
            log.debug("%s: ?", str(token))
            log.debug("%s: %s", token, unicode(e)[0:32])
            log.debug("%s: %s", token, str(e)[0:32])


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
# Communication between components is done via queues.  Queues are
# uni-directional, ie. Queues have an input-end for which one can call 'put()',
# and and output-end, for which one can call 'get()'.
#
# The semantics we expect (and which is what is matched by the native Python
# `Queue.Queue`), is:
#
#   - multiple upstream   components put messages onto the same queue (input)
#   - multiple downstream components get messages from the same queue (output)
#   - local order of messages is maintained: order of messages pushed onto the
#     *same* input is preserved when pulled on any output
#   - message routing is fair: whatever downstream component calls 'get' first
#     will get the next message
#
# We implement the interface of Queue.Queue:
#
#   put(msg)
#   get()
#   get_nowait()
#
# Not implemented is, at the moment:
#
#   qsize
#   empty
#   full 
#   put(msg, block, timeout)
#   put_nowait
#   get(block, timeout)
#   task_done
#
# Our Queue additionally takes 'name', 'role' and 'address' parameter on the
# constructor.  'role' can be 'input', 'bridge' or 'output', where 'input' is
# the end of a queue one can 'put()' messages into, and 'output' the end of the
# queue where one can 'get()' messages from. A 'bridge' acts as as a message
# forwarder.  'address' denominates a connection endpoint, and 'name' is
# a unique identifier: if multiple instances in the current process space use
# the same identifier, they will get the same queue instance (are connected to
# the same bridge).
#
class Queue(Bridge):

    def __init__(self, cfg):
        '''
        This Queue type sets up an zmq channel of this kind:

            input \            / output
                   -- bridge -- 
            input /            \ output

        ie. any number of inputs can 'zmq.push()' to a bridge (which
        'zmq.pull()'s), and any number of outputs can 'zmq.request()' 
        messages from the bridge (which 'zmq.response()'s).

        The bridge is the entity which 'bind()'s network interfaces, both input
        and output type endpoints 'connect()' to it.  It is the callees
        responsibility to ensure that only one bridge of a given type exists.

        Addresses are of the form 'tcp://host:port'.  Both 'host' and 'port' can
        be wildcards for BRIDGE roles -- the bridge will report the in and out
        addresses as obj.addr_in and obj.addr_out.
        '''

        super(Queue, self).__init__(cfg)

        self._channel    = self._cfg['name']
        self._stall_hwm  = self._cfg.get('stall_hwm', 1)  # FIXME: use
        self._bulk_size  = self._cfg.get('bulk_size', 10)

        if self._bulk_size <= 0:
            self._bulk_size = 1

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
        return 'PUT'

    @property
    def type_out(self):
        return 'GET'


    # --------------------------------------------------------------------------
    # 
    def _initialize_bridge(self):

        self._log.info('start bridge %s', self._uid)

        self._url        = 'tcp://*:*'

        self._ctx        = zmq.Context()  # rely on GC for destruction
        self._in         = self._ctx.socket(zmq.PULL)
        self._in.linger  = _LINGER_TIMEOUT
        self._in.hwm     = _HIGH_WATER_MARK
        self._in.bind(self._url)

        self._out        = self._ctx.socket(zmq.REP)
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

        self._log.info('bridge in  %s: %s'  % (self._uid, self._addr_in ))
        self._log.info('       out %s: %s'  % (self._uid, self._addr_out))

        # start polling senders
        self._poll_in = zmq.Poller()
        self._poll_in.register(self._in, zmq.POLLIN)

        # start polling receivers
        self._poll_out = zmq.Poller()
        self._poll_out.register(self._out, zmq.POLLIN)

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
        join would negate the daemon thread settings, in that it stops us from
        killing the parent process w/o hanging it.  So we do a slow pull on the
        thread state instead.
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

        # TODO: *always* pull for messages and buffer them.  Serve requests from
        #       that buffer.

        try:

            buf = list()

            i = -1
            while True:

                i += 1

                # we avoid busy pulling during inactivity
                active = False

                # check for incoming messages, and buffer them
                ev_in = dict(_uninterruptible(self._poll_in.poll, timeout=0))

                if self._in in ev_in:

                    active = True
                    data   = _uninterruptible(self._in.recv)
                    msgs   = msgpack.unpackb(data) 

                    if isinstance(msgs, list): buf += msgs
                    else                     : buf.append(msgs)

                    log_bulk(self._log, msgs, '>< %s [%d]'
                                              % (self._channel, len(buf)))

                # if we don't have any data in the buffer, there is no point in
                # checking for receivers
                if buf:
                    # check if somebody wants our messages
                    ev_out = dict(_uninterruptible(self._poll_out.poll,
                                                   timeout=0))

                    if self._out in ev_out:

                        # send up to `bulk_size` messages from the buffer
                        # NOTE: this sends partial bulks on buffer underrun
                        active = True
                        req    = _uninterruptible(self._out.recv)
                        bulk   = buf[:self._bulk_size]
                        data   = msgpack.packb(bulk) 
                        _uninterruptible(self._out.send, data)

                        log_bulk(self._log, bulk, '<> %s' % (self._channel))

                        # remove sent messages from buffer
                        del(buf[:self._bulk_size])

                if active:
                    # keep this bridge alive
                    self.heartbeat()

                else:
                    # let CPU sleep a bit when there is nothing to do
                    time.sleep(0.01)

        except  Exception:
            self._log.exception('bridge failed')


# ------------------------------------------------------------------------------
#
class Putter(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url):

        self._channel  = channel
        self._url      = url

        self._uid      = generate_id('%s.put.%s' % (self._channel,
                                                   '%(counter)04d'), ID_CUSTOM)
        self._log      = Logger(name=self._uid, ns='radical.utils')
        self._log.info('connect put to %s: %s'  % (self._channel, self._url))

        self._ctx      = zmq.Context()  # rely on GC for destruction
        self._q        = self._ctx.socket(zmq.PUSH)
        self._q.linger = _LINGER_TIMEOUT
        self._q.hwm    = _HIGH_WATER_MARK
        self._q.connect(self._url)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return 'Putter(%s @ %s)'  % (self.channel, self._url)

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
    def put(self, msg):

        log_bulk(self._log, msg, '-> %s' % self._channel)
        data = msgpack.packb(msg) 
        _uninterruptible(self._q.send, data)


# ------------------------------------------------------------------------------
#
class Getter(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url):

        self._channel   = channel
        self._url       = url

        self._uid       = generate_id('%s.get.%s' % (self._channel, 
                                                    '%(counter)04d'), ID_CUSTOM)
        self._log       = Logger(name=self._uid, ns='radical.utils')
        self._log.info('connect get to %s: %s'  % (self._channel, self._url))

        self._lock      = mt.RLock()
        self._requested = False          # send/recv sync

        self._ctx       = zmq.Context()  # rely on GC for destruction
        self._q         = self._ctx.socket(zmq.REQ)
        self._q.linger  = _LINGER_TIMEOUT
        self._q.hwm     = _HIGH_WATER_MARK
        self._q.connect(self._url)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return 'Getter(%s @ %s)'  % (self.channel, self._url)

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
    def get(self):

        if not self._requested:
            req = 'Request %s' % os.getpid()
            _uninterruptible(self._q.send, req)
            self._requested = True
            log_bulk(self._log, req, '>> %s [%-5s]' % (self._channel, self._requested))

        data = _uninterruptible(self._q.recv)
        msg  = msgpack.unpackb(data) 
        self._requested = False
        log_bulk(self._log, msg, '-- %s [%-5s]' % (self._channel, self._requested))

        return msg


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        with self._lock:  # need to protect self._requested

            if not self._requested:
                # we can only send the request once per recieval
                req = 'request %s' % os.getpid()
                _uninterruptible(self._q.send, req)
                self._requested = True
                log_bulk(self._log, req, '-> %s [%-5s]' % (self._channel, self._requested))

            if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):
                data = _uninterruptible(self._q.recv)
                msg  = msgpack.unpackb(data) 
                self._requested = False
                log_bulk(self._log, msg, '<- %s [%-5s]' % (self._channel, self._requested))
                return msg

            else:
                log_bulk(self._log, None, '-- %s [%-5s]' % (self._channel, self._requested))
                return None


# ------------------------------------------------------------------------------

