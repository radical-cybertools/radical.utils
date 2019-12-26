
import os
import zmq
import time
import msgpack

import threading as mt

from .bridge  import Bridge, no_intr, log_bulk

from ..ids    import generate_id, ID_CUSTOM
from ..url    import Url
from ..misc   import get_hostip, as_string, as_bytes
from ..logger import Logger


# FIXME: the log bulk method is frequently called and slow

# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping


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

            input \\              // output
                     == bridge ==
            input //              \\ output

        ie. any number of inputs can 'zmq.push()' to a bridge (which
        'zmq.pull()'s), and any number of outputs can 'zmq.request()'
        messages from the bridge (which 'zmq.response()'s).

        The bridge is the entity which 'bind()'s network interfaces, both input
        and output type endpoints 'connect()' to it.  It is the callees
        responsibility to ensure that only one bridge of a given type exists.

        Addresses are of the form 'tcp://host:port'.  Both 'host' and 'port' can
        be wildcards for BRIDGE roles -- the bridge will report the in and out
        addresses as obj.addr_put and obj.addr_get.
        '''

        super(Queue, self).__init__(cfg)

        self._stall_hwm  = self._cfg.get('stall_hwm', 1)  # FIXME: use
        self._bulk_size  = self._cfg.get('bulk_size', 10)

        if self._bulk_size <= 0:
            self._bulk_size = 1


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
        return 'put'

    @property
    def type_out(self):
        return 'get'

    @property
    def addr_in(self):
        # protocol independent addr query
        return self._addr_put

    @property
    def addr_out(self):
        # protocol independent addr query
        return self._addr_get

    @property
    def addr_put(self):
        return self._addr_put

    @property
    def addr_get(self):
        return self._addr_get

    def addr(self, spec):
        if spec.lower() == self.type_in : return self.addr_put
        if spec.lower() == self.type_out: return self.addr_get


    # --------------------------------------------------------------------------
    #
    def _bridge_initialize(self):

        self._log.info('start bridge %s', self._uid)

        self._url        = 'tcp://*:*'
        self._lock       = mt.Lock()

        self._ctx        = zmq.Context()  # rely on GC for destruction
        self._put         = self._ctx.socket(zmq.PULL)
        self._put.linger  = _LINGER_TIMEOUT
        self._put.hwm     = _HIGH_WATER_MARK
        self._put.bind(self._url)

        self._get        = self._ctx.socket(zmq.REP)
        self._get.linger = _LINGER_TIMEOUT
        self._get.hwm    = _HIGH_WATER_MARK
        self._get.bind(self._url)

        # communicate the bridge ports to the parent process
        _addr_put = as_string(self._put.getsockopt (zmq.LAST_ENDPOINT))
        _addr_get = as_string(self._get.getsockopt(zmq.LAST_ENDPOINT))

        # store addresses
        self._addr_put = Url(_addr_put)
        self._addr_get = Url(_addr_get)

        # use the local hostip for bridge addresses
        self._addr_put.host = get_hostip()
        self._addr_get.host = get_hostip()

        self._log.info('bridge in  %s: %s'  % (self._uid, self._addr_put))
        self._log.info('       out %s: %s'  % (self._uid, self._addr_get))

        # start polling senders
        self._poll_put = zmq.Poller()
        self._poll_put.register(self._put, zmq.POLLIN)

        # start polling receivers
        self._poll_get = zmq.Poller()
        self._poll_get.register(self._get, zmq.POLLIN)


    # --------------------------------------------------------------------------
    #
    def _bridge_work(self):

        # TODO: *always* pull for messages and buffer them.  Serve requests from
        #       that buffer.

        try:

            buf = list()
            while not self._term.is_set():

                # check for incoming messages, and buffer them
                ev_put = dict(no_intr(self._poll_put.poll, timeout=0))
                active = False

                if self._put in ev_put:

                    with self._lock:
                        data = no_intr(self._put.recv)

                    msgs = msgpack.unpackb(data)

                    if isinstance(msgs, list): buf += msgs
                    else                     : buf.append(msgs)

                    active = True
                    log_bulk(self._log, msgs, '>< %s [%d]'
                                              % (self._uid, len(buf)))


                # if we don't have any data in the buffer, there is no point in
                # checking for receivers
                if buf:

                    # check if somebody wants our messages
                    ev_get = dict(no_intr(self._poll_get.poll,
                                                   timeout=0))
                    if self._get in ev_get:

                        # send up to `bulk_size` messages from the buffer
                        # NOTE: this sends partial bulks on buffer underrun
                        with self._lock:
                            req = no_intr(self._get.recv)

                        bulk   = buf[:self._bulk_size]
                        data   = msgpack.packb(bulk)
                        active = True

                        no_intr(self._get.send, data)
                        log_bulk(self._log, bulk, '<> %s [%s]'
                                                % (self._uid, req))

                        # remove sent messages from buffer
                        del(buf[:self._bulk_size])

                if not active:
                    # let CPU sleep a bit when there is nothing to do
                    # We don't want to use poll timouts since we use two
                    # competing polls and don't want the idle channel slow down
                    # the busy one.
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
        self._lock     = mt.Lock()

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

        with self._lock:
            no_intr(self._q.send, data)


# ------------------------------------------------------------------------------
#
class Getter(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url,  log=None):

        self._channel   = channel
        self._url       = url
        self._lock      = mt.Lock()

        self._uid       = generate_id('%s.get.%s' % (self._channel,
                                                    '%(counter)04d'), ID_CUSTOM)
        self._log       = log

        if not self._log:
            self._log   = Logger(name=self._uid, ns='radical.utils')

        self._log.info('connect get to %s: %s'  % (self._channel, self._url))

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

            with self._lock:
                no_intr(self._q.send_string, req)

            self._requested = True
            log_bulk(self._log, req, '>> %s [%-5s]'
                                   % (self._channel, self._requested))

        with self._lock:
            data = no_intr(self._q.recv)

        msg = msgpack.unpackb(data)
        self._requested = False
        log_bulk(self._log, msg, '-- %s [%-5s]'
                               % (self._channel, self._requested))

        return msg


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        with self._lock:  # need to protect self._requested

            if not self._requested:

                # send the request *once* per recieval (got lock above)
                req = 'request %s' % os.getpid()
                no_intr(self._q.send, as_bytes(req))

                self._requested = True
                log_bulk(self._log, req, '-> %s [%-5s]'
                                         % (self._channel, self._requested))

        if no_intr(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            with self._lock:
                data = no_intr(self._q.recv)

            msg = msgpack.unpackb(data)
            self._requested = False
            log_bulk(self._log, msg, '<- %s [%-5s]'
                                     % (self._channel, self._requested))
            return as_string(msg)

        else:
            log_bulk(self._log, None, '-- %s [%-5s]'
                                      % (self._channel, self._requested))
            return None


# ------------------------------------------------------------------------------

