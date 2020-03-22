
import os
import zmq
import time
import msgpack

import threading as mt

from .bridge  import Bridge, no_intr, log_bulk

from ..atfork import atfork
from ..config import Config
from ..ids    import generate_id, ID_CUSTOM
from ..url    import Url
from ..misc   import get_hostip, is_string, as_string, as_bytes, as_list, noop
from ..logger import Logger


# FIXME: the log bulk method is frequently called and slow

# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT    =  250  # ms to linger after close
_HIGH_WATER_MARK   =    0  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE = 1024  # number of messages to put in a bulk


# ------------------------------------------------------------------------------
#
def _atfork_child():
    Getter._callbacks = dict()                                            # noqa


atfork(noop, noop, _atfork_child)


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

    def __init__(self, cfg=None, channel=None):
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

        if cfg and not channel and is_string(cfg):
            # allow construction with only channel name
            channel = cfg
            cfg     = None

        if   cfg    : cfg = Config(cfg=cfg)
        elif channel: cfg = Config(cfg={'channel': channel})
        else: raise RuntimeError('Queue needs cfg or channel parameter')

        if not cfg.channel:
            raise ValueError('no channel name provided for queue')

        if not cfg.uid:
            cfg.uid = generate_id('%s.bridge.%%(counter)04d' % cfg.channel,
                                  ID_CUSTOM)

        super(Queue, self).__init__(cfg)

        self._bulk_size  = self._cfg.get('bulk_size', 0)

        if self._bulk_size <= 0:
            self._bulk_size = _DEFAULT_BULK_SIZE


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
        self._url      = as_string(url)
        self._lock     = mt.Lock()

        self._uid      = generate_id('%s.put.%%(counter)04d' % self._channel,
                                     ID_CUSTOM)
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

    # instead of creating a new listener thread for each endpoint which then, on
    # incoming messages, calls a getter callback, we only create *one*
    # listening thread per ZMQ endpoint address and call *all* registered
    # callbacks in that thread.  We hold those endpoints in a class dict, so
    # that all class instances share that information
    _callbacks = dict()


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_nowait(url, timeout, log):  # timeout in ms

        info = Getter._callbacks[url]

        with info['lock']:

          # log.debug('=== %s  %s  %s', info['lock'], info['socket'],
          #           info['requested'])

            if not info['requested']:

                # send the request *once* per recieval (got lock above)
                req = 'request %s' % os.getpid()
                no_intr(info['socket'].send, as_bytes(req))
                info['requested'] = True

                log_bulk(log, req, '-> %s (2) [%-5s]' % (info['channel'],
                                                         info['requested']))


            if no_intr(info['socket'].poll, flags=zmq.POLLIN, timeout=timeout):

                data = no_intr(info['socket'].recv)
                info['requested'] = False

                msg = msgpack.unpackb(data)
                log_bulk(log, msg, '<- %s (2) [%-5s]' % (info['channel'], info['requested']))
                return as_string(msg)

            else:
                log_bulk(log, None, '-- %s [%-5s]' % (info['channel'], info['requested']))
                return None


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _listener(url, log):
        '''
        other than the pubsub listener, the queue listener will not deliver
        an incoming message to all subscribers, but only to exactly *one*
        subscriber.  We this perform a round-robin over all known callbacks
        '''

        assert(url in Getter._callbacks)
        time.sleep(1)

        try:
            idx = 0  # round-robin cb index
            while True:

                # this list is dynamic
                callbacks = Getter._callbacks[url]['callbacks']

                if not callbacks:
                    print('no cb')
                    time.sleep(0.01)
                    continue

                msg = Getter._get_nowait(url, 500, log)

              # log_bulk(log, msg, '>> msg in listener (%s)' %
              #         [cb[0].__name__ for cb in Getter._callbacks[url]['callbacks']])

                if msg:
                    for m in as_list(msg):

                        idx += 1
                        if idx >= len(callbacks):  # FIXME: lock callbacks
                            idx = 0

                        cb, _lock = callbacks[idx]
                      # log.debug('==== %s [%s] <- %s', cb.__name__, idx, m)
                        if _lock:
                            with _lock:
                                cb(as_string(m))
                        else:
                            cb(as_string(m))

        except:
            log.exception('listener died')

    # --------------------------------------------------------------------------
    #
    def _start_listener(self):

      # import pprint
      # self._log.debug(' === X 0 %s: %s : %s', self._channel, self._url,
      #         pprint.pformat(Getter._callbacks))

        # only start if needed
        if Getter._callbacks[self._url]['thread']:
            return

      # self._log.debug(' === X 1 %s', self._channel)

        t = mt.Thread(target=Getter._listener, args=[self._url, self._log])
        t.daemon = True
        t.start()
      # self._log.debug(' === X 2 %s', self._channel)

        Getter._callbacks[self._url]['thread'] = t
      # self._log.debug(' === X 3 %s', self._channel)


    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url, cb=None, log=None):
        '''
        When a callback `cb` is specified, then the Getter c'tor will spawn
        a separate thread which continues to listen on the channel, and the
        cb is invoked on any incoming message.  The message will be the only
        argument to the cb.
        '''

        self._channel   = channel
        self._url       = as_string(url)
        self._lock      = mt.Lock()
        self._log       = log
        self._uid       = generate_id('%s.get.%%(counter)04d' % self._channel,
                                      ID_CUSTOM)

        if not self._log:
            self._log   = Logger(name=self._uid, ns='radical.utils')

        self._log.info('connect get to %s: %s'  % (self._channel, self._url))

        self._requested = False          # send/recv sync
        self._ctx       = zmq.Context()  # rely on GC for destruction
        self._q         = self._ctx.socket(zmq.REQ)
        self._q.linger  = _LINGER_TIMEOUT
        self._q.hwm     = _HIGH_WATER_MARK
        self._q.connect(self._url)

        if url not in Getter._callbacks:

            Getter._callbacks[url] = {'socket'   : self._q,
                                      'channel'  : self._channel,
                                      'lock'     : mt.Lock(),
                                      'requested': self._requested,
                                      'thread'   : None,
                                      'callbacks': list()}
        if cb:
          # self._log.debug('=== init cb 0 %s', cb.__name__)
            self.subscribe(cb)
          # self._log.debug('=== init cb 1 %s', cb.__name__)
        else:
            self._interactive = True


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
    def subscribe(self, cb, lock=None):

        # if we need to serve callbacks, then open a thread to watch the socket
        # and register the callbacks.  If a thread is already runnning on that
        # channel, just register the callback.
        #
        # Note that once a thread is watching a socket, we cannot allow to use
        # `get()` and `get_nowait()` anymore, as those will interfere with the
        # thread consuming the messages,
        #
        # The given lock (if any) is used to shield concurrent cb invokations.
        #
        # FIXME: clean up lock usage - see self._lock

      # self._log.debug(' === S 0 %s', cb.__name__)

        if self._url not in Getter._callbacks:

            Getter._callbacks[self._url] = {'socket'   : self._q,
                                            'channel'  : self._channel,
                                            'lock'     : mt.Lock(),
                                            'requested': self._requested,
                                            'thread'   : None,
                                            'callbacks': list()}

        Getter._callbacks[self._url]['callbacks'].append([cb, lock])

      # import pprint
      # self._log.debug(' === S 1 %s', pprint.pformat(Getter._callbacks))

        self._interactive = False
        self._start_listener()
        log_bulk(self._log, cb.__name__, '~~ %s' % self.channel)

      # self._log.debug(' === S 2 %s', cb)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._interactive:
            raise RuntimeError('invalid get(): callbacks are registered')

        if not self._requested:
            req = 'Request %s' % os.getpid()

          # self._log.debug('=== O2 %s  %s  %s', self._lock, self._q, self._requested)

            with self._lock:
                no_intr(self._q.send, as_bytes(req))
                self._requested = True

            log_bulk(self._log, req, '>> %s [%-5s]'
                                   % (self._channel, self._requested))

        with self._lock:
            data = no_intr(self._q.recv)
            self._requested = False

        msg = msgpack.unpackb(data)
        log_bulk(self._log, msg, '-- %s [%-5s]'
                               % (self._channel, self._requested))

        return as_string(msg)


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        if not self._interactive:
            raise RuntimeError('invalid get(): callbacks are registered')

        if not self._requested:

          # self._log.debug('=== O1 %s  %s  %s', self._lock, self._q, self._requested)

            # send the request *once* per recieval (got lock above)
            req = 'request %s' % os.getpid()

            with self._lock:  # need to protect self._requested
                no_intr(self._q.send, as_bytes(req))
                self._requested = True

            log_bulk(self._log, req, '-> %s (3) [%-5s]'
                                     % (self._channel, self._requested))

        if no_intr(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            with self._lock:
                data = no_intr(self._q.recv)
                self._requested = False

            msg = msgpack.unpackb(data)
            log_bulk(self._log, msg, '<- %s (3) [%-5s]'
                                     % (self._channel, self._requested))
            return as_string(msg)

        else:
            log_bulk(self._log, None, '-- %s [%-5s]'
                                      % (self._channel, self._requested))
            return None


# ------------------------------------------------------------------------------

