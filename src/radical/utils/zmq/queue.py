# pylint: disable=protected-access, abstract-class-instantiated

import sys
import zmq
import time

import threading as mt

from typing      import Optional

from ..atfork    import atfork
from ..config    import Config
from ..ids       import generate_id, ID_CUSTOM
from ..url       import Url
from ..misc      import as_string, as_bytes, as_list, noop, find_port
from ..host      import get_hostip
from ..logger    import Logger
from ..profile   import Profiler
from ..debug     import print_exception_trace
from ..serialize import to_msgpack, from_msgpack

from .bridge     import Bridge
from .utils      import zmq_bind, no_intr
from .utils      import log_bulk, LOG_ENABLED
# from .utils    import prof_bulk


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

    def __init__(self, channel: str, cfg: Optional[dict] = None, log=None):
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

        if cfg:
            # create deep copy
            cfg = Config(cfg=cfg)
        else:
            cfg = Config()

        # ensure channel is set in config
        if cfg.channel:
            assert cfg.channel == channel
        else:
            cfg.channel = channel

        if not cfg.uid:
            cfg.uid = generate_id('%s.bridge.%%(counter)04d' % cfg.channel,
                                  ID_CUSTOM)

        super().__init__(cfg, log=log)

        self._bulk_size  = self._cfg.get('bulk_size', 0)

        if self._bulk_size <= 0:
            self._bulk_size = _DEFAULT_BULK_SIZE


    # --------------------------------------------------------------------------
    #

    # protocol independent addr query
    @property
    def type_in(self):
        return 'put'

    @property
    def type_out(self):
        return 'get'

    @property
    def addr_in(self):
        return self._addr_put

    @property
    def addr_out(self):
        return self._addr_get

    # protocol dependent addr query
    @property
    def addr_put(self):
        return self._addr_put

    @property
    def addr_get(self):
        return self._addr_get


    # --------------------------------------------------------------------------
    #
    def _bridge_initialize(self):

        self._log.info('start bridge %s', self._uid)

        self._lock       = mt.Lock()

        self._ctx        = zmq.Context()  # rely on GC for destruction
        self._put        = self._ctx.socket(zmq.PULL)
        self._put.linger = _LINGER_TIMEOUT
        self._put.hwm    = _HIGH_WATER_MARK
        self._addr_put   = zmq_bind(self._put)

        self._get        = self._ctx.socket(zmq.REP)
        self._get.linger = _LINGER_TIMEOUT
        self._get.hwm    = _HIGH_WATER_MARK
        self._addr_get   = zmq_bind(self._get)

        self._log.info('bridge in  %s: %s', self._uid, self._addr_put)
        self._log.info('bridge out %s: %s', self._uid, self._addr_get)

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

            self.nin  = 0
            self.nout = 0
            self.last = 0

            buf = dict()
            while not self._term.is_set():

                active = False

                # check for incoming messages, and buffer them
                ev_put = dict(no_intr(self._poll_put.poll, timeout=10))
              # self._prof.prof('poll_put', msg=len(ev_put))
                self._log.debug_9('polled put: %s', ev_put)

                if self._put in ev_put:

                    with self._lock:
                        data = list(no_intr(self._put.recv_multipart))
                    self._log.debug_9('recvd  put: %s', data)

                    if len(data) != 2:
                        raise RuntimeError('%d frames unsupported' % len(data))

                    qname = as_string(from_msgpack(data[0]))
                    msgs  = from_msgpack(data[1])
                  # prof_bulk(self._prof, 'poll_put_recv', msgs)
                    log_bulk(self._log, '<> %s' % qname, msgs)
                    self._log.debug_9('put %s: %s ! ', qname, len(msgs))

                    if qname not in buf:
                        buf[qname] = list()
                    buf[qname] += msgs
                    self.nin   += len(msgs)

                    active = True


                # check if somebody wants our messages
                ev_get = dict(no_intr(self._poll_get.poll, timeout=10))
              # self._prof.prof('poll_get', msg=len(ev_get))
                self._log.debug_9('polled get: %s [%s]', ev_get, self._get)

                if self._get in ev_get:

                    # send up to `bulk_size` messages from the buffer
                    # NOTE: this sends partial bulks on buffer underrun
                    with self._lock:
                        # the actual req message is ignored - we only care
                        # about who sent it
                        qname = as_string(no_intr(self._get.recv))

                    if not qname:
                        qname = 'default'

                    if qname in buf:
                        msgs = buf[qname][:self._bulk_size]
                    else:
                        self._log.debug_9('get: %s not in %s', qname,
                                                             list(buf.keys()))
                        msgs = list()

                    log_bulk(self._log, '>< %s' % qname, msgs)

                    data   = [to_msgpack(qname), to_msgpack(msgs)]
                    active = True

                  # self._log.debug_9('==== get %s: %s', qname, list(buf.keys()))
                  # self._log.debug_9('==== get %s: %s', qname, list(buf.values()))
                  # self._log.debug_9('==== get %s: %s ! [%s]', qname, len(msgs),
                  #                        [[x, len(y)] for x,y in buf.items()])
                    no_intr(self._get.send_multipart, data)
                  # prof_bulk(self._prof, 'poll_get_send', msgs=msgs, msg=req)

                    self.nout += len(msgs)
                    self.last  = time.time()

                    # remove sent messages from buffer
                    if msgs:
                        del buf[qname][:self._bulk_size]

                if not active:
                  # self._prof.prof('sleep', msg=len(buf))
                    # let CPU sleep a bit when there is nothing to do
                    # We don't want to use poll timouts since we use two
                    # competing polls and don't want the idle channel slow down
                    # the busy one.
                    time.sleep(0.1)

        except Exception:
            self._log.exception('bridge failed')

    def stop(self):
        Bridge.stop(self)


# ------------------------------------------------------------------------------
#
class Putter(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url=None, log=None, prof=None, path=None):

        self._channel  = channel
        self._url      = as_string(url)
        self._log      = log
        self._prof     = prof
        self._lock     = mt.Lock()
        self._uid      = generate_id('%s.put.%%(counter)04d' % self._channel,
                                     ID_CUSTOM)

        if not self._url:
            self._url = Bridge.get_config(channel, path).get('put')

        if not self._url:
            raise ValueError('no contact url specified, no config found')

        if not self._log:
            if LOG_ENABLED: level = 'DEBUG_9'
            else          : level = 'ERROR'
            self._log = Logger(name=self._uid, ns='radical.utils.zmq',
                               level=level, path=path)

        if not self._prof:
            self._prof = Profiler(name=self._uid, ns='radical.utils', path=path)
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect put to %s: %s', self._channel, self._url)

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
    def put(self, msgs, qname=None):

        msgs = as_list(msgs)

        if not qname:
            qname = 'default'

        log_bulk(self._log, '-> %s[%s]' % (self._channel, qname), msgs)
        data = [to_msgpack(qname), to_msgpack(msgs)]

        with self._lock:
            no_intr(self._q.send_multipart, data)
      # prof_bulk(self._prof, 'put', msgs)


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
    def _get_nowait(url, qname=None, timeout=None, uid=None):  # timeout in ms

        info = Getter._callbacks[url]

        if not qname:
            qname = 'default'

        with info['lock']:

            if LOG_ENABLED: level = 'DEBUG_9'
            else          : level = 'ERROR'
            logger = Logger(name=qname, ns='radical.utils.zmq', level=level)

            if not info['requested']:

                # send the request *once* per recieval (got lock above)
                # FIXME: why is this sent repeatedly?
                logger.debug_9('=== => from %s[%s]', uid, qname)
                no_intr(info['socket'].send, as_bytes(qname))
                info['requested'] = True


            if no_intr(info['socket'].poll, flags=zmq.POLLIN, timeout=timeout):

                data = list(no_intr(info['socket'].recv_multipart))
                info['requested'] = False

                qname = as_string(from_msgpack(data[0]))
                msgs  = as_string(from_msgpack(data[1]))
                log_bulk(logger, '<-1 %s [%s]' % (uid, qname), msgs)
                return msgs

            else:
                return None


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _listener(url, qname=None, uid=None):
        '''
        other than the pubsub listener, the queue listener will not deliver
        an incoming message to all subscribers, but only to exactly *one*
        subscriber.  We this perform a round-robin over all known callbacks
        '''

        if not qname:
            qname = 'default'

        assert url in Getter._callbacks
        time.sleep(0.1)

        try:
            term = Getter._callbacks.get(url, {}).get('term')
            idx  = 0  # round-robin cb index
            while not term.is_set():

                # this list is dynamic
                callbacks = Getter._callbacks[url]['callbacks']

                if not callbacks:
                    time.sleep(0.01)
                    continue

                msgs = Getter._get_nowait(url, qname=qname, timeout=500, uid=uid)
                BULK = True
                if msgs:

                    if BULK:
                        idx += 1
                        if idx >= len(callbacks):
                            idx = 0
                        cb, _lock = callbacks[idx]
                        if _lock:
                            with _lock:
                                cb(as_string(msgs))
                        else:
                            cb(as_string(msgs))

                    else:
                        for m in as_list(msgs):

                            idx += 1
                            if idx >= len(callbacks):
                                idx = 0

                            cb, _lock = callbacks[idx]
                            if _lock:
                                with _lock:
                                    cb(as_string(m))
                            else:
                                cb(as_string(m))

        except Exception as e:
            print_exception_trace()
            sys.stderr.write('listener died: %s : %s : %s\n'
                            % (qname, url, repr(e)))
            sys.stderr.flush()


    # --------------------------------------------------------------------------
    #
    def _start_listener(self, qname=None):

        if not qname:
            qname = 'default'

        # only start if needed
        if Getter._callbacks[self._url]['thread']:
            return

        t = mt.Thread(target=Getter._listener, args=[self._url, qname, self._uid])
        t.daemon = True
        t.start()

        Getter._callbacks[self._url]['thread'] = t


    # --------------------------------------------------------------------------
    #
    def _stop_listener(self, force=False):

        # only stop listener if no callbacks remain registered (unless forced)
        if force or not Getter._callbacks[self._url]['callbacks']:
            if  Getter._callbacks[self._url]['thread']:
                Getter._callbacks[self._url]['term'  ].set()
                Getter._callbacks[self._url]['thread'].join()
                Getter._callbacks[self._url]['term'  ].unset()
                Getter._callbacks[self._url]['thread'] = None


    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, url=None, cb=None,
                                log=None, prof=None, path=None):
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
        self._prof      = prof
        self._uid       = generate_id('%s.get.%%(counter)04d' % self._channel,
                                      ID_CUSTOM)

        if not self._url:
            self._url = Bridge.get_config(channel, path).get('get')

        if not self._url:
            raise ValueError('no contact url specified, no config found')

        if not self._log:
            if LOG_ENABLED: level = 'DEBUG_9'
            else          : level = 'ERROR'
            self._log = Logger(name=self._uid, ns='radical.utils.zmq',
                               level=level, path=path)

        if not self._prof:
            self._prof  = Profiler(name=self._uid, ns='radical.utils', path=path)
            self._prof.disable()

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()

        self._log.info('connect get to %s: %s', self._channel, self._url)

        self._requested = False          # send/recv sync
        self._ctx       = zmq.Context()  # rely on GC for destruction
        self._q         = self._ctx.socket(zmq.REQ)
        self._q.linger  = _LINGER_TIMEOUT
        self._q.hwm     = _HIGH_WATER_MARK
        self._q.connect(self._url)

        if url not in Getter._callbacks:

            Getter._callbacks[url] = {'uid'      : self._uid,
                                      'socket'   : self._q,
                                      'channel'  : self._channel,
                                      'lock'     : mt.Lock(),
                                      'term'     : mt.Event(),
                                      'requested': self._requested,
                                      'thread'   : None,
                                      'callbacks': list()}
        if cb:
            self.subscribe(cb)
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

        if self._url not in Getter._callbacks:

            Getter._callbacks[self._url] = {'uid'      : self._uid,
                                            'socket'   : self._q,
                                            'channel'  : self._channel,
                                            'lock'     : mt.Lock(),
                                            'term'     : mt.Event(),
                                            'requested': self._requested,
                                            'thread'   : None,
                                            'callbacks': list()}

        # we allow only one cb per queue getter process at the moment, until we
        # have more clarity on the RR behavior of concurrent callbacks.
        if Getter._callbacks[self._url]['callbacks']:
            raise RuntimeError('multiple callbacks not supported')

        Getter._callbacks[self._url]['callbacks'].append([cb, lock])

        self._interactive = False
        self._start_listener()


    # --------------------------------------------------------------------------
    #
    def unsubscribe(self, cb):

        if self._url in Getter._callbacks:
            for _cb, _lock in Getter._callbacks[self._url]['callbacks']:
                if cb == _cb:
                    Getter._callbacks[self._url]['callbacks'].remove([_cb, _lock])
                    break

        self._stop_listener()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._stop_listener(force=True)


    # --------------------------------------------------------------------------
    #
    def get(self, qname=None):

        if not self._interactive:
            raise RuntimeError('invalid get(): callbacks are registered')

        if not qname:
            qname = 'default'


        # double-check: minimize lock use which is only needed for a very
        # rare race anyway
        if not self._requested:
            with self._lock:
                if not self._requested:
                    self._log.debug_9('=== => from %s[%s]', self._channel, qname)
                    no_intr(self._q.send, as_bytes(qname))
                    self._requested = True

          # self._prof.prof('requested')

        with self._lock:
            data = list(no_intr(self._q.recv_multipart))
            self._requested = False

        qname = from_msgpack(data[0])
        msgs  = from_msgpack(data[1])

        log_bulk(self._log, '<-2 %s [%s]' % (self._channel, qname), msgs)

        return as_string(msgs)


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, qname=None, timeout=None):  # timeout in ms

        if not self._interactive:
            raise RuntimeError('invalid get(): callbacks are registered')

        # backward compatibility to `get_nowait(timeout=None)`
        if timeout is None and isinstance(qname, int):
            timeout = qname
            qname   = None

        if not qname:
            qname = 'default'

        if not self._requested:
            with self._lock:  # need to protect self._requested
                if not self._requested:
                    self._log.debug_9('=== => from %s[%s]', self._channel, qname)
                    no_intr(self._q.send_multipart, [as_bytes(qname)])
                    self._requested = True

        if no_intr(self._q.poll, flags=zmq.POLLIN, timeout=timeout):
            with self._lock:
                data = list(no_intr(self._q.recv_multipart))
                self._requested = False

            qname = from_msgpack(data[0])
            msgs  = from_msgpack(data[1])
            log_bulk(self._log, '<-3 %s [%s]' % (self._channel, qname), msgs)

            return as_string(msgs)

        else:
            return None


# ------------------------------------------------------------------------------
#
def test_queue(channel, addr_pub, addr_sub):

    c_a  = 200
    c_b  = 400
    data = dict()

    for i in 'ABCD':
        data[i] = dict()
        for j in 'AB':
            data[i][j] = 0

    def cb(uid, msg):
        if msg['idx'] is None:
            return False
        data[uid][msg['src']] += 1

    cb_C = lambda t,m: cb('C', m)
    cb_D = lambda t,m: cb('D', m)

    Getter(channel=channel, url=addr_sub, cb=cb_C)
    Getter(channel=channel, url=addr_sub, cb=cb_D)

    # --------------------------------------------------------------------------
    def work_pub(uid, n, delay):

        pub = Putter(channel=channel, url=addr_pub)
        idx = 0

        while idx < n:
            time.sleep(delay)
            pub.put({'src': uid,
                     'idx': idx})
            idx += 1
            data[uid][uid] += 1

        # send EOF
        pub.put({'src': uid,
                 'idx': None})
    # --------------------------------------------------------------------------

    t_a = mt.Thread(target=work_pub, args=['A', c_a, 0.001])
    t_b = mt.Thread(target=work_pub, args=['B', c_b, 0.001])

    t_a.start()
    t_b.start()

    t_a.join()
    t_b.join()

    time.sleep(0.1)

    import pprint
    pprint.pprint(data)

    assert data['A']['A'] == c_a
    assert data['B']['B'] == c_b

    assert data['C']['A'] + data['C']['B'] + \
           data['D']['A'] + data['D']['B'] == 2 * (c_a + c_b)

    return data


# ------------------------------------------------------------------------------

