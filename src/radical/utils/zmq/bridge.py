
import zmq
import errno
import msgpack

import threading as mt

from ..logger    import Logger
from ..profile   import Profiler


# --------------------------------------------------------------------------
#
# zmq will (rightly) barf at interrupted system calls.  We are able to rerun
# those calls.
#
# This is presumably rare, and repeated interrupts increasingly unlikely.
# More than, say, 3 point to races or I/O thrashing
#
# FIXME: how does that behave wrt. timeouts?  We probably should include
#        an explicit timeout parameter.
#
# kudos: https://gist.github.com/minrk/5258909
#
def no_intr(f, *args, **kwargs):

    _max = 3
    cnt  = 0
    return f(*args, **kwargs)
    while True:
        try:
            return f(*args, **kwargs)

        except zmq.ContextTerminated:
            return None    # connect closed or otherwise became unusable

        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                if cnt > _max:
                    raise  # interrupted too often - forward exception
                cnt += 1
                continue   # interrupted, try again
            raise          # some other error condition, raise it


# ------------------------------------------------------------------------------
#
def log_bulk(log, bulk, token):

    if not bulk:
      # log.debug("%s: None", token)
        return

    if hasattr(bulk, 'read'):
        bulk = msgpack.unpack(bulk)

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
            log.debug("%s: %s", token, str(e)[0:32])


# ------------------------------------------------------------------------------
#
class Bridge(object):
    '''

    A bridge can be configured to have a finite lifetime: when no messages are
    received in `timeout` seconds, the bridge process will terminate.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self._cfg     = cfg
        self._channel = self._cfg.channel
        self._uid     = self._cfg.uid
        self._log     = Logger(name=self._uid, ns='radical.utils',
                               level='DEBUG', path=self._cfg.path)
        self._prof    = Profiler(name=self._uid, path=self._cfg.path)

        self._prof.prof('init3', uid=self._uid, msg=self._cfg.path)
        self._log.debug('bridge %s init', self._uid)

        self._bridge_initialize()


    # --------------------------------------------------------------------------
    #
    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def start(self):

        # the bridge runs in a thread.  It is the bridge's owner process'
        # responsibility to ensure the thread is seeing suffient time to perform
        # as needed.  Given Python's thread performance (or lack thereof), this
        # basically means that the user of this class should create a separate
        # process instance to host the bridge thread.
        self._term          = mt.Event()
        self._bridge_thread = mt.Thread(target=self._bridge_work)
        self._bridge_thread.daemon = True
        self._bridge_thread.start()

        self._log.info('started bridge %s', self._uid)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(cfg):

        # NOTE: I'd rather have this as class data than as stack data, but
        #       python stumbles over circular imports at that point :/
        #       Another option though is to discover and dynamically load
        #       components.
        from .pubsub import PubSub
        from .queue  import Queue

        _btypemap = {'pubsub' : PubSub,
                     'queue'  : Queue}

        kind = cfg['kind']

        if kind not in _btypemap:
            raise ValueError('unknown bridge type (%s)' % kind)

        btype  = _btypemap[kind]
        bridge = btype(cfg)

        return bridge


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):

        self._term.set()
      # self._bridge_thread.join(timeout=timeout)
        self._prof.prof('term', uid=self._uid)

      # if timeout is not None:
      #     return not self._bridge_thread.is_alive()


    # --------------------------------------------------------------------------
    #
    @property
    def alive(self):
        return self._bridge_thread.is_alive()


# ------------------------------------------------------------------------------

