
import zmq
import copy
import time
import errno
import msgpack

import threading as mt

from ..logger    import Logger
from ..profile   import Profiler


_MAX_RETRY = 3  # max number of ZMQ snd/rcv retries on interrupts


# --------------------------------------------------------------------------
#
# zmq will (rightly) barf at interrupted system calls.  We are able to rerun
# those calls.
#
# FIXME: how does that behave wrt. timeouts?  We probably should include
#        an explicit timeout parameter.
#
# kudos: https://gist.github.com/minrk/5258909
#
def no_intr(f, *args, **kwargs):

    cnt = 0
    while True:
        try:
            return f(*args, **kwargs)

        except zmq.ContextTerminated:
            return None    # connect closed or otherwise became unusable

        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                if cnt > _MAX_RETRY:
                    raise  # interrupted too often - forward exception
                continue   # interrupted, try again
            raise          # some other error condition, raise it
        finally:
            cnt += 1


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
                               level='INFO', path=self._cfg.session_pwd)
        self._log.debug('=== ### %s init 1', self._uid)
        self._prof    = Profiler(name=self._uid, path=self._cfg.session_pwd)
        self._log.debug('=== ### %s init 2', self._uid)

        self._log.debug('bridge %s init', self._uid)

        self._bridge_initialize()
        self._log.debug('=== ### %s init 3', self._uid)


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


    # --------------------------------------------------------------------------
    #
    @property
    def alive(self):
        return self._bridge_thread.is_alive()


# ------------------------------------------------------------------------------

