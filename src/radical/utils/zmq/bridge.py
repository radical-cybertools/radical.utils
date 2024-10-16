
import os

import threading as mt

from typing import Optional

from ..logger  import Logger
from ..profile import Profiler
from ..config  import Config
from ..json_io import read_json, write_json

from .utils import LOG_ENABLED

QUEUE   = 'QUEUE'
PUBSUB  = 'PUBSUB'
UNKNOWN = 'UNKNOWN'


# ------------------------------------------------------------------------------
#
class Bridge(object):
    '''

    A bridge can be configured to have a finite lifetime: when no messages are
    received in `timeout` seconds, the bridge process will terminate.
    '''

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_config(name, pwd=None):

        if not pwd:
            pwd = '.'

        fname = '%s/%s.cfg' % (pwd, name)

        cfg = dict()
        if os.path.isfile(fname):
            cfg = read_json(fname)

        return Config(from_dict=cfg)


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log=None):

        self._cfg     = cfg
        self._log     = log
        self._channel = self._cfg.channel
        self._uid     = self._cfg.uid
        self._pwd     = self._cfg.path

        if not self._pwd:
            self._pwd = os.getcwd()

        if not self._log:
            if LOG_ENABLED: level = self._cfg.log_lvl
            else          : level = 'ERROR'
            self._log = Logger(name=self._uid, ns='radical.utils.zmq',
                               level=level, path=self._pwd)

        self._prof = Profiler(name=self._uid, path=self._pwd)

        if 'hb' in self._uid or 'heartbeat' in self._uid:
            self._prof.disable()
        else:
            self._prof.disable()

        self._prof.prof('init', uid=self._uid, msg=self._pwd)
        self._log.debug('bridge %s init', self._uid)

        self._bridge_initialize()


    # --------------------------------------------------------------------------
    #
    def write_config(self, fname=None):

        if not fname:
            fname = '%s/%s.cfg' % (self._pwd, self._cfg.uid)

        write_json(fname, {'uid'        : self._cfg.uid,
                           self.type_in : str(self.addr_in),
                           self.type_out: str(self.addr_out)})


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

    # protocol independent addr query
    @property
    def type_in(self):
        raise NotImplementedError()

    @property
    def type_out(self):
        raise NotImplementedError()

    @property
    def addr_in(self):
        raise NotImplementedError()

    @property
    def addr_out(self):
        raise NotImplementedError()

    def _bridge_initialize(self):
        raise NotImplementedError()

    def _bridge_work(self):
        raise NotImplementedError()


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
    def wait(self, timeout=None):
        '''
        wait for the bridge to terminate.  If `timeout` is set, the call will
        return after that many seconds, with a return value indicating whether
        the bridge is still alive.
        '''

        self._bridge_thread.join(timeout=timeout)

        if timeout is not None:
            return not self._bridge_thread.is_alive()


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(channel : str,
               kind    : Optional[str]  = None,
               cfg     : Optional[dict] = None):

        # FIXME: add other config parameters: batch size, log level, etc.

        # NOTE: I'd rather have this as class data than as stack data, but
        #       python stumbles over circular imports at that point :/
        #       Another option though is to discover and dynamically load
        #       components.

        from .pubsub import PubSub
        from .queue  import Queue

        _btypemap = {PUBSUB: PubSub,
                     QUEUE : Queue}

        if not kind:
            if   'queue'  in channel.lower(): kind = QUEUE
            elif 'pubsub' in channel.lower(): kind = PUBSUB
            else                            : kind = UNKNOWN

        if kind not in _btypemap:
            raise ValueError('unknown bridge type (%s)' % kind)

        btype  = _btypemap[kind]
        bridge = btype(channel, cfg=cfg)

        return bridge


    # --------------------------------------------------------------------------
    #
    def stop(self):

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

