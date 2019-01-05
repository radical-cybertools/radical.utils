
import copy

from ..logger import Logger


# ------------------------------------------------------------------------------
#
class Bridge(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self._cfg     = copy.deepcopy(cfg)

        self._channel = self._cfg['name']
        self._uid     = self._cfg['uid']   # FIXME: generate?
        self._pwd     = self._cfg['pwd']
        self._log     = Logger(name=self._uid, ns='radical.utils')


    # --------------------------------------------------------------------------
    #
    def start(self):
        pass


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(cfg):

        # ----------------------------------------------------------------------
        # NOTE:  I'd rather have this as class data than as stack data, but
        #        python stumbles over circular imports at that point :/
        #        Another option though is to discover and dynamically load
        #        components.
        from .pubsub import PubSub
        from .queue  import Queue

        _btypemap = {'pubsub' : PubSub, 
                     'queue'  : Queue}
        # ----------------------------------------------------------------------

        kind = cfg['kind']
        uid  = cfg['uid']

        ldir = cfg.get('logdir')
        llvl = cfg.get('log_level')

        log  = Logger(name=uid, ns='radical.utils',
                      path=ldir, targets='%s.log' % uid, level=llvl)

        log.debug('start bridge %s [%s]', uid, kind)

        if kind not in _btypemap:
            raise ValueError('unknown bridge type (%s)' % kind)

        btype  = _btypemap[kind]
        bridge = btype(cfg)

        return bridge


# ------------------------------------------------------------------------------

