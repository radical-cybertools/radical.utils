
import time

import threading as mt

from rc.process  import Process
from functools   import partial
from typing      import List

from ..url        import Url
from ..ids        import generate_id
from ..logger     import Logger

from .flux_module import FluxModule


# ------------------------------------------------------------------------------
#
class FluxService(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid     : str    = None,
                       log     : Logger = None,
                       launcher: str    = None
                       ) -> None:

        self._uid      = uid      or generate_id('ru.flux')
        self._log      = log      or Logger('radical.utils.flux', level='DEBUG')
        self._launcher = launcher or ''

        self._fm    = FluxModule()
        self._uri   = None
        self._r_uri = None
        self._host  = None
        self._proc  = None
        self._ready = mt.Event()

        self._fm.verify()


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:
        return self._uid

    @property
    def uri(self) -> str:
        return self._uri


    @property
    def r_uri(self) -> str:
        return self._r_uri


    # --------------------------------------------------------------------------
    #
    def _proc_line_cb(self, prefix: str,
                            proc  : Process,
                            lines : List[str]
                     ) -> None:

        try:
            for line in lines:
                self._log.info('%s: flux io : %s', self._uid, line)

                if line.startswith('FLUX_URI='):
                    parts = line.strip().split(' ', 1)
                    self._log.info('%s: found flux info: %s', self._uid, parts)

                    self._uri   = parts[0].split('=', 1)[1]
                    self._host  = parts[1].split('=', 1)[1]

                    url         = Url(self._uri)
                    url.host    = self._host
                    url.schema  = 'ssh'
                    self._r_uri = str(url)

                    self._log.info('%s: flux uri: %s', self._uid, self._uri)
                    self._log.info('%s:    r uri: %s', self._uid, self._r_uri)
                    self._ready.set()
        except:
            self._log.exception('line processing failed')


    # --------------------------------------------------------------------------
    #
    def _proc_state_cb(self, proc: Process, state: str) -> None:

        self._log.info('flux instance state update: %s', state)
        if state in Process.FINAL:

            self._log.info('flux instance stopped: %s', state)
            self.stop()


    # --------------------------------------------------------------------------
    #
    def start(self, timeout: float = None) -> None:

        fcmd  = 'echo FLUX_URI=\\$FLUX_URI FLUX_HOST=\\$(hostname) '
        fcmd += ' && flux resource list '
        fcmd += ' && sleep inf '
        cmd   = ' %s start bash -c "%s"' % (self._fm.exe, fcmd)

        if self._launcher:
            cmd = '%s %s' % (self._launcher, cmd)

        self._log.info('%s: start flux instance: %s', self._uid, cmd)

        p = Process(cmd)
        p.register_cb(p.CB_OUT_LINE, partial(self._proc_line_cb, 'out'))
        p.register_cb(p.CB_ERR_LINE, partial(self._proc_line_cb, 'err'))
        p.register_cb(p.CB_STATE, self._proc_state_cb)
        p.polldelay = 0.1
        p.start()

        self._proc  = p
        self._ptime = time.time()

        return self.ready(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def ready(self, timeout: float = None) -> None:

        if timeout is not None:
            if timeout < 0: self._ready.wait()
            else          : self._ready.wait(timeout)

        return self._ready.is_set()


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        if not self._proc:
            self._uri  = None
            return

        self._proc.cancel()
        self._proc.wait()

        self._uri  = None
        self._proc = None

        self._log.info('%s: found flux uri: %s', self._uid, self.uri)


# ------------------------------------------------------------------------------

