
import zmq

import threading as mt

from typing      import Optional, Union, Iterator, Any, Dict

from ..ids       import generate_id
from ..url       import Url
from ..misc      import as_string
from ..host      import get_hostip
from ..logger    import Logger
from ..profile   import Profiler
from ..debug     import get_exception_trace
from ..serialize import to_msgpack, from_msgpack

from .utils      import no_intr, zmq_bind


# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT    =         250  # ms to linger after close
_HIGH_WATER_MARK   = 1024 * 1024  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE =        1024  # number of messages to put in a bulk


# ------------------------------------------------------------------------------
#
class Server(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, port: Optional[Union[int, str]] = None,
                       uid : Optional[str]             = None,
                       path: Optional[str]             = None) -> None:

        # this server offers only synchronous communication: a request will be
        # worked upon and answered before the next request is received.

        self._cbs  = dict()
        self._path = path

        if not self._path:
            self._path = './'

        if uid: self._uid = uid
        else  : self._uid = generate_id('server', ns='radical.utils')

        self._log    = Logger(self._uid,   path=self._path)
        self._prof   = Profiler(self._uid, path=self._path)

        self._addr   = None
        self._thread = None
        self._up     = mt.Event()
        self._term   = mt.Event()

        self.register_request('echo', self._request_echo)
        self.register_request('fail', self._request_fail)


        # FIXME: interpret hostname part as specification for the interface to
        #        be used.
        # `ports` can specify as port ranges to use - check if that is the case
        if port is None           : pmin = pmax = None
        elif isinstance(port, str):
            if '-' in port        : pmin,  pmax = port.split('-', 1)
            else                  : pmin = pmax = port
        elif isinstance(port, int): pmin = pmax = port
        else:
            raise ValueError('invalid port specification: %s' % str(port))

        self._pmin = int(pmin) if pmin else None
        self._pmax = int(pmax) if pmax else None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:

        return self._uid


    @property
    def addr(self) -> Optional[str]:

        return self._addr


    # --------------------------------------------------------------------------
    #
    def start(self) -> None:

        self._log.info('start server %s', self._uid)

        if self._thread:
            raise RuntimeError('`start()` can be called only once')

        self._thread = mt.Thread(target=self._work)
        self._thread.daemon = True
        self._thread.start()

        self._up.wait()


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        self._log.info('stop server %s', self._uid)
        self._term.set()


    # --------------------------------------------------------------------------
    #
    def wait(self) -> None:

        self._log.info('wait server %s', self._uid)

        if self._thread:
            self._thread.join()

        self._log.info('wait server %s', self._uid)


    # --------------------------------------------------------------------------
    #
    def register_request(self, req, cb) -> None:

        self._log.info('add handler: %s: %s', req, cb)
        self._cbs[req] = cb


    # --------------------------------------------------------------------------
    #
    def _request_fail(self, arg) -> None:

        raise RuntimeError('task failed successfully')


    # --------------------------------------------------------------------------
    #
    def _request_echo(self, arg: Any) -> Any:

        return arg


    # --------------------------------------------------------------------------
    #
    def _success(self, res: Optional[str] = None) -> Dict[str, Optional[str]]:

        return {'err': None,
                'exc': None,
                'res': res}


    # --------------------------------------------------------------------------
    #
    def _error(self, err: Optional[str] = None,
                     exc: Optional[str] = None) -> Dict[str, Optional[str]]:

        if not err:
            err = 'invalid request'

        return {'err': err,
                'exc': exc,
                'res': None}


    # --------------------------------------------------------------------------
    #
    def _work(self) -> None:

        self._ctx  = zmq.Context()
        self._sock = self._ctx.socket(zmq.REP)

        self._sock.linger = _LINGER_TIMEOUT
        self._sock.hwm    = _HIGH_WATER_MARK

        addr = zmq_bind(self._sock, port_min=self._pmin,
                                    port_max=self._pmax)
        assert addr
        self._addr = str(addr)

        self._up.set()

        self._poll = zmq.Poller()
        self._poll.register(self._sock, zmq.POLLIN)

        while not self._term.is_set():

            event = dict(no_intr(self._poll.poll, timeout=100))

            if self._sock not in event:
                continue

            # default response
            rep = None
            req = None

            try:
                data = no_intr(self._sock.recv)
                req  = as_string(from_msgpack(data))
                self._log.debug('req: %s', str(req)[:128])

                if not isinstance(req, dict):
                    rep = self._error(err='invalid message type')

                else:
                    cmd    = req['cmd']
                    args   = req['args']
                    kwargs = req['kwargs']

                    if not cmd:
                        rep = self._error(err='no command in request')

                    elif cmd not in self._cbs:
                        rep = self._error(err='command [%s] unknown' % cmd)

                    else:
                        rep = self._success(self._cbs[cmd](*args, **kwargs))

            except Exception as e:
                self._log.exception('command failed: %s', req)
                rep = self._error(err='command failed: %s' % str(e),
                                  exc='\n'.join(get_exception_trace()))

            finally:
                if not rep:
                    rep = self._error('server error')
                no_intr(self._sock.send, to_msgpack(rep))
                self._log.debug('rep: %s', str(rep)[:128])

        self._sock.close()
        self._log.debug('term')


# ------------------------------------------------------------------------------

