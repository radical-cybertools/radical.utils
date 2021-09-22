
import zmq
import msgpack

import threading as mt

from typing import Optional, Union, Iterator, Any, Dict

from ..ids     import generate_id
from ..url     import Url
from ..misc    import as_string
from ..host    import get_hostip
from ..logger  import Logger
from ..profile import Profiler
from ..debug   import get_exception_trace

from .utils    import no_intr


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
    def __init__(self, url: Optional[str] = None,
                       uid: Optional[str] = None) -> None:

        # this server offers only synchronous communication: a request will be
        # worked upon and answered before the next request is received.

        self._url = url
        self._uid = uid
        self._cbs = dict()

        if not self._uid:
            self._uid = generate_id('server', ns='radical.utils')

        self._log    = Logger(self._uid, level='debug', targets='.')
        self._prof   = Profiler(self._uid, path='.')

        self._addr   = None
        self._thread = None
        self._up     = mt.Event()
        self._term   = mt.Event()

        self.register_request('echo', self._request_echo)
        self.register_request('fail', self._request_fail)

        if not self._url:
            self._url = 'tcp://*:10000-11000'

        # URLs can specify port ranges to use - check if that is the case (see
        # default above) and initilize iterator.  The URL is expected to have
        # the form:
        #
        #   <proto>://<iface>:<ports>/
        #
        # where
        #   <proto>: any protocol accepted by zmq,       defaults to `tcp`
        #   <iface>: IP number of interface to bind to   defaults to `*`
        #   <ports>: port range to find port to bind to  defaults to `*`
        #
        # The port range can be formed as:
        #
        #   '*'      : any port
        #   '100+'   : any port equal or larger than 100
        #   '100-'   : any port equal or larger than 100
        #   '100-110': any port equal or larger than 100, up to 110
        tmp = self._url.split(':', 2)
        assert(len(tmp) == 3)
        self._proto = tmp[0]
        self._iface = tmp[1].lstrip('/')
        self._ports = tmp[2].replace('+', '-')

        tmp = self._ports.split('-')

        self._port_this : Union[int, str, None] = None
        self._port_start: Optional[int]
        self._port_stop : Optional[int]

        if len(tmp) == 0:
            self._port_start = 1
            self._port_stop  = None
        elif len(tmp) == 1:
            if tmp[0] == '*':
                self._port_this  = '*'
                self._port_start = None
                self._port_stop  = None
            else:
                self._port_start = int(tmp[0])
                self._port_stop  = int(tmp[0])
        elif len(tmp) == 2:
            if tmp[0]: self._port_start = int(tmp[0])
            else     : self._port_start = 1
            if tmp[1]: self._port_stop  = int(tmp[1])
            else     : self._port_stop  = None
        else:
            raise RuntimeError('cannot parse port spec %s' % self._ports)


    # --------------------------------------------------------------------------
    #
    def _iterate_ports(self) -> Iterator[Union[int, str, None]]:

        if self._port_this == '*':
            # leave scanning to zmq
            yield self._port_this

        if self._port_this is None:
            # initialize range iterator
            self._port_this = self._port_start

        if self._port_stop is None:
            while True:
                yield self._port_this
                self._port_this += 1

        else:
            # make type checker happy
            assert(isinstance(self._port_this,  int))
            assert(isinstance(self._port_start, int))

            while self._port_this <= self._port_stop:
                yield self._port_this
                self._port_this += 1


    # --------------------------------------------------------------------------
    #
    def _iterate_urls(self) -> Iterator[str]:

        for port in self._iterate_ports():
            yield '%s://%s:%s' % (self._proto, self._iface, port)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> Optional[str]:
        return self._uid


    @property
    def addr(self) -> Optional[str]:
        return self._addr


    # --------------------------------------------------------------------------
    #
    def start(self) -> None:

        self._log.info('start bridge %s', self._uid)

        if self._thread:
            raise RuntimeError('`start()` can be called only once')

        self._thread = mt.Thread(target=self._work)
        self._thread.daemon = True
        self._thread.start()

        self._up.wait()


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        self._log.info('stop bridge %s', self._uid)
        self._term.set()


    # --------------------------------------------------------------------------
    #
    def wait(self) -> None:

        self._log.info('wait bridge %s', self._uid)

        if self._thread:
            self._thread.join()

        self._log.info('wait bridge %s', self._uid)


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

        for url in self._iterate_urls():
            try:
                self._log.debug('try url %s', url)
                self._sock.bind(url)
                self._log.debug('success')
                break
            except Exception:
                self._log.exception('pass')

        addr       = Url(as_string(self._sock.getsockopt(zmq.LAST_ENDPOINT)))
        addr.host  = get_hostip()
        self._addr = str(addr)

        self._up.set()

        self._poll = zmq.Poller()
        self._poll.register(self._sock, zmq.POLLIN)

        while not self._term.is_set():

            event = dict(no_intr(self._poll.poll, timeout=100))

            if self._sock not in event:
                continue

            # default response
            rep = self._error('server error')

            try:
                data = no_intr(self._sock.recv)
                req  = msgpack.unpackb(data)
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
                rep = self._error(err='command failed: %s' % str(e),
                                  exc='\n'.join(get_exception_trace()))

            finally:
                no_intr(self._sock.send, msgpack.packb(rep))
                self._log.debug('rep: %s', str(rep)[:128])

        self._sock.close()
        self._log.debug('term')


# ------------------------------------------------------------------------------

