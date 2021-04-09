
import zmq
import msgpack

import threading as mt

from ..ids     import generate_id
from ..url     import Url
from ..misc    import as_string, get_hostip
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
    def __init__(self, url=None):

        # this server offers only synchronous communication: a request will be
        # worked upon and answered before the next request is received.

        self._url    = url
        self._uid    = generate_id('server', ns='radical.utils')
        self._cbs    = dict()

        self._log    = Logger(self._uid, level='debug', targets='.')
        self._prof   = Profiler(self._uid, path='.')

        self._addr   = None
        self._thread = None
        self._up     = mt.Event()
        self._term   = mt.Event()

        self.register_request('echo', self._request_echo)
        self.register_request('fail', self._request_fail)

        if not self._url:
            self._url  = 'tcp://*:10000-11000'

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

        self._port_this = None


    # --------------------------------------------------------------------------
    #
    def _iterate_ports(self):

        if self._port_this == '*':
            # leave scanning to zmq
            yield self._port_this
            return


        if self._port_this is None:
            # initizliaze range iterator
            self._port_this = self._port_start

        if self._port_stop is None:
            while True:
                yield self._port_this
                self._port_this += 1

        else:
            while self._port_this <= self._port_start:
                yield self._port_this
                self._port_this += 1


    # --------------------------------------------------------------------------
    #
    def _iterate_urls(self):

        for port in self._iterate_ports():
            yield '%s://%s:%s' % (self._proto, self._iface, port)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    @property
    def addr(self):
        return self._addr


    # --------------------------------------------------------------------------
    #
    def start(self):

        self._log.info('start bridge %s', self._uid)

        if self._thread:
            raise RuntimeError('`start()` can be called only once')

        self._thread = mt.Thread(target=self._work)
        self._thread.start()

        self._up.wait()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._log.info('stop bridge %s', self._uid)
        self._term.set()


    # --------------------------------------------------------------------------
    #
    def wait(self):

        self._log.info('wait bridge %s', self._uid)

        if self._thread:
            self._thread.join()
        self._log.info('wait bridge %s', self._uid)


    # --------------------------------------------------------------------------
    #
    def register_request(self, req, cb):

        self._log.info('add handler: %s: %s', req, cb)
        self._cbs[req] = cb


    # --------------------------------------------------------------------------
    #
    def _request_fail(self, arg):

        raise RuntimeError('task failed successfully')


    # --------------------------------------------------------------------------
    #
    def _request_echo(self, arg):

        return {'res': arg}


    # --------------------------------------------------------------------------
    #
    def _success(self, res=None):

        return {'err': None,
                'exc': None,
                'res': res}


    # --------------------------------------------------------------------------
    #
    def _error(self, err=None, exc=None):

        if not err:
            err = 'invalid request'

        return {'err': err,
                'exc': exc,
                'res': None}


    # --------------------------------------------------------------------------
    #
    def _work(self):

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


        self._addr      = Url(as_string(self._sock.getsockopt(zmq.LAST_ENDPOINT)))
        self._addr.host = get_hostip()
        self._addr      = str(self._addr)

        self._up.set()

        self._poll = zmq.Poller()
        self._poll.register(self._sock, zmq.POLLIN)

        while not self._term.is_set():

            event = dict(no_intr(self._poll.poll, timeout=100))

            if self._sock not in event:
                continue

            data = no_intr(self._sock.recv)
            req  = msgpack.unpackb(data)
            self._log.debug('req: %s', req)

            if not isinstance(req, dict):
                rep = self._error(err='invalid message type')

            else:
                cmd = req.get('cmd')
                arg = req.get('arg')

                if not cmd:
                    rep = self._error(err='no command in request')

                elif cmd not in self._cbs:
                    rep = self._error(err='command unknown')

                elif not arg:
                    rep = self._error(err='missing arguments')

                else:
                    try:
                        rep = self._success(self._cbs[cmd](arg))
                    except Exception as e:
                        rep = self._error(err='command failed: %s' % str(e),
                                          exc=get_exception_trace())

            no_intr(self._sock.send, msgpack.packb(rep))
            self._log.debug('rep: %s', rep)

        self._log.debug('term')


# ------------------------------------------------------------------------------

