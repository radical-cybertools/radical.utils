
import sys
import zmq
import msgpack

import threading as mt

from ..ids     import generate_id
from ..misc    import as_string
from ..logger  import Logger
from ..profile import Profiler

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
    def __init__(self):

        # this service offers only synchronous communication: a request will be
        # worked upon and answered before the next request is received.

        self._uid  = generate_id('server', ns='radical.utils')
        self._url  = 'tcp://*:*'
        self._cbs  = dict()

        self._log  = Logger(self._uid, level='debug', targets='.')
        self._prof = Profiler(self._uid, path='.')

        self._addr = None
        self._proc = None
        self._up   = mt.Event()
        self._term = mt.Event()

        self.register_request('echo', self._request_echo)
        self.register_request('fail', self._request_fail)


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

        if self._proc:
            raise RuntimeError('`start()` can be called only once')

        self._proc = mt.Thread(target=self._work)
        self._proc.daemon = True
        self._proc.start()

        self._up.wait()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._log.info('stop bridge %s', self._uid)

        self._term.set()
        if self._proc:
            self._proc.join()

        self._log.info('stoped bridge %s', self._uid)


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

        rep = {'cmd': 'echo_ok',
               'res': arg}

        self._log.debug('request echo: %s', arg )
        return rep


    # --------------------------------------------------------------------------
    #
    def _error(self, err=None, exc=None):

        if not err:
            err = 'invalid request'

        rep = {'cmd': 'error',
               'err': err,
               'exc': exc,
               'res': None}

        self._log.error(err)
        return rep


    # --------------------------------------------------------------------------
    #
    def _work(self):

        self._ctx  = zmq.Context()
        self._sock = self._ctx.socket(zmq.REP)

        self._sock.linger = _LINGER_TIMEOUT
        self._sock.hwm    = _HIGH_WATER_MARK

        self._sock.bind(self._url)

        self._addr = as_string(self._sock.getsockopt(zmq.LAST_ENDPOINT))
        self._up.set()

        while not self._term.is_set():

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
                        rep = self._cbs[cmd](arg)
                    except Exception as e:
                        rep = self._error(err='command failed: %s' % str(e),
                                          exc=e.__dict__)

            self._log.debug('rep: %s', rep)
            no_intr(self._sock.send, msgpack.packb(rep))


# ------------------------------------------------------------------------------

