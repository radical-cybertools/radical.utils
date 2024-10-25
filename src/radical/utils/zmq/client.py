
import zmq

from typing import Any

import threading as mt

from ..json_io   import read_json
from ..misc      import as_string
from ..serialize import to_msgpack, from_msgpack
from ..logger    import Logger
from .utils      import no_intr, sock_connect


# ------------------------------------------------------------------------------
#
_LINGER_TIMEOUT    = 1000  # ms to linger after close
_HIGH_WATER_MARK   = 1024  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE =    1  # number of messages to put in a bulk


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, server: str    = None,
                       url:    str    = None,
                       log:    Logger = None) -> None:

        if server:
            self._url = read_json('%s.cfg' % server)['addr']

        elif url:
            self._url = url

        else:
            raise ValueError('need server name/cfg or Url')

        self._log  = log
        self._cb   = None
        self._ctx  = zmq.Context()
        self._sock = self._ctx.socket(zmq.REQ)

        self._sock.linger = _LINGER_TIMEOUT
        self._sock.hwm    = _HIGH_WATER_MARK

        sock_connect(self._sock, self._url)

        self._term   = mt.Event()
        self._active = False


    # --------------------------------------------------------------------------
    #
    @property
    def url(self) -> str:
        return self._url


    # --------------------------------------------------------------------------
    #
    def request(self, cmd: str, *args: Any, **kwargs: Any) -> Any:

        msg = {'cmd'   : cmd,
               'args'  : args,
               'kwargs': kwargs}

        if self._log:
            self._log.debug('request: %s', msg)

        req = to_msgpack(msg)

        no_intr(self._sock.send, req)

        msg = no_intr(self._sock.recv)
        res = as_string(from_msgpack(msg))

        # FIXME: assert proper res structure

        if res.get('err'):
            err_msg = 'ERROR: %s' % res['err']
            if res['exc']:
                err_msg += '\n%s' % ''.join(res['exc'])
            raise RuntimeError(err_msg)

        return res['res']


    # --------------------------------------------------------------------------
    #
    def close(self) -> None:

        self._sock.close()


# ------------------------------------------------------------------------------

