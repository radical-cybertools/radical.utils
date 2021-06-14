
import zmq
import msgpack

from typing import List, Optional, Dict, Any

import threading as mt

from ..json_io import read_json
from ..misc    import as_list

from .utils    import no_intr, sock_connect


# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT    = 1000  # ms to linger after close
_HIGH_WATER_MARK   = 1024  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE =    1  # number of messages to put in a bulk


# ------------------------------------------------------------------------------
#
class Request(object):

    def __init__(self,
                 cmd: Optional[str]       = None,
                 arg: Optional[List[Any]] = None
                ) -> None:

        self._cmd = cmd
        self._arg = arg


    @classmethod
    def from_dict(cls, req:  Dict[str, Any]) -> 'Request':

        return Request(cmd=req['cmd'], arg=req.get('arg'))


    def packb(self) -> bytearray:

        msg_req = {'cmd': self._cmd, 'arg': self._arg}
        return msgpack.packb(msg_req)


    @property
    def cmd(self) -> Optional[str]:
        return self._cmd

    @property
    def arg(self) -> Optional[List[Any]]:
        return self._arg


# ------------------------------------------------------------------------------
#
class Response(object):

    # FIXME: inherit future
    def __init__(self,
                 res: Optional[str]       = None,
                 err: Optional[str]       = None,
                 exc: Optional[List[str]] = None
                ) -> None:

        self._res = res
        self._err = err
        self._exc = exc


    def __repr__(self) -> str:

        ret = ''
        if self._res: ret += 'res: %s  ' % self._res
        if self._err: ret += 'err: %s  ' % self._err
        if self._exc: ret += 'exc: %s  ' % self._exc[-1]

        return ret.strip()


    def str(self) -> str:

        if self._res: ret = 'res: %s' % self._res
        else        : ret = 'err: %s' % self._err

        return ret.strip()


    @classmethod
    def from_msg(cls, msg: bytearray) -> 'Response':
        return cls.from_dict(msgpack.unpackb(msg))


    @classmethod
    def from_dict(cls, msg: Dict[str, Any]) -> 'Response':

        return Response(res=msg.get('res'),
                        err=msg.get('err'),
                        exc=msg.get('exc'))


    @property
    def res(self) -> str:
        return self._res

    @property
    def err(self) -> str:
        return self._err

    @property
    def exc(self) -> List[str]:
        return as_list(self._exc)


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, server: str = None, url: str = None) -> None:

        if server:
            self._url = read_json('%s.cfg' % server)['addr']

        elif url:
            self._url = url

        else:
            raise ValueError('need server name/cfg or Url')

        self._cb   = None

        self._ctx  = zmq.Context()
        self._sock = self._ctx.socket(zmq.REQ)

        self._sock.linger = _LINGER_TIMEOUT
        self._sock.hwm    = _HIGH_WATER_MARK

        sock_connect(self._sock, self._url)

        self._term   = mt.Event()
        self._active = False


    @property
    def url(self) -> str:
        return self._url


    # --------------------------------------------------------------------------
    #
    def request(self, cmd: str, arg: Optional[List[Any]] = None) -> 'Response':

        req = Request(cmd=cmd, arg=arg)

        no_intr(self._sock.send, req.packb())

        res = Response.from_msg(no_intr(self._sock.recv))

        for l in res.exc:
            print(l)

        if res.err:
            raise RuntimeError('ERROR: %s' % res.err)

        return res


    # --------------------------------------------------------------------------
    #
    def close(self) -> None:

        self._sock.close()


# ------------------------------------------------------------------------------

