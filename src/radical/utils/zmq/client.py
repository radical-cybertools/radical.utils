
import zmq
import msgpack

from typing import Optional, List, Dict, Tuple, Any

import threading as mt

from ..json_io import read_json
from ..misc    import as_list

from .utils    import no_intr, sock_connect


# ------------------------------------------------------------------------------
#
_LINGER_TIMEOUT    = 1000  # ms to linger after close
_HIGH_WATER_MARK   = 1024  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE =    1  # number of messages to put in a bulk


# ------------------------------------------------------------------------------
#
class Request(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cmd:      str,
                       *args:    Any,
                       **kwargs: Any) -> None:

        self._cmd    = cmd
        self._args   = args
        self._kwargs = kwargs


    def packb(self) -> bytes:

        msg_req = {'cmd'   : self._cmd,
                   'args'  : self._args,
                   'kwargs': self._kwargs}
        return msgpack.packb(msg_req)


    @property
    def cmd(self) -> str:
        return self._cmd


    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args


    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs


# ------------------------------------------------------------------------------
#
class Response(object):

    # --------------------------------------------------------------------------
    #
    # FIXME: inherit future
    def __init__(self,
                 res: Optional[Any]       = None,
                 err: Optional[str]       = None,
                 exc: Optional[List[str]] = None) -> None:

        self._res = res
        self._err = err
        self._exc = exc


    # --------------------------------------------------------------------------
    #
    def __repr__(self) -> str:

        ret = ''
        if self._res: ret += 'res: %s  ' % str(self._res)
        if self._err: ret += 'err: %s  ' % self._err
        if self._exc: ret += 'exc: %s  ' % self._exc[-1]

        return ret.strip()


    # --------------------------------------------------------------------------
    #
    def __str__(self) -> str:

        if self._res: ret = 'res: %s  ' % str(self._res)
        else        : ret = 'err: %s  ' % self._err

        return ret.strip()


    # --------------------------------------------------------------------------
    #
    @classmethod
    def from_msg(cls, msg: bytes) -> 'Response':

        return cls.from_dict(msgpack.unpackb(msg))


    # --------------------------------------------------------------------------
    # type hinting for classmethods are not well supported, so we don't use them
    @classmethod
    def from_dict(cls, msg: Dict[str, Any]) -> 'Response':

        return Response(res=msg.get('res'),
                        err=msg.get('err'),
                        exc=msg.get('exc'))


    # --------------------------------------------------------------------------
    #
    @property
    def res(self) -> Optional[Any]:
        return self._res


    # --------------------------------------------------------------------------
    #
    @property
    def err(self) -> Optional[str]:
        return self._err


    # --------------------------------------------------------------------------
    #
    @property
    def exc(self) -> List[str]:
        return as_list(self._exc)                                 # type: ignore


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, server: str = None,
                       url:    str = None) -> None:

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


    # --------------------------------------------------------------------------
    #
    @property
    def url(self) -> str:
        return self._url


    # --------------------------------------------------------------------------
    #
    def request(self, cmd: str, *args: Any, **kwargs: Any) -> Any:

        req = Request(cmd, *args, **kwargs)

        no_intr(self._sock.send, req.packb())

        res = Response.from_msg(no_intr(self._sock.recv))

        if res.err:
            err_msg = 'ERROR: %s' % res.err
            if res.exc:
                err_msg += '\n%s' % '\n'.join(res.exc)
            raise RuntimeError(err_msg)

        return res.res


    # --------------------------------------------------------------------------
    #
    def close(self) -> None:

        self._sock.close()


# ------------------------------------------------------------------------------

