
import zmq
import msgpack

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

    def __init__(self, cmd=None, arg=None):

        self._cmd = cmd
        self._arg = arg


    @classmethod
    def from_dict(cls, req):

        return Request(cmd=req['cmd'], arg=req.get('arg'))


    def packb(self):

        msg_req = {'cmd': self._cmd, 'arg': self._arg}
        return msgpack.packb(msg_req)


    @property
    def cmd(self): return self._cmd

    @property
    def arg(self): return self._arg


# ------------------------------------------------------------------------------
#
class Response(object):

    # FIXME: inherit future
    def __init__(self, res=None, err=None, exc=None):

        self._res = res
        self._err = err
        self._exc = exc

    def __repr__(self):

        ret = ''
        if self._res: ret += 'res: %s  ' % self._res
        if self._err: ret += 'err: %s  ' % self._err
        if self._exc: ret += 'exc: %s  ' % self._exc[-1]

        return ret.strip()


    def str(self):

        if self._res: ret = 'res: %s' % self._res
        else        : ret = 'err: %s' % self._err

        return ret.strip()


    @classmethod
    def from_msg(cls, msg):
        return cls.from_dict(msgpack.unpackb(msg))


    @classmethod
    def from_dict(cls, msg):

        return Response(res=msg.get('res'),
                        err=msg.get('err'),
                        exc=msg.get('exc'))


    @property
    def res(self): return self._res

    @property
    def err(self): return self._err

    @property
    def exc(self): return as_list(self._exc)


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, server=None, url=None):

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
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def request(self, cmd=None, arg=None):

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
    def close(self):

        self._sock.close()


# ------------------------------------------------------------------------------

