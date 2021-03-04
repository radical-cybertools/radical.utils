
import zmq
import time
import msgpack

import threading as mt

from ..misc    import as_string
from ..json_io import read_json

from .utils    import no_intr, prof_bulk


# --------------------------------------------------------------------------
#
_LINGER_TIMEOUT    =  250  # ms to linger after close
_HIGH_WATER_MARK   = 1024  # number of messages to buffer before dropping
_DEFAULT_BULK_SIZE =    1  # number of messages to put in a bulk


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

        self._sock.connect(self._url)

        self._term   = mt.Event()
        self._active = False


    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def request(self, req, arg):

        msg_req  = {'cmd': req, 'arg': arg}
        data_req = msgpack.packb(msg_req)

        no_intr(self._sock.send, data_req)

        data_rep = no_intr(self._sock.recv)
        msg_rep  = msgpack.unpackb(data_rep)

        if msg_rep.get('exc'):
            exception = msg_rep['exc']
            raise exception

        elif msg_rep.get('err'):
            raise RuntimeError('ERROR: %s' % msg_rep['err'])


        assert(msg_rep.get('res')), msg_rep

        return msg_rep['res']


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._sock.close()


# ------------------------------------------------------------------------------

