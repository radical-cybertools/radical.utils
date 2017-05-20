
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"

# `select.poll()` is a nice thing - alas, it is not implemented in MacOS'
# default python deployment.  Since RU process management relies on socket
# polling, we reimplement it here based on `select.select()`.

import time
import select
import socket
import threading as mt

from .logger  import get_logger
from .debug   import print_exception_trace, print_stacktrace

POLLIN	 = 0b0001
POLLPRI	 = POLLIN
POLLOUT	 = 0b0010
POLLERR	 = 0b0100
POLLHUP	 = POLLERR
POLLNVAL = POLLERR
POLLALL  = POLLIN | POLLOUT | POLLERR

_POLLTYPES = [POLLIN, POLLOUT, POLLERR]

# ------------------------------------------------------------------------------
#
class Poller(object):
    '''

    This object will accept a set of things we can call `select.select()` on,
    and will basically implement the same interface as `Poller` objects as
    returned by `select.poll()`.  We don't attempt to cleanly distinguish
    between `POLLHUP`, `POLLERR`, `POLLNVAL`, as those are platform specific.
    We always use `POLLERR` to signal any of those conditions.  We will also not
    separate `POLLIN` and `POLLPRI`, but fold both into `POLLIN`.
    
    '''
   
    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._lock       = mt.RLock()
        self._registered = {POLLIN  : list(),
                            POLLOUT : list(),
                            POLLERR : list()}


    # --------------------------------------------------------------------------
    #
    def register(self, fd, eventmask=None):

        if not eventmask:
            eventmask = POLLIN | POLLOUT | POLLERR


        with self._lock:

            self.unregister(fd, _assert_existence=False)

            for e in _POLLTYPES:
                if eventmask & e:
                    self._registered[e].append(fd)


     
    # --------------------------------------------------------------------------
    #
    def _exists(self, fd):

        with self._lock:
            for e in _POLLTYPES:
                if fd in self._registered[e]:
                    return True

        return False


    # --------------------------------------------------------------------------
    #
    def modify(self, fd, eventmask=None):

        if not eventmask:
            eventmask = POLLIN | POLLOUT | POLLERR

        with self._lock:
            assert(self._exists(fd))
            self._register(fd, eventmask)


    # --------------------------------------------------------------------------
    #
    def unregister(self, fd, _assert_existence=True):

        with self._lock:

            exists = self._exists(fd)

            if _assert_existence:
                assert(exists)
            elif not exists:
                return

            for e in _POLLTYPES:
                if fd in self._registered[e]:
                    self._registered.remove[e](fd)


    # --------------------------------------------------------------------------
    #
    def poll(self, timeout=None):

        ret = list()

        with self._lock:
             rlist = self._registered[POLLIN]
             wlist = self._registered[POLLOUT]
             xlist = self._registered[POLLERR]

        if rlist + wlist + xlist:

            rret, wret, xret = select.select(rlist, wlist, xlist, timeout)

            for fd in rret: ret.append([fd, POLLIN ])
            for fd in wret: ret.append([fd, POLLOUT])
            for fd in xret: ret.append([fd, POLLERR])

            # a socket being readable may also mean the socket has been closed.
            # We do a zero-length read to check
            #
            # NOTE: I am so happy we can do type inspection in Python, so that
            #       we can have different checks for, say, files and sockets.
            #       Only this shit doesn't work on sockets! Argh!
            #       Oh well, we derive the type from `recv` and `read` methods
            #       being available. 
            #       Its a shame to do this on every `poll()` call, as this is
            #       likely in a performance critical path...
            for fd in rlist:

                # file object
                if hasattr(fd, 'closed'):
                    if fd.closed:
                        ret.append([fd, POLLERR])

                # anything with a `fileno()` and `read()`/`write()`
                elif hasattr(fd, 'read'):
                    try:
                        fd.read(0)
                        fd.write('')
                    except:
                        ret.append([fd, POLLERR])

                # socket
                elif hasattr(fd, 'recv'):
                    try:
                        fd.recv(0)
                        fd.send('')
                    except Exception as e:
                      # print 'check : error %s' % e
                        ret.append([fd, POLLERR])

                # we can't handle errors on other types
                else:
                    raise TypeError('cannot check %s [%s]' % (fd, type(fd)))

        return ret


# ------------------------------------------------------------------------------

