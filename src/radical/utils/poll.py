
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"

# `select.poll()` is a nice thing - alas, it is not implemented in MacOS'
# default python deployment.  Since RU process management relies on socket
# polling, we reimplement it here based on `select.select()`.

import os
import time
import select
import socket
import threading as mt

from .logger  import get_logger
from .debug   import print_exception_trace, print_stacktrace


def poll():
    return (Poller())

_use_pypoll = os.environ.get('RU_USE_PYPOLL', False)


# ------------------------------------------------------------------------------
if _use_pypoll:

  # print "using Python's poll implementation"
    import select

    POLLIN   = select.POLLIN
    POLLPRI  = select.POLLPRI
    POLLOUT  = select.POLLOUT
    POLLERR  = select.POLLERR
    POLLHUP  = select.POLLHUP
    POLLNVAL = select.POLLNVAL

    POLLALL  = POLLIN | POLLOUT | POLLERR | POLLPRI | POLLHUP | POLLNVAL

    # --------------------------------------------------------------------------
    #
    class Poller(object):
        '''
        This is a this wrapper around select.Poller to have it API compatible
        with our own, select based implementation.
        '''

        def __init__(self):
            self._poll = select.poll()

        def register(self, fd, eventmask=None):
            return self._poll.register(fd, eventmask)

        def modify(self, fd, eventmask=None):
            return self._poll.modify(fd, eventmask)

        def unregister(self, fd):
            return self._poll.unregister(fd)

        def poll(self, timeout=None):
            time.sleep(0.2)
            ret = self._poll.poll(timeout)
          # _o.write('py %-50s called %s\n%s\n' % (_p, timeout,  _r(ret)))
            return ret


# ------------------------------------------------------------------------------
else:
  # print 're-implementing poll over select'

    POLLIN   = 0b000001
    POLLPRI  = 0b000010
    POLLOUT  = 0b000100
    POLLERR  = 0b001000
    POLLHUP  = 0b010000
    POLLNVAL = 0b100000

    POLLALL  = POLLIN | POLLOUT | POLLERR | POLLPRI | POLLHUP | POLLNVAL

    _POLLTYPES = [POLLIN, POLLOUT, POLLERR, POLLHUP]

    # --------------------------------------------------------------------------
    #
    class Poller(object):
        '''
        This object will accept a set of things we can call `select.select()`
        on, and will basically implement the same interface as `Poller` objects
        as returned by `select.poll()`.  

        NOTE: `poll()` returns the original object handle instead of the polled
              file descriptor.
        NOTE: Support for `POLLPRI` and `POLLNVAL` selection is not implemented,
              and will never be returned on `poll()`.
        '''

        # ----------------------------------------------------------------------
        #
        def __init__(self):

            self._lock       = mt.RLock()
            self._registered = {POLLIN  : list(),
                                POLLOUT : list(),
                                POLLERR : list(),
                                POLLHUP : list()}


        # ----------------------------------------------------------------------
        #
        def register(self, fd, eventmask=None):

            if not eventmask:
                eventmask = POLLIN | POLLOUT | POLLERR


            with self._lock:

                self.unregister(fd, _assert_existence=False)

                for e in _POLLTYPES:
                    if eventmask & e:
                        self._registered[e].append(fd)



        # ----------------------------------------------------------------------
        #
        def _exists(self, fd):

            with self._lock:
                for e in _POLLTYPES:
                    if fd in self._registered[e]:
                        return True

            return False


        # ----------------------------------------------------------------------
        #
        def modify(self, fd, eventmask=None):

            if not eventmask:
                eventmask = POLLIN | POLLOUT | POLLERR | POLLHUP

            with self._lock:
                assert(self._exists(fd))
                self._register(fd, eventmask)


        # ----------------------------------------------------------------------
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


        # ----------------------------------------------------------------------
        #
        def poll(self, timeout=None):

            ret = list()

            with self._lock:
                 rlist = self._registered[POLLIN]
                 wlist = self._registered[POLLOUT]
                 xlist = self._registered[POLLERR]
                 hlist = self._registered[POLLHUP]

            # only select if we have any FDs to watch
            if not rlist + wlist + xlist + hlist:
                return ([], [], [])

            rret, wret, xret = select.select(rlist+hlist, wlist, xlist, timeout)

            # do not return hlist-only FDs
            ret += [[fd, POLLOUT] for fd in set(wret)]
            ret += [[fd, POLLERR] for fd in set(xret)]

            # A socket being readable may also mean the socket has been 
            # closed.  We do a zero-length read to check for POLLHUP if
            # needed.
            #
            # NOTE: I am so happy we can do type inspection in Python, so 
            #       that we can have different checks for, say, files and
            #       sockets.  Only this shit doesn't work on sockets! Argh!
            #       Oh well, we derive the type from `recv` and `read`
            #       methods being available.
            #       Its a shame to do this on every `poll()` call, as this is
            #       likely in a performance critical path...
            for fd in set(rret):

                hup = False

                # file object
                if hasattr(fd, 'closed'):
                    if fd.closed:
                        hup = True

                # anything with a `fileno()` and `read()`/`write()`
                elif hasattr(fd, 'read'):
                    try:
                        fd.read(0)
                        fd.write('')
                    except:
                        hup = True

                # socket
                elif hasattr(fd, 'recv'):
                    try:
                        fd.recv(0)
                        fd.send('')
                    except Exception as e:
                      # print 'check : error %s' % e
                        hup = True

                # we can't handle errors on other types
                else:
                    raise TypeError('cannot check %s [%s]' % (fd, type(fd)))

                if hup: ret.append([fd, POLLIN | POLLHUP])
                else  : ret.append([fd, POLLIN])

          # _o.write('ru %-50s called %s\n%s\n' % (_p, timeout,  _r(ret)))

            return ret


# ------------------------------------------------------------------------------
#
# import pprint
# from .threads import get_thread_name
# 
# _o = open('/tmp/o', 'a+')
# _p = '%s -  %s ' % (os.getpid(), get_thread_name())
# 
# def _r(ret):
#     out = ''
#     for fd, event in ret:
#         vals = []
#         if event & POLLIN   : vals.append('IN')
#         if event & POLLPRI  : vals.append('PRI')
#         if event & POLLOUT  : vals.append('OUT')
#         if event & POLLERR  : vals.append('ERR')
#         if event & POLLHUP  : vals.append('HUP')
#         if event & POLLNVAL : vals.append('NVAL')
#         out += '%s : %s\n' % (type(fd), str(vals))
#     return out
# ------------------------------------------------------------------------------

