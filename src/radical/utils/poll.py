
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"

import os
import time
import select
import threading as mt


# `select.poll()` is a nice thing - alas, it is not implemented in MacOS'
# default python deployment.  Since RU process management relies on socket
# polling, we reimplement it here based on `select.select()`.
#
# The following import directive should enable any select based code to run
# unchanged on MacOS (for the parts we port, at least):
#
#     import radical.utils.poll as select
#
#     poller = select.poll()
#     ...
#
# If `RU_USE_PYPOLL` is set (to an arbitrary, non-empty value) in the
# environment, we fall back to the native Python implementation.
#
_use_pypoll = os.environ.get('RU_USE_PYPOLL', False)


# ------------------------------------------------------------------------------
# define the Poller factory.  No idea why the Poller object is not directly
# exposed in the `select` module... :/
def poll(log=None):
    return (Poller(log))


# ------------------------------------------------------------------------------
if _use_pypoll:

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

        def __init__(self, log=None):

            self._log  = log
            self._poll = select.poll()


        def close(self):
            self._poll = None


        def register(self, fd, eventmask=None):
            assert(self._poll)
            return self._poll.register(fd, eventmask)


        def modify(self, fd, eventmask=None):
            assert(self._poll)
            return self._poll.modify(fd, eventmask)


        def unregister(self, fd):
            assert(self._poll)
            return self._poll.unregister(fd)


        def poll(self, timeout=None):
            assert(self._poll)
            time.sleep(0.2)
            ret = self._poll.poll(timeout)
          # if self._log:
          #     self._log.debug('pypoll: %s', ret)
            return ret


# ------------------------------------------------------------------------------
#
else:
    # these defines are magically compatible with the ones in Python's `select`
    # module...
    POLLIN   = 0b000001
    POLLPRI  = 0b000010
    POLLOUT  = 0b000100
    POLLERR  = 0b001000
    POLLHUP  = 0b010000
    POLLNVAL = 0b100000

    # POLLALL is not defined by `select`.
    POLLALL  = POLLIN | POLLOUT | POLLERR | POLLPRI | POLLHUP | POLLNVAL

    # we only really support the following 4 types - all others are silently
    # ignored, and will never be triggered.
    _POLLTYPES = [POLLIN, POLLOUT, POLLERR, POLLHUP]


    # --------------------------------------------------------------------------
    #
    class Poller(object):
        '''
        This object will accept a set of things we can call `select.select()`
        on, which is basically anything which has a file desciptor exposed via
        a `fileno()` member method, or any integers which directly represent an
        OS level file escriptor.  This class implements most of the interface
        as defined by Python's `select.Poller` class, which is created by
        calling `select.poll()`.  This implementation is thread-safe.

        NOTE: `Poller.poll()` returns the original object handle instead of the
              polled file descriptor.
        NOTE: Support for `POLLPRI` and `POLLNVAL` selection is not implemented,
              and will never be returned on `poll()`.
        '''

        # ----------------------------------------------------------------------
        #
        def __init__(self, log=None):

            self._log        = log
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
        def close(self):

            with self._lock:

                for e in _POLLTYPES:
                    for fd in self._registered[e]:
                        fd.close()
                    self._registered[e] = list()


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
                self.register(fd, eventmask)


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
                        self._registered[e].remove(fd)


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

                # Des Pudel's Kern!
                rret, wret, xret = select.select(rlist + hlist, wlist,
                                                 xlist, timeout)

              # if self._log:
              #     self._log.debug('rppoll 0 : %s - %s - %s', rret, wret, xret)

                # do not return hlist-only FDs
                ret += [[fd, POLLOUT] for fd in set(wret)]
                ret += [[fd, POLLERR] for fd in set(xret)]

                # A socket being readable may also mean the emdpoint has been
                # closed.  We do a zero-length read to check for POLLHUP.
                #
                # NOTE: I am so happy we can do type inspection in Python, so
                #       that we can have different checks for, say, files and
                #       sockets.  Only this shit doesn't work on sockets! Argh!
                #       Oh well, we now guess the type from `recv` and `read`
                #       methods being available.
                #       Its a shame to do this on every `poll()` call, as this
                #       is likely in a performance critical path...
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
                            fd.write(b'')
                        except Exception:
                            hup = True

                    # socket
                    elif hasattr(fd, 'recv'):
                        try:
                            fd.recv(0)
                            fd.send(b'')
                        except Exception:
                            hup = True

                    # we can't handle errors on other types
                    else:
                        raise TypeError('cannot check %s [%s]' % (fd, type(fd)))

                    # we always return the POLLIN event, too, as that is what
                    # Python natively does.  I don't think it makes sense,
                    # semantically, but whatever...
                    if hup: ret.append([fd, POLLIN | POLLHUP])
                    else  : ret.append([fd, POLLIN])

              # if self._log:
              #     self._log.debug('rppoll 1 : %s (%s)', ret, rlist)

                return ret


# ------------------------------------------------------------------------------

