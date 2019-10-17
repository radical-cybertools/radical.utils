
import os
import time
import signal

import threading as mt

from .misc   import as_list
from .logger import Logger


# ------------------------------------------------------------------------------
#
class Heartbeat(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid, timeout, interval=1, cb=None, term_cb=None,
                       log=None):
        '''
        This is a simple hearteat monitor: after construction, it's `beat()`
        method needs to be called in intervals shorter than the given `timeout`
        value.  A thread will be created which checks if heartbeats arrive
        timely - if not, the current process is killed via `os.kill()` (but see
        below).

        If a callback `cb` is specified, the watcher will also invoke that
        callback after every `interval` seconds.  This can be used to ensure
        heartbeats by the owning instance (which may feed this or any other
        `Heartbeat` monitor instance).  The `term_cb` callback is invoked on
        heartbeat failure, i.e., just before the process would be killed, and
        gets a single argument, the uid of the failing heartbeeat sender.  If
        that term callback returns `True`, the kill is avoided though: timers
        are reset and everything continues like before.  This should be used to
        recover from failing components.
        '''

        if interval > timeout:
            raise ValueError('timeout [%.1f] too small [>%.1f]'
                            % (timeout, interval))

        self._uid      = uid
        self._log      = log
        self._timeout  = timeout
        self._interval = interval
        self._cb       = cb
        self._term_cb  = term_cb
        self._term     = mt.Event()

        if not self._log:
            self._log  = Logger('radical.utils.heartbeat')

        self._tstamps  = dict()
        self._pid      = os.getpid()

        self._watcher  = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._term.set()
        self._watcher.join()


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        while not self._term.is_set():

            time.sleep(self._interval)
            now = time.time()

            if  self._cb:
                self._cb()

            # avoid iteration over changing dict
            uids = list(self._tstamps.keys())
            for uid in uids:

              # self._log.debug('hb %s check %s', self._uid, uid)

                # use `get()` in case python dict population is not atomic
                last = self._tstamps.get(uid)
                if not last:
                    self._log.warn('hb %s[%s]: initial hb missing', self._uid, uid)
                    continue

                if now - last > self._timeout:

                    if self._log:
                        self._log.warn('hb %s[%s]: %.1f - %.1f > %1.f: timeout',
                                       self._uid, uid, now, last, self._timeout)

                    ret = False
                    if  self._term_cb:
                        ret = self._term_cb(uid)

                    if ret is True:
                        # recovered - remove that failed UID from the registry
                        self._log.info('hb recovered for %s', uid)
                        del(self._tstamp[uid])

                    else:
                        # could not recover: abandon mothership
                        self._log.warn('hb failure for %s - fatal', uid)
                        from .logger import _logger_registry
                        _logger_registry.close_all()

                        os.kill(self._pid, signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def beat(self, uid=None, timestamp=None):

        if not timestamp:
            timestamp = time.time()

        if not uid:
            uid = 'default'

      # last = self._tstamps.get(uid, 0.0)
      # self._log.debug('hb %s beat [%s]', self._uid, uid)
        self._tstamps[uid] = timestamp


    # --------------------------------------------------------------------------
    #
    def is_alive(self, uid=None):
        '''
        Check if an entity of the given UID sent a recent heartbeat
        '''

        if not uid:
            uid = 'default'

        ts = self._tstamps.get(uid)

        if ts and time.time() - ts <= self._timeout:
            return True

        return False


    # --------------------------------------------------------------------------
    #
    def wait_startup(self, uids=None, timeout=None):
        '''
        Wait for the first heartbeat of the given UIDs to appear.  This returns
        the list of UIDs which have *not* been found, or `None` otherwise.
        '''

        if not uids:
            uids = ['default']

        uids = as_list(uids)

        start = time.time()
        ok    = list()
        while True:

            ok = [uid for uid in uids if self._tstamps.get(uid)]
            if len(ok) == len(uids):
                break

            if timeout:
                if time.time() - start > timeout:
                    break

            time.sleep(0.05)

        if len(ok) != len(uids):
            return [uid for uid in uids if uid not in ok]


# ------------------------------------------------------------------------------

