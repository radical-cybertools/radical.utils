
import os
import sys
import time
import pprint
import signal

import threading as mt

from .misc   import as_list
from .logger import Logger


# ------------------------------------------------------------------------------
#
class Heartbeat(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid, timeout, interval=1, beat_cb=None, term_cb=None,
                       log=None):
        '''
        This is a simple hearteat monitor: after construction, it's `beat()`
        method needs to be called in intervals shorter than the given `timeout`
        value.  A thread will be created which checks if heartbeats arrive
        timely - if not, the current process is killed via `os.kill()` (but see
        below).

        If a callback `beat_cb` is specified, the watcher will also invoke that
        callback after every `interval` seconds.  This can be used to ensure
        heartbeats by the owning instance (which may feed this or any other
        `Heartbeat` monitor instance).  The `term_cb` callback is invoked on
        heartbeat failure, i.e., just before the process would be killed, and
        gets a single argument, the uid of the failing heartbeat sender.  If
        that term callback returns `True`, the kill is avoided though: timers
        are reset and everything continues like before.  This should be used to
        recover from failing components.

        When timeout is set to `None`,  no trigger action on missing heartbeats
        will ever be triggered.
        '''

        # we should not need to lock timestamps, in the current CPython
        # implementation, dict access is assumed to be atomic.  But heartbeats
        # should not be in the performance critical path, and should not have
        # threads competing with the (private) dict lock, so we accept the
        # overhead.

        if timeout and interval > timeout:
            raise ValueError('timeout [%.1f] too small [>%.1f]'
                            % (timeout, interval))

        self._uid      = uid
        self._log      = log
        self._timeout  = timeout
        self._interval = interval
        self._beat_cb  = beat_cb
        self._term_cb  = term_cb
        self._term     = mt.Event()
        self._lock     = mt.Lock()
        self._tstamps  = dict()
        self._pid      = os.getpid()
        self._watcher  = None

        if not self._log:
            self._log  = Logger('radical.utils.heartbeat')

        self._log.debug_1('hb %s create', self._uid)


    # --------------------------------------------------------------------------
    #
    def start(self):

        self._log.debug_1('hb %s start', self._uid)
        self._watcher  = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._term.set()

      # # no need to join, is a daemon thread
      # self._watcher.join()


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    def dump(self, log):

        if not log: log = self._log

        log.debug_9('hb dump %s: \n%s', self._uid, pprint.pformat(self._tstamps))


    # --------------------------------------------------------------------------
    #
    def watch(self, uid):

        with self._lock:
            if uid not in self._tstamps:
                self._log.debug_8('hb %s watch %s', self._uid, uid)
                self._tstamps[uid] = None


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        # initial heartbeat without delay
        if  self._beat_cb:
            self._log.debug_8('hb %s beat cb init', self._uid)
            self._beat_cb()

        while not self._term.is_set():

            self._log.debug_8('hb %s loop %s', self._uid, self._interval)

            time.sleep(self._interval)
            now = time.time()

            if  self._beat_cb:
                self._log.debug_8('hb %s beat cb', self._uid)
                self._beat_cb()

            # avoid iteration over changing dict
            with self._lock:
                uids = list(self._tstamps.keys())

            self._log.debug_8('hb %s uids %s', self._uid, uids)
            for uid in uids:

                self._log.debug_8('hb %s check %s', self._uid, uid)

                with self._lock:
                    last = self._tstamps.get(uid)

                if last is None:
                    self._log.warn('hb %s inval %s', self._uid, uid)
                    continue

                if now - last > self._timeout:

                    if self._log:
                        self._log.warn('hb %s tout  %s: %.1f - %.1f > %.1f',
                                       self._uid, uid, now, last, self._timeout)

                    ret = None
                    if self._timeout:
                        # attempt to recover
                        if  self._term_cb:
                            ret = self._term_cb(uid)
                    else:
                        # we silently assume that the watchee recovered, thus
                        # avoiding termination
                        ret = True

                    if ret in [None, False]:
                        # could not recover: abandon mothership
                        self._log.warn('hb %s fail  %s: fatal (%d)',
                                       self._uid, uid, self._pid)
                        os.kill(self._pid, signal.SIGTERM)
                        time.sleep(0.1)
                        os.kill(self._pid, signal.SIGKILL)

                    else:
                        # recovered - the failed UID was replaced with the one
                        # returned by the callback.  We delete the heartbeat
                        # information for the old uid and register a new
                        # heartbeat for the new one, so that we can immediately
                        # begin to watch it.
                        assert isinstance(ret, str)
                        self._log.info_1('hb %s recov %s -> %s (%s)',
                                          self._uid, uid, ret, self._term_cb)
                        with self._lock:
                            del self._tstamps[uid]
                            self._tstamps[ret] = time.time()


    # --------------------------------------------------------------------------
    #
    def beat(self, uid=None, timestamp=None):

        if not timestamp:
            timestamp = time.time()

        if not uid:
            uid = 'default'

        with self._lock:
            self._log.debug_9('hb %s beat [%s]', self._uid, uid)
            self._tstamps[uid] = timestamp


  # # --------------------------------------------------------------------------
  # #
  # def is_alive(self, uid=None):
  #     '''
  #     Check if an entity of the given UID sent a recent heartbeat
  #     '''
  #
  #     if not uid:
  #         uid = 'default'
  #
  #     with self._lock:
  #         ts = self._tstamps.get(uid)
  #
  #     if ts and time.time() - ts <= self._timeout:
  #         return True
  #
  #     return False


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

            with self._lock:
                ok  = [uid for uid in uids if self._tstamps.get(uid)]
                nok = [uid for uid in uids if uid not in ok]

            self._log.debug_3('wait for : %s', nok)

            if len(ok) == len(uids):
                break

            if timeout:
                if time.time() - start > timeout:
                    self._log.debug_3('wait time: %s', nok)
                    break

            time.sleep(0.25)

        if len(ok) != len(uids):
            nok = [uid for uid in uids if uid not in ok]
            self._log.error('wait fail: %s', nok)
            return nok

        else:
            self._log.debug_3('wait ok  : %s', ok)


# ------------------------------------------------------------------------------
#
class PWatcher(object):

    NOTHING = 'nothing'
    SUICIDE = 'suicide'
    KILLALL = 'killall'
    RAMPAGE = 'rampage'

    # --------------------------------------------------------------------------
    #
    def __init__(self, action=None, uid=None, log=None):
        '''

        This is a simple process monitor: once started it runs in a separate
        thread and monitors all given process IDs (`self._watch_pid`).  If
        a process is found to have died, the watcher will invoke the given
        action:

          - `nothing`: log event and do nothing else
          - `suicide`: kill the curent process
          - `killall`: kill all monitored pids
          - `rampage`: both of the above (`suicide + killall`)

        The default action is `rampage`.

        The passed uid (default: `pwatcher`) is used for logging purposes only.
        '''

        self._action = action or self.RAMPAGE
        self._uid    = uid    or 'pwatcher'
        self._log    = log    or Logger(name=self._uid, ns='radical.utils')
        self._pids   = list()
        self._lock   = mt.Lock()

        self._log.debug_1('pwatcher create')

        self._thread = mt.Thread(target=self._watch)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def _is_alive(self, pid):

        try           : os.kill(pid, 0)
        except OSError: return False
        else          : return True


    # --------------------------------------------------------------------------
    #
    def _kill(self, pid):

        try   : os.killpg(pid, signal.SIGTERM)
        except: pass

        try   : os.kill(pid, signal.SIGTERM)
        except: pass


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        self._log.debug_1('pwatcher started')

        while True:

            with self._lock:

                for pid in list(self._pids):

                    if not self._is_alive(pid):

                        self._log.warn('process %d died, exit', pid)
                        self._pids.remove(pid)

                        if   self._action == self.NOTHING: self._nothing(pid)
                        elif self._action == self.SUICIDE: self._suicide(pid)
                        elif self._action == self.KILLALL: self._killall(pid)
                        elif self._action == self.RAMPAGE: self._rampage(pid)

            time.sleep(0.05)


    # --------------------------------------------------------------------------
    #
    def watch(self, pid):

        self._log.debug('add pid %d to watchlist', pid)

        with self._lock:
            self._pids.append(pid)


    # --------------------------------------------------------------------------
    #
    def unwatch(self, pid):

        self._log.debug('remove pid %d from watchlist', pid)

        with self._lock:
            if pid in self._pids:
                self._pids.remove(pid)

    # --------------------------------------------------------------------------
    #
    def _nothing(self, pid):

        self._log.debug("process %d's demise triggered, well, nothing", pid)


    # --------------------------------------------------------------------------
    #
    def _suicide(self, pid):

        self._log.debug("process %d's demise triggered suicide", pid)
        self._kill(os.getpid())


    # --------------------------------------------------------------------------
    #
    def _killall(self, pid):

        self._log.debug("process %d's demise triggered killall (%s)",
                        pid, self._pids)

        for pid in list(self._pids):
            self._kill(pid)


    # --------------------------------------------------------------------------
    #
    def _rampage(self, pid):

        self._killall(pid)
        self._suicide(pid)


# ------------------------------------------------------------------------------

