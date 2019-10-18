
import os
import time
import signal

import threading as mt


# ------------------------------------------------------------------------------
#
class Heartbeat(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid, timeout, interval=1, cb=None, term=None, log=None):
        '''
        This is a simple hearteat monitor: after construction, it's `beat()`
        method needs to be called in intervals shorter than the given `timeout`
        value.  A thread will be created which checks if heartbeats arrive
        timely - if not, the current process is killed via `os.kill()`.

        If a callback `cb` is specified, the watcher will also invoke that
        callback after every `interval` seconds.  This can be used to ensure
        heartbeats by the owning instance (which may feed this or any other
        `Heartbeat` monitor instance).  The `term` callback is invoked on
        heartbeat failure, i.e., just before the process is killed.
        '''

        if interval > timeout:
            raise ValueError('timeout [%.1f] too small [>%.1f]'
                            % (timeout, interval))

        self._uid      = uid
        self._log      = log
        self._timeout  = timeout
        self._interval = interval
        self._cb       = cb
        self._term     = term

        self._tstamps  = dict()
        self._pid      = os.getpid()

        self._watcher  = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        while True:

            time.sleep(self._interval)
            now = time.time()

            if  self._cb:
                self._cb()

            # avoid iteration over changing dict
            uids = list(self._tstamps.keys())
            for uid in uids:

                # use `get()` in case python dict population is not atomic
                last = self._tstamps.get(uid)
                if not last:
                    continue

                if now - last > self._timeout:

                    if self._log:
                        self._log.warn('hb %s[%s]: %.1f - %.1f > %1.f: timeout',
                                       self._uid, uid, now, last, self._timout)

                    if  self._term:
                        self._term()

                    os.kill(self._pid, signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def beat(self, uid=None, timestamp=None):

        if not timestamp:
            timestamp = time.time()

        if not uid:
            uid = 'default'

        if self._log:
            self._log.debug('hb %s[%s]: %.1f %.1f', self._uid, uid, self._last,
                                                                      timestamp)
        self._tstamps[uid] = timestamp


# ------------------------------------------------------------------------------

