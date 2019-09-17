
import os
import sys
import time
import signal

import threading as mt


# ------------------------------------------------------------------------------
#
class Heartbeat(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid, timeout, interval=1, log=None):
        '''
        This is a simple hearteat monitor: after construction, it needs to be
        called in intervals shorter than the given `timeout` value.  A thread
        will be created which checks if heartbeats arrive timely - if not, the
        current process is killed via `os.kill()`.
        '''

        self._uid      = uid
        self._log      = log
        self._timeout  = timeout
        self._interval = interval
        self._last     = time.time()
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

            if now - self._last > self._timeout:

                if self._log:
                    self._log.warn('heartbeat %s: %.1f - %.1f > %1.f: timeout',
                                       self._uid, now, self._last, self._timout)

                os.kill(self._pid, signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def beat(self, timestamp=None):

        if not timestamp:
            timestamp = time.time()

        if self._log:
            self._log.debug('heartbeat %s: %.1f %.1f', self._uid, self._last,
                                                                      timestamp)

        self._last = timestamp


# ------------------------------------------------------------------------------

