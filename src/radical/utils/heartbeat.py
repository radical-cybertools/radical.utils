
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
    def __init__(self, uid, timeout, frequency=1):
        '''
        This is a simple hearteat monitor: after construction, it needs to be
        called in ingtervals shorter than the given `timeout` value.  A thread
        will be created which checks if heartbeats arrive timeely - if not, the
        current process is killed via `os.kill()`.

        If no timeout is given, all class methods are noops.
        '''

        self._uid     = uid
        self._timeout = timeout
        self._freq    = frequency
        self._last    = time.time()
        self._cnt     = 0
        self._pid     = os.getpid()

        if self._timeout:
            self._watcher = mt.Thread(target=self._watch)
            self._watcher.daemon = True
            self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        while True:

            time.sleep(self._freq)

            if time.time() - self._last > self._timeout:

                os.kill(self._pid, signal.SIGTERM)
                sys.stderr.write('Heartbeat timeout: %s\n' % self._uid)
                sys.stderr.flush()


    # --------------------------------------------------------------------------
    #
    def beat(self, timestamp=None):

        if not self._timeout:
            return

        if not timestamp:
            timestamp = time.time()

        self._last = timestamp
        self._cnt += 1


# ------------------------------------------------------------------------------

