

import os
import time
import fcntl


# ------------------------------------------------------------------------------
#
class Lockfile(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, fname, timeout=None):

        self._fname   = fname
        self._timeout = timeout
        self._fd      = None


    # --------------------------------------------------------------------------
    #
    def __enter__(self):

        if self._timeout is None:

            return self._open()

        start = time.time()

        while True:

            try:
                return self._open()
            except:
                pass

            now = time.time()
            if now - start > self._timeout:
                raise RuntimeError('lock timeout for %s' % self._fname)

            time.sleep(0.5)


    # --------------------------------------------------------------------------
    #
    def _open(self):

        try:
            fd = os.open(self._fname, os.O_RDWR | os.O_CREAT)
            ret = fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        except IOError:
            raise RuntimeError('Could not lock %s' % self._fname)

        if not ret == 0:
            self._fd = fd

        else:
            os.close(fd)
            self._fd = None

        if not self._fd:
            raise RuntimeError('failed to lock %s' % self._fname)

        return self._fd


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self._fd:
            os.close(self._fd)


    # --------------------------------------------------------------------------
    #
    def __exit__(self, foo, bar,  baz):

        return self.close()


# ------------------------------------------------------------------------------

