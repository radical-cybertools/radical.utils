
import os
import time
import fcntl

import threading as mt


# ------------------------------------------------------------------------------
#
class Lockfile(object):
    '''
    This class represents a lockfile.  On calling `open()` (or entering
    a resource context `with`), the lockfile is exclusively opened and locked.
    The returned object can be used for `read()`, `write()` and `seek()`
    operations, and the lock is only released on `close()` (or when leaving the
    resource context).

    Example:

        with ru.Lockfile(fname) as fd0:
            assert(fd0)
            fd0.write('test 0\n')

        with ru.Lockfile(fname) as fd1:
            assert(fd1)
            fd1.lseek(0, os.SEEK_SET)
            fd1.write('test 1\n')

        with ru.Lockfile(fname) as fd2:
            assert(fd2)
            fd2.lseek(0, os.SEEK_END)
            fd2.write('test 2\n')

            # this would raise a RuntimeError
          # with ru.Lockfile(fname) as fd3:
          #     fd3.lseek(0, os.SEEK_END)
          #     fd3.write('test 3\n')

        with open(fname, 'r') as fin:
            data = fin.read()
            assert(data == 'test 1\ntest 2\n')
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, fname):

        self._fname = fname
        self._fd    = None
        self._tlock = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def __enter__(self):

        self.acquire()
        return self


    # --------------------------------------------------------------------------
    #
    def __exit__(self, foo, bar, baz):

        return self.release()


    # --------------------------------------------------------------------------
    #
    def acquire(self, timeout=None):

        with self._tlock:

            if self._fd:
                raise RuntimeError('cannot call open twice')

            start = time.time()
            while True:

                try:
                    fd  = os.open(self._fname, os.O_RDWR | os.O_CREAT, 0o600)
                    ret = fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                    if not ret:
                        # fd is be valid and locked
                        self._fd = fd
                        return True

                    else:
                        # try again
                        if fd:
                            os.close(fd)
                        continue

                except IOError:
                    # try again
                    if fd:
                        os.close(fd)
                    continue

                if timeout is None:
                    continue  # never stop trying

                now = time.time()
                if now - start > timeout:
                    # timed out
                    return False

                time.sleep(0.1)

            assert(False), 'should never get here'


    # --------------------------------------------------------------------------
    #
    def locked(self):

        with self._tlock:

            if self._fd:
                return True

            return False


    # --------------------------------------------------------------------------
    #
    def release(self):

        with self._tlock:
            if not self._fd:
                raise ValueError('lockfile is not open')

            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


    # --------------------------------------------------------------------------
    #
    def read(self, length):

        if not self._fd:
            raise ValueError('lockfile is not open')

        return os.read(self._fd, length)


    # --------------------------------------------------------------------------
    #
    def write(self, data):

        if not self._fd:
            raise ValueError('lockfile is not open')

        return os.write(self._fd, data)


    # --------------------------------------------------------------------------
    #
    def seek(self, pos, how):

        return self.lseek(pos, how)


    # --------------------------------------------------------------------------
    #
    def lseek(self, pos, how):

        if not self._fd:
            raise ValueError('lockfile is not open')

        return os.lseek(self._fd, pos, how)


    # --------------------------------------------------------------------------
    #
    def remove(self):

        try:    os.unlink(self._fname)
        except: pass


# ------------------------------------------------------------------------------

