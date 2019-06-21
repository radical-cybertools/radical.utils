

import os
import time
import fcntl


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
    def __init__(self, fname, timeout=None):

        self._fname   = fname
        self._timeout = timeout
        self._fd      = None


    # --------------------------------------------------------------------------
    #
    def __enter__(self):

        if self._timeout is None:
            self.open()
            return self

        start = time.time()
        while True:

            try:
                self.open()
                return self
            except:
                pass

            now = time.time()
            if now - start > self._timeout:
                raise RuntimeError('lock timeout for %s' % self._fname)

            time.sleep(0.5)


    # --------------------------------------------------------------------------
    #
    def open(self):

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


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self._fd:
            os.close(self._fd)


    # --------------------------------------------------------------------------
    #
    def read(self, length):

        if not self._fd:
            raise ValueError('lockfile is not open')

        return os.read(self._fd, length)


    # --------------------------------------------------------------------------
    #
    def write(self, data):

        print self._fd
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
    def __exit__(self, foo, bar,  baz):

        return self.close()


# ------------------------------------------------------------------------------

