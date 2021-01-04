

import os
import time
import errno
import fcntl

from .misc  import as_bytes
from .debug import get_caller_name


# ------------------------------------------------------------------------------
#
class Lockfile(object):
    '''
    This class represents a lockfile.  On calling `open()` (or entering
    a resource context `with`), the lockfile is exclusively opened and locked.
    The returned object can be used for `read()`, `write()` and `seek()`
    operations, and the lock is only released on `close()` (or when leaving the
    resource context).

    If `delete=True` is specified on construction, then the lockfile is removed
    uppon `release()`, and the content is lost.  Only for this mode is it
    possible to obtain the current lock owner when using `get_owner()` while
    waiting for a lock owned by another process or thread - the call will return
    'unknown' otherwise.

    Note that the file offset is not shared between owner of the lock file
    - a newly acquired lock on the file should prompt a seek to the desired
    file location (the initial offset is '0').

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
    def __init__(self, fname, delete=False, *args, **kwargs):
        '''
        The `args` and `kwargs` arguments are passed to `acquire()` when used in
        a `with Lockfile():` clause:

            with ru.Lockfile(fname, timeout=3) as flock:
                flock.write(data)

        '''

        self._fname   = fname
        self._fd      = None
        self._delete  = delete

        self._args    = args
        self._kwargs  = kwargs


    # --------------------------------------------------------------------------
    #
    def __call__(self, *args, **kwargs):
        '''
        helper method to pass arguments while using the `with lock` clause:

            with lock(timeout=2, owner=foo):
                lock.write(data)
        '''

        self._args   = args
        self._kwargs = kwargs

        return self


    # --------------------------------------------------------------------------
    #
    def __enter__(self):

        self.acquire(*self._args, **self._kwargs)
        return self


    # --------------------------------------------------------------------------
    #
    def __exit__(self, foo, bar, baz):

        return self.release()


    # --------------------------------------------------------------------------
    #
    def acquire(self, timeout=None, owner=None):
        '''
        When the lock could not be acquired after `timeout` seconds, an
        `TimeoutError` is raised (i.e. an `OSError` with errno `ETIME`).  When
        `owner` is specified and the lockfile was created with `delete=True`,
        then that string will be passed to any other method which tries to
        acquire this lockfile while it is locked (see `get_owner()`).  If not
        specified, the `owner` string is set to `ru.get_caller_name()`.
        '''

        if self._fd:
            raise RuntimeError('cannot call open twice')

        start = time.time()
        while True:

            try:
                fd  = os.open(self._fname, os.O_RDWR | os.O_CREAT)
                ret = fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                if not ret:
                    self._fd = fd
                    break  # fd should be valid and locked

                else:  # try again
                    os.close(fd)

            except IOError:
                # try again
                pass

            if timeout is None:
                break  # stop trying

            now = time.time()
            if now - start > timeout:
                raise TimeoutError(errno.ETIME, 'lock timeout for %s ()' %
                                                self._fname, self.get_owner())

            time.sleep(0.1)

        if not self._fd:
            raise RuntimeError('failed to lock %s (%s)' % (self._fname,
                                                           self.get_owner()))

        if self._delete:
            if owner: os.write(self._fd, as_bytes(owner))
            else    : os.write(self._fd, as_bytes(get_caller_name()))

        os.fsync(self._fd)


    # --------------------------------------------------------------------------
    #
    def get_owner(self):
        '''
        If the lockfile was created with `delete=True`, then the name of the
        method which successfully calles `acquire()` is strored in the file.
        That name can then be retrieved by any other process or thread which
        attempts to lock the same file.  Otherwise this method returns
        'unknown'.
        '''

        if not self._delete:
            return 'unknown'

        if not os.path.isfile(self._fname):
            return None

        try:
            with open(self._fname, 'r') as fin:
                return(fin.read())

        except:
            return 'unknown'


    # --------------------------------------------------------------------------
    #
    def release(self):
        '''
        Release the lock on the file.  If `delete=True` was specified on
        construction, then the file (and all owner information) are removed.
        Once released, all other threads/processes waiting in the `acquire()`
        method call will compete for the lock, and one of them will obtain it.
        '''

        if not self._fd:
            raise RuntimeError('lockfile is not open')

        if self._delete:
            os.unlink(self._fname)

        os.close(self._fd)


    # --------------------------------------------------------------------------
    #
    def read(self, length):
        '''
        Read from the locked file at the current offset.  This method will raise
        an `RuntimeError` when being called without the file being locked.
        '''

        if not self._fd:
            raise RuntimeError('lockfile is not open')

        return os.read(self._fd, length)


    # --------------------------------------------------------------------------
    #
    def write(self, data):
        '''
        Write to the locked file at the current offset.  This method will raise
        an `RuntimeError` when being called without the file being locked.
        '''

        if not self._fd:
            raise RuntimeError('lockfile is not open')

        return os.write(self._fd, as_bytes(data))


    # --------------------------------------------------------------------------
    #
    def seek(self, pos, how):
        '''
        Same as `lseek()`
        '''

        return self.lseek(pos, how)


    # --------------------------------------------------------------------------
    #
    def lseek(self, pos, how):
        '''
        Change the offset at which the next read or write method is applied.
        The arguments are interpreted as documented by `os.lseek()`.
        '''

        if not self._fd:
            raise RuntimeError('lockfile is not open')

        return os.lseek(self._fd, pos, how)


# ------------------------------------------------------------------------------

