

import os
import time
import errno

import threading as mt

from .misc   import as_bytes
from .debug  import get_caller_name


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

        with ru.Lockfile(fname) as lf0:
            lf0.write('test 0\n')

        with ru.Lockfile(fname) as lf1:
            lf1.lseek(0, os.SEEK_SET)
            lf1.write('test 1\n')

        with ru.Lockfile(fname) as lf2:
            lf2.lseek(0, os.SEEK_END)
            lf2.write('test 2\n')

            # raises a RuntimeError, as we won't be able to acquire the lock
            with ru.Lockfile(fname) as lf3:
                lf3.lseek(0, os.SEEK_END)
                lf3.write('test 3\n')

        with open(fname, 'r') as fin:
            data = fin.read()
            assert(data == 'test 1\ntest 2\n')

    Example:

        lf1 = ru.Lockfile(fname)
        lf2 = ru.Lockfile(fname)

        with lf1:
            lf1.write('test 0')

            with lf2(timeout=3):
                # we won't reach this, the `with` above will time out and raise
                lf2.write('test 0')


    Implementation:

    The implementation relies on cooperative locking: a process or thread which
    uses this API to lock a file will create a temporary file in the same
    directory (filename: `.<filename>.<pid>.<counter>` where `counter` is
    a process singleton).  On lock acquisition, we will attempt to create
    a symbolic link to the file `<filename>.lck`, which will only succeed if no
    other process or thread holds that lock.  On `release`, that symbolic gets
    removed; on object destruction, the temporary file gets removed.

    The private file will contain the name of the owner.  Any other process or
    thread can follow the `.<filename>.lck` symlink, open the link's target
    file, and read the name of the owner.  No attempt is made to avoid races
    between failure to acquire the lockfile and querying the current owner, so
    that information is not reliable, but intented to be an informative help for
    debugging and tracing purposes.

    Example:
        lf1 = ru.Lockfile(fname)
        lf2 = ru.Lockfile(fname)

        with lf1:
            lf1.write('test 0')

            try:
                lf2.acquire()  # timeout == None == 0.0
            except RuntimeError:
                print('lock is held by %s' % lf2.get_owner())
    '''

    _counter = 0
    _lock    = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def __init__(self, fname, *args, **kwargs):
        '''
        The `args` and `kwargs` arguments are passed to `acquire()` when used in
        a `with Lockfile():` clause:

            with ru.Lockfile(fname, timeout=3) as lockfile:
                lockfile.write(data)

        '''

        kwargs.setdefault('delete', False)

        self._fname   = fname
        self._fd      = None

        if 'delete' in kwargs:
            self._delete = kwargs['delete']
            del(kwargs['delete'])
        else:
            self._delete = False

        self._args    = args
        self._kwargs  = kwargs

        # create a tempfile to be used for lock acquisition
        with Lockfile._lock:
            cnt = Lockfile._counter
            Lockfile._counter += 1


        fname_base = os.path.basename(self._fname)
        fname_dir  = os.path.dirname(self._fname)

        self._lck  = '%s/%s.lck'    % (fname_dir, fname_base)
        self._tmp  = '%s/.%s.%d.%d' % (fname_dir, fname_base, os.getpid(), cnt)

        # make sure our tmp file exists
        with open(self._tmp, 'w') as fout:
            fout.write('%s\n' % get_caller_name())


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

        timeout:
            < 0 : wait forever
              0 : try locking once and raise `RuntimeError` on failure
            > 0 : try for that many seconds, and raise `TimeoutError` on failure
            None: same as 0

        '''

        if timeout is None:
            timeout = 0.0

        if self._fd:
            raise RuntimeError('cannot call open twice')

        # pre-emptively record who wants to acquire the lock
        if not owner:
            owner = get_caller_name()

        with open(self._tmp, 'w') as fout:
            fout.write('%s\n' % owner)

        start = time.time()
        while True:
            # attempt to link self._tmp to self._lck.  Once that succeeds, open
            # the self._fnmame for read/write.

            try:
                os.symlink(self._tmp, self._lck)
                break  # link succeeded, we have the lock

            # if self._lck exists the above `link` will raise, and we try
            # again after a bit.  All other errors are fatal
            except OSError as e:

                # pylint: disable=W0707
                if not e.errno == errno.EEXIST:
                    raise

                if timeout == 0.0:
                    raise RuntimeError('failed to lock %s (%s)'
                                       % (self._fname, self.get_owner()))

                elif timeout > 0:
                    now = time.time()
                    if now - start > timeout:
                        raise TimeoutError(errno.ETIME, 'lock timeout for %s ()'
                                           % self._fname, self.get_owner())
                # try again
                time.sleep(0.1)  # FIXME: granulatiy?
                continue

        # the link succeeded, so open the file and break
        self._fd = os.open(self._fname, os.O_RDWR | os.O_CREAT)


    # --------------------------------------------------------------------------
    #
    def get_owner(self):
        '''
        That name of the current owner of the lockfile can be retrieved by any
        other process or thread which attempts to lock the same file.  This
        method returns `None` if the file is not currently locked.

        Note: this method may race against lock acquisition / release, and the
              information returned may be outdated.
        '''

        try:
            with open(self._lck, 'r') as fin:
                # strip newline
                return(fin.readline()[:-1])

        except OSError as e:
            if e.errno == errno.EEXIST:
                return None
            else:
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

        # release the file handle
        os.close(self._fd)
        self._fd = None

        if self._delete:
            os.unlink(self._fname)

        # release the lock
        os.unlink(self._lck)


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

