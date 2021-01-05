#!/usr/bin/env python

import os
import sys
import time
import pytest


import multiprocessing as mp
import radical.utils   as ru


fname = '/tmp/ru.test.lockfile.%s' % os.getpid()


def _get_lock(_fname, delay, timeout, duration):
    lock = ru.Lockfile(_fname, delete=True)
    try:
        time.sleep(delay)
        with lock(timeout=timeout):
            time.sleep(duration)
            sys.exit(1)
    except TimeoutError:
        sys.exit(2)
    except RuntimeError:
        sys.exit(3)


# ------------------------------------------------------------------------------
#
def test_lockfile():

    try:
        with ru.Lockfile(fname, owner='foo') as fd0:
            assert(fd0)
            assert(fd0.get_owner() == 'foo'), fd0.get_owner()
            fd0.write('test 0\n')

        with ru.Lockfile(fname) as fd1:
            assert(fd1)
            fd1.lseek(0, os.SEEK_SET)
            fd1.write('test 1\n')

        with ru.Lockfile(fname) as fd2:
            assert(fd2)
            fd2.lseek(0, os.SEEK_END)
            fd2.write('test 2\n')

            with pytest.raises(RuntimeError):
                with ru.Lockfile(fname) as fd3:
                    fd3.lseek(0, os.SEEK_END)
                    fd3.write('test 3\n')

        with open(fname, 'r') as fin:
            data = fin.read()

            assert(data == 'test 1\ntest 2\n')

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_lockfile_timer():

    try:

        with ru.Lockfile(fname) as fd0:
            assert(fd0)
            fd0.write('test 0\n')

            fd1   = None
            start = time.time()
            with pytest.raises(TimeoutError):
                fd1 = ru.Lockfile(fname)
                fd1.acquire(timeout=0.1)

            stop = time.time()
            assert(stop - start > 0.1)
            assert(stop - start < 0.2)

            if fd1:
                try:
                    fd1.release()
                except:
                    pass

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_lockfile_ok():

    try:

        p1 = mp.Process(target=_get_lock, args=[fname, 0.0, 0.0, 0.2])
        p2 = mp.Process(target=_get_lock, args=[fname, 0.1, 0.3, 0.0])

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        assert(p1.exitcode == 1), p1.exitcode
        assert(p2.exitcode == 1), p2.exitcode

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_lockfile_nok():

    try:

        p1 = mp.Process(target=_get_lock, args=[fname, 0.0, 0.0, 0.3])
        p2 = mp.Process(target=_get_lock, args=[fname, 0.1, 0.1, 0.1])

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        assert(p1.exitcode == 1), p1.exitcode
        assert(p2.exitcode == 2), p2.exitcode

    finally:
        try   : os.unlink(fname)
        except: pass


    try:

        p1 = mp.Process(target=_get_lock, args=[fname, 0.0, 0.0, 0.5])
        p2 = mp.Process(target=_get_lock, args=[fname, 0.1, 0.0, 0.0])

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        assert(p1.exitcode == 1), p1.exitcode
        assert(p2.exitcode == 3), p2.exitcode

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_lockfile_delete():

    try:
        p1 = mp.Process(target=_get_lock, args=[fname, 0.0, 0.0, 0.5])
        p1.start()
        time.sleep(0.1)
        assert(os.path.isfile(fname))

        p1.join()
        assert(not os.path.isfile(fname))
        assert(p1.exitcode == 1)

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lockfile()
    test_lockfile_timer()
    test_lockfile_ok()
    test_lockfile_nok()
    test_lockfile_delete()

# ------------------------------------------------------------------------------

