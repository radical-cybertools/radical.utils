#!/usr/bin/env python

import os
import sys
import time
import errno
import pytest


import multiprocessing as mp
import radical.utils   as ru


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lockfile():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    with ru.Lockfile(fname) as fd0:
        assert(fd0)
        assert(fd0.get_owner() == 'unknown')
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

    try:
        os.unlink(fname)
    except:
        pass


# ------------------------------------------------------------------------------
#
def test_lockfile_timer():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

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

    try:
        os.unlink(fname)
    except:
        pass


# ------------------------------------------------------------------------------
#
def test_lockfile_ok():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    def get_lock(delay, timeout):
        lock = ru.Lockfile(fname)
        try:
            with lock(timeout=timeout):
                time.sleep(delay)
        except TimeoutError as e:
            sys.exit(2)


    p1 = mp.Process(target=get_lock, args=[0.0, 0.1])
    p2 = mp.Process(target=get_lock, args=[0.3, 0.0])

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    assert(p1.exitcode == 0)
    assert(p2.exitcode == 2)


    try:
        os.unlink(fname)
    except:
        pass


# ------------------------------------------------------------------------------
#
def test_lockfile_nok():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    def get_lock(delay, timeout):
        lock = ru.Lockfile(fname)
        try:
            with lock(timeout=timeout):
                time.sleep(delay)
        except TimeoutError as e:
            sys.exit(2)


    p1 = mp.Process(target=get_lock, args=[0.0, 0.3])
    p2 = mp.Process(target=get_lock, args=[0.1, 0.0])

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    assert(p1.exitcode == 0)
    assert(p2.exitcode == 2)


    try:
        os.unlink(fname)
    except:
        pass


# ------------------------------------------------------------------------------
#
def test_lockfile_delete():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    def get_lock(delay, timeout):
        lock = ru.Lockfile(fname, delete=True)
        try:
            with lock(timeout=timeout):
                time.sleep(delay)
        except TimeoutError as e:
            sys.exit(2)


    p1 = mp.Process(target=get_lock, args=[0.0, 0.3])
    p1.start()
    p1.join()

    assert(p1.exitcode == 0)
    assert(not os.path.isfile(fname))

    try:
        os.unlink(fname)
    except:
        pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_lockfile()
    test_lockfile_timer()

# ------------------------------------------------------------------------------

