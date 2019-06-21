#!/usr/bin/env python

import os
import time
import pytest

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lockfile():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    try:
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

            with pytest.raises(RuntimeError):
                with ru.Lockfile(fname) as fd3:
                    fd3.lseek(0, os.SEEK_END)
                    fd3.write('test 3\n')

        with open(fname, 'r') as fin:
            data = fin.read()

            assert(data == 'test 1\ntest 2\n')

    finally:
        try:
            os.unlink(fname)
        except:
            pass


# ------------------------------------------------------------------------------
#
def test_lockfile_timer():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    try:
        with ru.Lockfile(fname) as fd0:
            assert(fd0)
            fd0.write('test 0\n')
            print fd0._fname

            fd1   = None
            start = time.time()
            with pytest.raises(RuntimeError):
                fd1 = ru.Lockfile(fname, timeout=2)

            stop = time.time()
            assert(stop - start > 2.0)
            assert(stop - start < 3.0)

            if fd1:
                fd1.close()
    finally:
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

