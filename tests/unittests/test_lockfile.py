#!/usr/bin/env python

import os
import pytest

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lockfile():

    fname = '/tmp/ru.test.lockfile.%s' % os.getpid()

    try:
        with ru.Lockfile(fname) as fd0:
            assert(fd0)
            os.write(fd0, 'test 0\n')

        with ru.Lockfile(fname) as fd1:
            assert(fd1)
            os.lseek(fd1, 0, os.SEEK_SET)
            os.write(fd1, 'test 1\n')

        with ru.Lockfile(fname) as fd2:
            assert(fd2)
            os.lseek(fd2, 0, os.SEEK_END)
            os.write(fd2, 'test 2\n')

            with pytest.raises(RuntimeError):
                with ru.Lockfile(fname) as fd3:
                    os.lseek(fd3, 0, os.SEEK_END)
                    os.write(fd3, 'test 3\n')

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
if __name__ == '__main__':

    test_lockfile()


# ------------------------------------------------------------------------------

