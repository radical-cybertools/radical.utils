#!/usr/bin/env python

__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


'''
Unit tests for `ru.Dameon()` and `ru.daemonize()`
'''

import os
import time
import pytest

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_daemonize():
    '''
    start a damon which sleeps for 2 seconds
    '''

    def main():
        print('main 1')
        time.sleep(2.0)
        print('main 2')

    print('0')
    pid = ru.daemonize(main=main)
    print('1')

    assert(pid)
    assert(os.kill(int(pid), 0) is None)  # process should exist
    print('2')

    time.sleep(3)
    print('3')
    with pytest.raises(OSError):
        os.kill(pid, 0)  # process should be gone
    print('4')

    # cleanup
    try   : os.kill(pid, 9)
    except: pass
    print('5')


# ------------------------------------------------------------------------------
#
def test_daemon_class():
    '''
    start a damon which sleeps for 2 seconds
    '''

    # --------------------------------------------------------------------------
    class P(ru.Daemon):

        def __init__(self):
            print('init 1')
            ru.Daemon.__init__(self, stdin=None, stdout=None, stderr=None)
            print('init 2')

        def run(self):
            print('run 1')
            time.sleep(2.0)
            print('run 2')
    # --------------------------------------------------------------------------

    print('main 0')
    p = P()
    p.start()
    assert(p.pid)
    assert(os.kill(p.pid, 0) is None)  # process should exist
    print('main 1')

    print('main 2')
    time.sleep(3)
    print('main 3')
    with pytest.raises(OSError):
        os.kill(p.pid, 0)  # process should be gone
    print('main 4')

    # cleanup
    print('main 5')
    try   : p.stop()
    except: pass
    print('main 6')


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_daemonize()
    test_daemon_class()


# ------------------------------------------------------------------------------

