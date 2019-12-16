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
        time.sleep(2.0)

    pid = ru.daemonize(main=main)

    assert(pid)
    assert(os.kill(int(pid), 0) is None)  # process should exist

    time.sleep(3)
    with pytest.raises(OSError):
        os.kill(pid, 0)  # process should be gone

    # cleanup
    try   : os.kill(pid, 9)
    except: pass


# ------------------------------------------------------------------------------
#
def test_daemon_class():
    '''
    start a damon which sleeps for 2 seconds
    '''

    # --------------------------------------------------------------------------
    class P(ru.Daemon):

        def __init__(self):
            ru.Daemon.__init__(self, stdin=None, stdout=None, stderr=None)

        def run(self):
            time.sleep(2.0)
    # --------------------------------------------------------------------------

    p = P()
    p.start()
    assert(p.pid)
    assert(os.kill(p.pid, 0) is None)  # process should exist

    time.sleep(3)
    with pytest.raises(OSError):
        os.kill(p.pid, 0)  # process should be gone

    # cleanup
    try   : p.stop()
    except: pass


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_daemonize()
    test_daemon_class()


# ------------------------------------------------------------------------------

