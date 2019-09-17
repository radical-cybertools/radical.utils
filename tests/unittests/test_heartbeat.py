#!/usr/bin/env python

__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


'''
Unit tests for ru.Heartbeat()
'''

import os
import sys
import time

import multiprocessing as mp

import radical.utils   as ru


# ------------------------------------------------------------------------------
#
def test_hb():
    '''
    spawn a process with HB and expect it to live for 3 seconds.  If it dies
    before, it will return a non-zero exit code.  If it stays alive longer than
    6 seconds, we consider this an error
    '''

    # --------------------------------------------------------------------------
    def proc():

        hb = ru.Heartbeat('test', timeout=0.1, interval=0.01)
        t0 = time.time()

        try:
            while time.time() < t0 + 3:
                hb.beat()
                time.sleep(0.05)
            while True:
                time.sleep(1)

        finally:
            if time.time() > t0 + 3.2:
                sys.exit(-1)
    # --------------------------------------------------------------------------

    p = None
    try:
        p = mp.Process(target=proc)
        p.start()

        # proc should still be alive after 2 seconds
        time.sleep(2)
        assert(p.is_alive())

        # but it should have a zero exit value after 2 more seconds
        time.sleep(2)
        assert(not p.is_alive())
        assert(p.exitcode)

    finally:
        try   : os.kill(p.pid, 9)
        except: pass


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_hb()


# ------------------------------------------------------------------------------

