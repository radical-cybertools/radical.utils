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
def test_hb_default():
    '''
    spawn a process with HB and expect it to live for `tout` seconds.  If it
    dies before, it will return a non-zero exit code.  If it stays alive longer
    than 6 seconds, we consider this an error.
    '''

    dur  = 3.0
    ival = 0.1
    tout = 0.3

    # --------------------------------------------------------------------------
    def get_counter():
        cnt = 0

        def count(test=False):
            nonlocal cnt
            if not test:
                cnt += 1
            return cnt
        return count

    # --------------------------------------------------------------------------
    def proc():

        cb = get_counter()

        def term_cb(uid):

            now = time.time()
            assert(now >= t0 + dur), '%.1f > %.1f + %.1f' % (now, t0, dur)

            cnt_tgt = dur / ival
            cnt_chk = cb(test=True)
            assert(cnt_tgt * 0.8 < cnt_chk < cnt_tgt * 1.2), [cnt_tgt, cnt_chk]

        hb = ru.Heartbeat('foo', timeout=tout, interval=ival,
                                 beat_cb=cb, term_cb=term_cb)
        t0 = time.time()

        hb.start()

        while time.time() < t0 + dur:
            hb.beat()

        while True:
            time.sleep(0.1)
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
#
def test_hb_uid():
    '''
    Use two different uids for heartbeats, and let only one
    time out.
    '''

    # --------------------------------------------------------------------------
    def proc():

        hb = ru.Heartbeat('test', timeout=0.1, interval=0.01)
        t0 = time.time()

        hb.start()

        try:
            while True:
                if time.time() < t0 + 3: hb.beat('short')
                if time.time() < t0 + 5: hb.beat('long')
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

    test_hb_default()
    test_hb_uid()


# ------------------------------------------------------------------------------

