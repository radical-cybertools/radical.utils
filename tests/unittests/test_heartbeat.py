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
            assert now >= t0 + dur, '%.1f > %.1f + %.1f' % (now, t0, dur)

            cnt_tgt = dur / ival
            cnt_chk = cb(test=True)
            assert cnt_tgt * 0.8 < cnt_chk < cnt_tgt * 1.2, [cnt_tgt, cnt_chk]

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
        assert p.is_alive()

        # but it should have a zero exit value after 2 more seconds
        time.sleep(2)
        assert not p.is_alive()
        assert p.exitcode

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

        hb = ru.Heartbeat('test', timeout=0.4, interval=0.1)
        t0 = time.time()

        hb.start()

        try:
            while True:
                if   time.time() < t0 + 0.3: hb.beat()
                elif time.time() < t0 + 0.6: hb.beat()
                else: break
                time.sleep(0.1)

            while True:
                time.sleep(0.1)

        finally:
            if time.time() > t0 + 0.35:
                sys.exit(-1)
    # --------------------------------------------------------------------------

    p = None
    try:
        p = mp.Process(target=proc)
        p.start()

        # proc should still be alive after 2 seconds
        time.sleep(0.4)
        assert p.is_alive()

        # but it should have a zero exit value after 2 more seconds
        time.sleep(1.2)
        assert not p.is_alive()
        assert p.exitcode

    finally:
        try   : os.kill(p.pid, 9)
        except: pass


# ------------------------------------------------------------------------------
#
def test_hb_pwatch_py():

    queue = mp.Queue()

    # pid check
    def is_alive(pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    # watcher process
    def _watcher(action):

        pwatcher = ru.PWatcher(action=action)

        # create two sleep processes to watch / handle
        proc_1 = mp.Process(target=time.sleep, args=(0.1,))
        proc_2 = mp.Process(target=time.sleep, args=(0.4,))

        proc_1.daemon = True
        proc_2.daemon = True

        proc_1.start()
        proc_2.start()

        pwatcher.watch(proc_1.pid)
        pwatcher.watch(proc_2.pid)

        queue.put([os.getpid(), proc_1.pid, proc_2.pid])

        # sleep for 1 seconds
        start = time.time()
        proc_1.join(timeout=1)

        remaining = max(0, 1 - (time.time() - start))
        proc_2.join(timeout=remaining)

        remaining = max(0, 1 - (time.time() - start))
        time.sleep(remaining)


    # NOTE: we cannot use `test_proc.daemon = True` here, as the daemon flag
    #       damon procs cannot spawn children in Python :-/

    # --------------------------------------------------------------------------
    # test mode `nothing`
    test_proc = mp.Process(target=_watcher, args=[ru.PWatcher.NOTHING])
    test_proc.start()

    pids = queue.get()

    # after 0.2 seconds, the watcher and second sleep should still be alive
    time.sleep(0.3)
    assert     is_alive(pids[0])
    assert not is_alive(pids[1])
    assert     is_alive(pids[2])

    # after 0.5 seconds, only the watcher should still be alive
    time.sleep(0.6)
    assert     is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])

    # after 1.1 seconds, the watcher should have exited
    time.sleep(1.2)
    test_proc.join(timeout=0.0)
    assert not is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])

    # --------------------------------------------------------------------------
    # test mode `suicide`
    test_proc = mp.Process(target=_watcher, args=[ru.PWatcher.SUICIDE])
    test_proc.start()

    pids = queue.get()

    # after 0.2 seconds, only second sleep should still be alive
    time.sleep(0.4)
    test_proc.join(timeout=0.1)
    assert not is_alive(pids[0])
    assert not is_alive(pids[1])
    assert     is_alive(pids[2])

    # after 0.5 seconds, none of the processes should be alive
    time.sleep(0.5)
    assert not is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])


    # --------------------------------------------------------------------------
    # test mode `killall`
    test_proc = mp.Process(target=_watcher, args=[ru.PWatcher.KILLALL])
    test_proc.start()

    pids = queue.get()

    # after 0.2 seconds, only second sleep should still be alive
    time.sleep(0.4)
    test_proc.join(timeout=0.1)
    assert     is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])

    # after 0.5 seconds, none of the processes should be alive
    time.sleep(0.5)
    test_proc.join(timeout=0.1)
    assert not is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])


    # --------------------------------------------------------------------------
    # test mode `rampage`
    test_proc = mp.Process(target=_watcher, args=[ru.PWatcher.RAMPAGE])
    test_proc.start()

    pids = queue.get()

    # after 0.2 seconds, the first sleep dies and no process should be alive
    time.sleep(0.3)
    test_proc.join(timeout=0.1)
    assert     is_alive(pids[0])
    assert not is_alive(pids[1])
    assert not is_alive(pids[2])


# ------------------------------------------------------------------------------
#
def test_hb_pwatch_sh():

    pwd    = os.path.dirname(__file__)
    script = '%s/../bin/test_pwatch.sh' % pwd

    out, err, ret = ru.sh_callout('%s -h' % script)

    assert ret == 0, [out, err, ret]


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_hb_default()
    test_hb_uid()
    test_hb_pwatch_py()
    test_hb_pwatch_sh()


# ------------------------------------------------------------------------------

