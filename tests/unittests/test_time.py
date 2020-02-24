

import time

import radical.utils as ru

from concurrent.futures import ThreadPoolExecutor


# ------------------------------------------------------------------------------
#
def test_time():

    t  = ru.Time()
    t0 = t.time()
    time.sleep(1)
    t1 = t.time()
    t.stop()

    assert(0.99 < (t1 - t0) < 1.01), [t0, t1, t1 - t0]


# ------------------------------------------------------------------------------
#
def test_tick_speed():

    t  = ru.Time(tick=0.01, speed=10.0)
    t0 = t.time()
    time.sleep(0.5)
    t1 = t.time()
    t.stop()

    assert(0.0 <=       t0  <= 0.1),  t0
    assert(4.9 <  (t1 - t0) <  5.1), [t0, t1, t1 - t0]


# ------------------------------------------------------------------------------
#
def test_advance():

    t  = ru.Time(tick=0.01, speed=10.0)
    t0 = t.time()
    time.sleep(0.5)
    t.advance(100)
    t1 = t.time()
    t.stop()

    assert(000.0 <=       t0 <=   0.1),  t0
    assert(104.9 < (t1 - t0) <  105.1), [t0, t1, t1 - t0]


# ------------------------------------------------------------------------------
#
def test_sleep():

    t  = ru.Time(tick=0.01, speed=10.0)
    t0 = t.time()
    t.sleep(5)
    t1 = t.time()
    t.stop()

    assert(4.9 <= (t1 - t0) <= 5.1), [t0, t1, t1 - t0]


# ------------------------------------------------------------------------------
#
def test_multithread_sleep():
    '''
    test that the sleep method of this class is thread safe.
    '''

    def t_sleep(t_obj, amount):
        tic = t_obj.time()
        t_obj.sleep(amount)
        toc = t_obj.time()
        return tic, toc

    tpe = ThreadPoolExecutor(max_workers=2)
    t   = ru.Time(speed=10)

    sleeping_thread1 = tpe.submit(t_sleep, t, 3)
    t.sleep(1)
    sleeping_thread2 = tpe.submit(t_sleep, t, 5)

    tic1, toc1 = sleeping_thread1.result()
    tic2, toc2 = sleeping_thread2.result()

    assert(0.0 <= tic1 <=  0.1), tic1
    assert(2.9 <= toc1 <=  3.1), toc1
    assert(0.9 <= tic2 <=  1.1), tic2
    assert(5.9 <= toc2 <=  6.1), toc2


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_time()
    test_tick_speed()
    test_advance()
    test_sleep()
    test_multithread_sleep()


# ------------------------------------------------------------------------------
