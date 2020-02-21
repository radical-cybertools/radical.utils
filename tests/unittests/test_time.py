from radical.utils.custom_time import Time
import time
from concurrent.futures import ThreadPoolExecutor
import threading as mt


def test_time():
    t  = Time()
    t0 = t.time()
    time.sleep(3)
    t1 = t.time()
    t.stop()
    assert(2.99 < (t1 - t0) < 3.01), [t0, t1, t1 - t0]


def test_tick_speed():
    t  = Time(tick=0.1, speed=3.0)
    t0 = t.time()
    time.sleep(3)
    t1 = t.time()
    t.stop()
    assert(0.00 <= t0 <= 0.01), t0
    assert(8.69 <  (t1 - t0) < 9.01), [t0, t1]


def test_advance():
    t  = Time(tick=0.1, speed=3.0)
    t0 = t.time()
    time.sleep(3)
    t.advance(60)
    t1 = t.time()
    t.stop()
    assert(0.00 <= t0 <= 0.01), t0
    assert(68.69 <  (t1 - t0) < 69.01), [t0, t1]


def test_sleep():
    t  = Time(tick=0.1, speed=3.0)
    t0 = t.time()
    t.sleep(3)
    t1 = t.time()
    t.stop()
    print(t1, t0)
    assert(2.75 <= t1 - t0 <= 3.01), t0


def test_multithread_sleep():

    def t_sleep(t_obj, amount):
        tic = t_obj.time()
        print(mt.current_thread().name, ': Start time ', t_obj.time())
        t_obj.sleep(amount)
        toc = t_obj.time()
        print(mt.current_thread().name, ': Final time ', t_obj.time())
        return tic, toc

    # This is a small test to show that the execution the sleep method of this
    # class is thread safe.
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='sleep')
    t  = Time(speed=100)
    sleeping_thread1 = executor.submit(t_sleep, t, 2)
    t.sleep(1)
    sleeping_thread2 = executor.submit(t_sleep, t, 5)
    tic1, toc1 = sleeping_thread1.result()
    tic2, toc2 = sleeping_thread2.result()
    assert(tic1 == 0)
    assert(2 <= toc1 <= 2.5)
    assert(1 <= tic2 <= 1.1)
    assert(5.6 <= toc2 <= 6.5)


if __name__ == '__main__':

    test_time()
    test_tick_speed()
    test_advance()
    test_sleep()
    test_multithread_sleep()
