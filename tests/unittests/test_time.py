from radical.utils.custom_time import Time

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

def mysleep(amount, t_obj):
    
    print(mt.currentThread().getName(),': Start time ', t_obj.time())
    t_obj.sleep(amount)
    print(mt.currentThread().getName(),': Final time ', t_obj.time())

if __name__ == '__main__':

    test_time()
    test_tick_speed()
    test_advance()

    # This is a small test to show that the execution the sleep method of this
    # class is thread safe.

    tic = time.time()
    t  = Time(speed=100)
    t0 = t.time()
    sleeping_thread2 = mt.Thread(target=mysleep, name='sleep2', args=(5,t,))
    sleeping_thread1 = mt.Thread(target=mysleep, name='sleep1', args=(2,t,))
    sleeping_thread1.start()
    sleeping_thread2.start()
    time.sleep(10)
    sleeping_thread1.join()
    sleeping_thread2.join()
    t1 = t.time()
    t.stop()
    print(t1,t0)
