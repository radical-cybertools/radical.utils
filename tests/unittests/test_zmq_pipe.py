#!/usr/bin/env python3

__author__    = 'Radical.Utils Development Team'
__copyright__ = 'Copyright 2021, RADICAL@Rutgers'
__license__   = 'MIT'


import time
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_zmq_pipe():

    pipe_1 = ru.zmq.Pipe(ru.zmq.Pipe.PUSH)
    pipe_2 = ru.zmq.Pipe(ru.zmq.Pipe.PULL, pipe_1.url)
    pipe_3 = ru.zmq.Pipe(ru.zmq.Pipe.PULL, pipe_1.url)

    time.sleep(0.01)

    for i in range(1000):
        pipe_1.put('foo %d' % i)

    result_2 = list()
    result_3 = list()

    for i in range(400):
        result_2.append(pipe_2.get())
        result_3.append(pipe_3.get())

    for i in range(100):
        result_2.append(pipe_2.get_nowait(timeout=0.01))
        result_3.append(pipe_3.get_nowait(timeout=0.01))

    assert len(result_2) == 500
    assert len(result_3) == 500

    test_2 = result_2.append(pipe_2.get_nowait(timeout=0.01))
    test_3 = result_3.append(pipe_3.get_nowait(timeout=0.01))

    assert test_2 is None
    assert test_3 is None


# ------------------------------------------------------------------------------
#
def test_zmq_pipe_cb():

    pipe_1  = ru.zmq.Pipe(ru.zmq.Pipe.PUSH)
    pipe_2  = ru.zmq.Pipe(ru.zmq.Pipe.PULL, pipe_1.url)
    results = list()

    time.sleep(0.01)

    def cb(msg):
        results.append(msg)

    pipe_2.register_cb(cb)

    n = 1000
    for i in range(n):
        pipe_1.put('foo %d' % i)

    time.sleep(0.01)

    assert len(results) == n, results


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_pipe()
    test_zmq_pipe_cb()


# ------------------------------------------------------------------------------

