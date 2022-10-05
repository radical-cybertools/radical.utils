#!/usr/bin/env python3

__author__    = 'Radical.Utils Development Team'
__copyright__ = 'Copyright 2021, RADICAL@Rutgers'
__license__   = 'MIT'


import time
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_zmq_pipe():

    pipe_1 = ru.zmq.Pipe(ru.zmq.MODE_PUSH)

    url = pipe_1.url

    pipe_2 = ru.zmq.Pipe(ru.zmq.MODE_PULL, url)
    pipe_3 = ru.zmq.Pipe(ru.zmq.MODE_PULL, url)

    # let ZMQ settle
    time.sleep(1)

    for i in range(1000):
        pipe_1.put('foo %d' % i)

    result_2 = list()
    result_3 = list()

    for i in range(400):
        result_2.append(pipe_2.get())
        result_3.append(pipe_3.get())

    for i in range(100):
        result_2.append(pipe_2.get_nowait(timeout=1.0))
        result_3.append(pipe_3.get_nowait(timeout=1.0))

    assert len(result_2) == 500
    assert len(result_3) == 500

    test_2 = result_2.append(pipe_2.get_nowait(timeout=1.0))
    test_3 = result_3.append(pipe_3.get_nowait(timeout=1.0))

    assert test_2 is None
    assert test_3 is None


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_pipe()


# ------------------------------------------------------------------------------

