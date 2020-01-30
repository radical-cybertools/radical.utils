#!/usr/bin/env python

__author__    = 'Radical.Utils Development Team'
__copyright__ = 'Copyright 2019, RADICAL@Rutgers'
__license__   = 'MIT'


import time
import threading     as mt

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_zmq_pubsub():
    '''
    create a bridge, 2 producers (A, B) and 2 consumers (C, D).  Send with the
    following rates for 10 seconds:

      A: 10/s
      B: 20/s

    Ensure that
      - the ratios of sent / received messages reflects the rates
      - the local order of messages is preserved
      - messages are received exactly once (no messages get lost / duplicated)
    '''

    c_a = 200
    c_b = 400

    cfg = ru.Config(cfg={'uid'      : 'test_pubsub',
                         'channel'  : 'test',
                         'kind'     : 'pubsub',
                         'log_level': 'error',
                         'path'     : '/tmp/',
                         'sid'      : 'test_sid',
                         'bulk_size': 0,
                         'stall_hwm': 1,
                        })

    b = ru.zmq.PubSub(cfg)
    b.start()

    assert(b.addr_in  != b.addr_out)
    assert(b.addr_in  == b.addr_pub)
    assert(b.addr_out == b.addr_sub)

    data = dict()
    for i in 'ABCD':
        data[i] = dict()
        for j in 'AB':
            data[i][j] = 0

    def cb(uid, topic, msg):
        if msg['idx'] is None:
            return False
        data[uid][msg['src']] += 1

    cb_C = lambda t,m: cb('C', t, m)
    cb_D = lambda t,m: cb('D', t, m)

    ru.zmq.Subscriber(channel=cfg['channel'], url=str(b.addr_sub),
                      topic='topic', cb=cb_C)
    ru.zmq.Subscriber(channel=cfg['channel'], url=str(b.addr_sub),
                      topic='topic', cb=cb_D)
    time.sleep(0.1)

    # --------------------------------------------------------------------------
    def work_pub(uid, n, delay):

        pub = ru.zmq.Publisher(channel=cfg['channel'], url=str(b.addr_pub))
        idx = 0
        while idx < n:
            time.sleep(delay)
            pub.put('topic', {'src': uid,
                              'idx': idx})
            idx += 1
            data[uid][uid] += 1

        # send EOF
        pub.put('topic', {'src': uid,
                          'idx': None})
    # --------------------------------------------------------------------------


    t_a = mt.Thread(target=work_pub, args=['A', c_a, 0.005])
    t_b = mt.Thread(target=work_pub, args=['B', c_b, 0.005])

    t_a.start()
    t_b.start()

    t_a.join()
    t_b.join()

    b.stop()
    time.sleep(0.1)

    assert(data['A']['A'] == c_a)
    assert(data['B']['B'] == c_b)

    assert(data['C']['A'] + data['C']['B'] +
           data['D']['A'] + data['D']['B'] == 2 * (c_a + c_b))


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_pubsub()


# ------------------------------------------------------------------------------

