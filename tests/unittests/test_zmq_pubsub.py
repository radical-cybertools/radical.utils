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
    data['A'] = list()
    data['B'] = list()
    data['C'] = list()
    data['D'] = list()

    def cb(uid, topic, msg):
        if msg['idx'] is None:
            return False
        data[uid].append(msg['src'])

    cb_C = lambda t,m: cb('C', t, m)
    cb_D = lambda t,m: cb('D', t, m)

    C = ru.zmq.Subscriber(channel=cfg['channel'], url=str(b.addr_sub),
                          topic='topic', cb=cb_C)
    D = ru.zmq.Subscriber(channel=cfg['channel'], url=str(b.addr_sub),
                          topic='topic', cb=cb_D)


    # --------------------------------------------------------------------------
    def work_pub(uid, n, delay):

        pub = ru.zmq.Publisher(channel=cfg['channel'], url=str(b.addr_pub))

        data[uid] = list()
        idx   = 0
        while idx < n:
            time.sleep(delay)
            pub.put('topic', {'src': uid,
                              'idx': idx})
            idx += 1
            data[uid].append(uid)

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

    assert(data['A'].count('A') == c_a)
    assert(data['B'].count('B') == c_b)
    assert(len(data['A'])       == c_a)
    assert(len(data['B'])       == c_b)

    assert(data['C'].count('A') + data['C'].count('B') +
           data['D'].count('A') + data['D'].count('B') == 2 * (c_a + c_b))


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_pubsub()


# ------------------------------------------------------------------------------

