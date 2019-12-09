#!/usr/bin/env python

__author__    = 'Radical.Utils Development Team'
__copyright__ = 'Copyright 2019, RADICAL@Rutgers'
__license__   = 'MIT'


import time
import threading     as mt

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_zmq_queue():
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

    c_a = 100
    c_b = 200

    cfg = ru.Config(cfg={'uid'      : 'test_queue',
                         'channel'  : 'test',
                         'kind'     : 'queue',
                         'log_level': 'error',
                         'path'     : '/tmp/',
                         'sid'      : 'test_sid',
                         'bulk_size': 0,
                         'stall_hwm': 1,
                        })

    b = ru.zmq.Queue(cfg)
    b.start()

    assert(b.addr_in  != b.addr_out)
    assert(b.addr_in  == b.addr_put)
    assert(b.addr_out == b.addr_get)

    A = ru.zmq.Putter(channel=cfg['channel'], url=str(b.addr_put))
    B = ru.zmq.Putter(channel=cfg['channel'], url=str(b.addr_put))

    C = ru.zmq.Getter(channel=cfg['channel'], url=str(b.addr_get))
    D = ru.zmq.Getter(channel=cfg['channel'], url=str(b.addr_get))

    data = dict()

    def work_put(putter, uid, n, delay):

        data[uid] = list()
        idx   = 0
        while idx < n:
            time.sleep(delay)
            putter.put({'src' : uid,
                        'idx' : idx})
            idx += 1
            data[uid].append(uid)

        # send EOF
        putter.put({'src' : uid,
                    'idx' : None})


    def work_get(getter, uid):

        data[uid] = list()
        done      = False
        while not done:
            msgs = getter.get()
            for msg in msgs:
                msg = ru.as_string(msg)
                if msg['idx'] is None:
                    done = True
                    break
                data[uid].append(msg['src'])

    t_a = mt.Thread(target=work_put, args=[A, 'A', c_a, 0.10])
    t_b = mt.Thread(target=work_put, args=[B, 'B', c_b, 0.05])
    t_c = mt.Thread(target=work_get, args=[C, 'C'])
    t_d = mt.Thread(target=work_get, args=[D, 'D'])

    t_a.start()
    t_b.start()
    t_c.start()
    t_d.start()

    t_a.join()
    t_b.join()
    t_c.join()
    t_d.join()

    b.stop()

  # uids = list(data.keys())
  # for x in uids:
  #     for y in uids:
  #         print('%s: %s: %d' % (x, y, data[x].count(y)))
  #
  # print(len(data['A']))
  # print(len(data['B']))
  # print(len(data['C']))
  # print(len(data['D']))


    assert(data['A'].count('A') == c_a)
    assert(data['B'].count('B') == c_b)
    assert(len(data['A'])       == c_a)
    assert(len(data['B'])       == c_b)

    assert(data['C'].count('A') + data['C'].count('B') +
           data['D'].count('A') + data['D'].count('B') == c_a + c_b)

    avg = (c_a + c_b) / 2
    assert(avg - 5 < data['C'].count('A') + data['C'].count('B') < avg + 5)
    assert(avg - 5 < data['D'].count('A') + data['D'].count('B') < avg + 5)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_queue()


# ------------------------------------------------------------------------------

