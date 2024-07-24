#!/usr/bin/env python3

import time

import threading     as mt

import radical.utils as ru

CHANNEL = 'test'
TOPIC   = 'test'


# ------------------------------------------------------------------------------
#
def put(channel, uid, url, n):

    put = ru.zmq.Putter(channel, url)
    for i in range(n):
        put.put(ru.as_bytes('%s: message %d' % (uid, i)))
        print('%s: message %d' % (uid, i))
        time.sleep(0.1)

    put.put(ru.as_bytes('%s: STOP' % uid))

    print('%s: done' % uid)


# ------------------------------------------------------------------------------
#
def get(channel, uid, url):

    cont = True
    get  = ru.zmq.Getter(channel, url)

    while cont:

        msgs = get.get()
        if not msgs:
            continue

        print('%s: %s' % (uid, ru.as_string(msgs)))

        for msg in msgs:
            if 'STOP' in ru.as_string(msg):
                cont = False

    print('%s: done' % uid)


# ------------------------------------------------------------------------------
#
def main():

    bridge = ru.zmq.Queue(CHANNEL)
    bridge.start()

    threads = list()

    # start some putters and getters
    # NOTE: start more putters than getters for clean termination
    for i in range(4):
        t = mt.Thread(target=put, args=(CHANNEL, 'put.%d' % i, bridge.addr_put, 5))
        t.daemon = True
        t.start()
        threads.append(t)

    for i in range(3):
        t = mt.Thread(target=get, args=(CHANNEL, 'get.%d' % i, bridge.addr_get))
        t.daemon = True
        t.start()
        threads.append(t)

    # wait for completion
    for t in threads:
        t.join()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    main()


# ------------------------------------------------------------------------------

