#!/usr/bin/env python3

import time

import threading     as mt

import radical.utils as ru

CHANNEL = 'channel'
TOPIC   = 'test'


# ------------------------------------------------------------------------------
#
def pub(uid, url, topic, n):

    pub = ru.zmq.Publisher(topic, url)
    for i in range(n):
        pub.put(topic, ru.as_bytes('%s: message %d' % (uid, i)))
        print('%s: message %d' % (uid, i))
        time.sleep(0.1)

    pub.put(topic, ru.as_bytes('%s: STOP' % uid))

    print('%s: done' % uid)


# ------------------------------------------------------------------------------
#
def sub(uid, url, topic):

    cont = True
    sub  = ru.zmq.Subscriber(topic, url)
    sub.subscribe(topic)

    while cont:
        topic, msg = sub.get()
        print('%s: %s' % (uid, ru.as_string(msg)))

        if 'STOP' in ru.as_string(msg):
            cont = False

    print('%s: done' % uid)


# ------------------------------------------------------------------------------
#
def main():

    bridge = ru.zmq.PubSub(CHANNEL)
    bridge.start()
    time.sleep(1)

    threads = list()


    # start some subscribers and publishers
    for i in range(3):
        t = mt.Thread(target=sub, args=('sub.%d' % i, bridge.addr_sub, TOPIC))
        t.daemon = True
        t.start()
        threads.append(t)

    for i in range(4):
        t = mt.Thread(target=pub, args=('pub.%d' % i, bridge.addr_pub, TOPIC, 5))
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

