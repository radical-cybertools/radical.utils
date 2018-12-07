#!/usr/bin/env python

import os
import sys
import time
import socket
import threading as mt

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_poll():

    sp = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    parent = os.getpid()
    child  = os.fork()

    if not child:

      # print 'child : %s' % os.getpid()
    
        sp[0].close()
        sp[1].send('foo')
        time.sleep(1)
        sp[1].send('bar')
        time.sleep(2)
        sp[1].close()
      # print 'child : done'
        sys.exit()
    
    else:

        sp[1].close()

        poller = ru.Poller()
        poller.register(sp[0], ru.POLLERR | ru.POLLIN | ru.POLLHUP)

      # print 'Parent: %s' % parent
      # print 'Child : %s' % child

        abort = False
        msgs  = list()
        while not abort:

            events = poller.poll(1.0)

            for fd,event in events:
                
                if event & ru.POLLIN:
                    msg = sp[0].recv(200)
                    assert(msg in ['foo', 'bar', ''])
                    msgs.append(msg)

                if  event & ru.POLLERR or event & ru.POLLHUP:
                    abort = True
                    assert(msgs[0] == 'foo')
                    assert(msgs[1] == 'bar')
                    assert(msgs[2] == '')
                    break

        os.waitpid(child, 0)
      # print 'child : %s gone' % child


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_poll()


# ------------------------------------------------------------------------------


