#!/usr/bin/env python3

import time
import radical.utils as ru

term = False


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    s = ru.zmq.Server(url='tcp://*:12345')


    # ------------------------------------------------------------------------------
    def hello(arg):
        return 'hello %s' % arg
    # ------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------
    def cmd(arg):
        if arg == 'exit':
            global term
            s.stop()
            return 'ok'
        else:
            raise ValueError('unknown command [%s]' % arg)
    # ------------------------------------------------------------------------------


    s.register_request('hello', hello)
    s.register_request('cmd',   cmd)
    s.start()
    s.wait()


# ------------------------------------------------------------------------------

