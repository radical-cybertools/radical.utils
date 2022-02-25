#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    s = ru.zmq.Server(url='tcp://*:12345')

    # --------------------------------------------------------------------------
    def hello(arg: str) -> str:
        return 'hello ' + arg


    # --------------------------------------------------------------------------
    def cmd(arg):
        if arg == 'exit':
            s.stop()
            return 'ok'
        else:
            raise ValueError("we don't like no [%s]" % arg)

    s.register_request('hello', hello)
    s.register_request('cmd',   cmd)
    s.start()
    s.wait()


# ------------------------------------------------------------------------------

