#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    c = ru.zmq.Client(url='tcp://localhost:12345')
    print(c.request(req='hello', arg='world'))
    try:
        print(c.request(req='cmd',   arg='foo'))
    except Exception as e:
        print(str(e))
    print(c.request(req='cmd',   arg='exit'))


# ------------------------------------------------------------------------------

