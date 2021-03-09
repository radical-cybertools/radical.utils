#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    c = ru.zmq.Client(url='tcp://localhost:12345')
    print(1, c.request(req='hello', arg='world'))

    try: print(2, c.request(req='foo', arg='bar'))
    except Exception as e: print(str(e))

    try: print(3, c.request(req='cmd', arg='buz'))
    except Exception as e: print(str(e))

    print(4, c.request(req='cmd',   arg='exit'))


# ------------------------------------------------------------------------------

