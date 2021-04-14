#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    c = ru.zmq.Client(url='tcp://localhost:12345')
    print(1, c.request(cmd='hello', arg='world'))

    try: print(2, c.request(cmd='foo', arg='bar'))
    except Exception as e: print(str(e))

    try: print(3, c.request(cmd='cmd', arg='buz'))
    except Exception as e: print(str(e))

    print(4, c.request(cmd='cmd',   arg='exit'))


# ------------------------------------------------------------------------------

