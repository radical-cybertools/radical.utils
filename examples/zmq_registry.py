#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    r = ru.zmq.Registry()

    try:
        r.start()

        assert r.addr
        c = ru.zmq.RegistryClient(url=r.addr)
        c.put('foo.bar.buz', {'biz': 42})
        print(c.get('foo'))
        print(c.get('foo.bar.buz.biz'))
        print(c.get('foo.bar.buz.biz.boz'))
        print('ok')

  # except:
  #     print('oops')

    finally:
        r.stop()
        r.wait()


# ------------------------------------------------------------------------------
#
