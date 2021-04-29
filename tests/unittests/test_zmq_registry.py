#!/usr/bin/env python3

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_zmq_registry():

    r = ru.zmq.Registry()

    try:
        r.start()

        c = ru.zmq.RegistryClient(url=r.addr)
        c.put('foo.bar.buz', {'biz': 42})
        assert(c.get('foo') == {'bar': {'buz': {'biz': 42}}})
        assert(c.get('foo.bar.buz.biz') == 42)
        assert(c.get('foo.bar.buz.biz.boz') is None)
        assert(c.get('foo') == {'bar': {'buz': {'biz': 42}}})

        assert(c['foo.bar.buz.biz'] == 42)
        assert(c['foo']['bar']['buz']['biz'] == 42)
        assert(c['foo.bar.buz.biz.boz'] is None)

        assert('foo' in c)
        assert(c.keys() == ['foo'])
        del(c['foo'])
        assert('foo' not in c)
        assert(c.keys() == [])

    finally:
        r.stop()
        r.wait()


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_registry()


# ------------------------------------------------------------------------------

