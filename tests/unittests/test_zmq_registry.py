#!/usr/bin/env python3

# pylint: no-value-for-parameter,unused-argument

import radical.utils as ru

from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch('radical.utils.zmq.server.Profiler')
def test_zmq_registry(mocked_prof):

    r = ru.zmq.Registry()
    r.start()

    try:
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

        c.close()
    except:
        pass

    r.stop()
    r.wait()


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_zmq_registry()


# ------------------------------------------------------------------------------

