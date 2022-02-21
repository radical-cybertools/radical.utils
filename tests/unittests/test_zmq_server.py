#!/usr/bin/env python3

# pylint: disable=no-value-for-parameter, protected-access

from typing   import Any, List, Dict
from unittest import mock, TestCase

import radical.utils as ru

from radical.utils.zmq.server import zmq
from radical.utils.zmq        import Server


# --------------------------------------------------------------------------
#
class MyZMQServer(ru.zmq.Server):

    def __init__(self):

        ru.zmq.Server.__init__(self)

        self.register_request('test_0', self._test_0)
        self.register_request('test_1', self._test_1)
        self.register_request('test_2', self._test_2)
        self.register_request('test_3', self._test_3)
        self.register_request('test_4', self._test_4)


    def _test_0(self) -> str:
        return 'default'


    def _test_1(self, foo: Any = None) -> Any:
        return foo


    def _test_2(self, foo: Any,
                      bar: Any) -> List[Any]:
        return [foo, bar]


    def _test_3(self, foo: Any,
                      bar: Any = 'default') -> Dict[str, Any]:
        return {'foo': foo,
                'bar': bar}


    def _test_4(self, *args, **kwargs) -> List[Any]:
        return list(args) + list(kwargs.values())


# ------------------------------------------------------------------------------
#
class TestZMQServer(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_init(self, mocked_profiler, mocked_logger):

        s = Server()
        self.assertTrue  (s.uid.startswith('server'))
        self.assertIsNone(s.addr)
        self.assertEqual (s._url, 'tcp://*:10000-11000')

        self.assertFalse(s._up.is_set())
        self.assertFalse(s._term.is_set())

        uid = 'test.server'
        s = Server(uid=uid)
        self.assertEqual(s.uid, uid)

        with self.assertRaises(AssertionError):
            # port(s) not set
            Server(url='tcp://*')

        with self.assertRaises(RuntimeError):
            # port(s) set incorrectly
            Server(url='tcp://*:10000-11000-22000')

        # default callbacks
        self.assertIn('echo', s._cbs)
        self.assertIn('fail', s._cbs)
        self.assertEqual(s._cbs['echo']('test_echo'), 'test_echo')
        with self.assertRaises(RuntimeError):
            s._cbs['fail'](None)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Server, '__init__', return_value=None)
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_exec_output(self, mocked_profiler, mocked_logger, mocked_init):

        s = Server()

        response_msg = 'response-00'
        output = s._success(res=response_msg)
        self.assertIsInstance(output, dict)
        self.assertIsNone(output['err'])
        self.assertIsNone(output['exc'])
        self.assertEqual (output['res'], response_msg)

        error_msg, exception_msg = 'error-00', 'exception-00'
        output = s._error(err=error_msg, exc=exception_msg)
        self.assertEqual (output['err'], error_msg)
        self.assertEqual (output['exc'], exception_msg)
        self.assertIsNone(output['res'])

        output = s._error()
        self.assertEqual(output['err'], 'invalid request')

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_start(self, mocked_profiler, mocked_logger):

        s = Server(url='tcp://*:12345')
        s.start()
        self.assertTrue(s.addr.endswith('12345'))

        self.assertTrue(s._up.is_set())

        with self.assertRaises(RuntimeError):
            # `start()` can be called only once
            s.start()

        s2 = Server(url='tcp://*:12345-')
        s2.start()
        # logged warning about port already in use
        self.assertTrue(s2._log.warn.called)
        self.assertTrue(s2.addr.endswith('12346'))

        s2.stop()
        s2.wait()

        s.stop()
        s.wait()

        self.assertTrue(s._term.is_set())

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.zmq.Context')
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_zmq(self, mocked_profiler, mocked_logger, mocked_zmq_ctx):

        s = Server()
        mocked_zmq_ctx().socket().bind = mock.Mock(
            side_effect=zmq.error.ZMQError(msg='random ZMQ error'))

        with self.assertRaises(zmq.error.ZMQError):
            s._work()


    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_server_class(self, mocked_profiler, mocked_logger):

        s = MyZMQServer()

        try:
            s.start()
            self.assertIsNotNone(s.addr)

            c = ru.zmq.Client(url=s.addr)
            self.assertEqual(c.url, s.addr)

            with self.assertRaisesRegex(RuntimeError, 'no command'):
                c.request('')

            with self.assertRaisesRegex(RuntimeError, 'command .* unknown'):
                c.request('no_registered_cmd')

            with self.assertRaisesRegex(RuntimeError,
                    '.* _test_0.* takes 1 positional argument'):
                c.request('test_0', None)

            ret = c.request('test_0')
            self.assertIsInstance(ret, str)
            self.assertEqual(ret, 'default')

            ret = c.request('test_1', 'foo')
            self.assertIsInstance(ret, str)
            self.assertEqual(ret, 'foo')

            ret = c.request('test_1', ['foo', 'bar'])
            self.assertIsInstance(ret, list)
            self.assertEqual(ret, ['foo', 'bar'])

            ret = c.request('test_1')
            self.assertEqual(ret, None)

            ret = c.request('test_2', 'foo', 'bar')
            self.assertIsInstance(ret, list)
            self.assertEqual(ret, ['foo', 'bar'])

            ret = c.request('test_3', 'foo')
            self.assertIsInstance(ret, dict)
            self.assertEqual(ret, {'foo': 'foo', 'bar': 'default'})

            ret = c.request('test_3', foo='foo', bar='bar')
            self.assertIsInstance(ret, dict)
            self.assertEqual(ret, {'foo': 'foo', 'bar': 'bar'})

            ret = c.request('test_3', 'foo', bar='bar')
            self.assertIsInstance(ret, dict)
            self.assertEqual(ret, {'foo': 'foo', 'bar': 'bar'})

            ret = c.request('test_3', 'foo', 'bar')
            self.assertIsInstance(ret, dict)
            self.assertEqual(ret, {'foo': 'foo', 'bar': 'bar'})

            ret = c.request('test_4', 'foo', 'bar')
            self.assertIsInstance(ret, list)
            self.assertEqual(ret, ['foo', 'bar'])

            ret = c.request('test_4', 'foo', ['bar'])
            self.assertIsInstance(ret, list)
            self.assertEqual(ret, ['foo', ['bar']])

            c.close()

        finally:
            s.stop()
            s.wait()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestZMQServer()
    tc.test_init()
    tc.test_exec_output()
    tc.test_start()
    tc.test_zmq()
    tc.test_server_class()

# ------------------------------------------------------------------------------

