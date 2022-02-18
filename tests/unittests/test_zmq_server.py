#!/usr/bin/env python3

# pylint: disable=no-value-for-parameter

from unittest import mock, TestCase

from radical.utils.zmq.server import zmq
from radical.utils.zmq import Server


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


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestZMQServer()
    tc.test_init()
    tc.test_exec_output()
    tc.test_start()
    tc.test_zmq()

# ------------------------------------------------------------------------------

