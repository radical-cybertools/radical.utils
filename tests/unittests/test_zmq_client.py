#!/usr/bin/env python3
# pylint: disable=no-value-for-parameter

import msgpack

from unittest import mock, TestCase

from radical.utils.zmq import Client, Server


# ------------------------------------------------------------------------------
#
class TestZMQClient(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_client(self, mocked_profiler, mocked_logger):

        with self.assertRaises(ValueError):
            # neither `server` nor `url` is set
            Client()

        s = Server()
        s.start()

        c = None
        try:

            c = Client(url=s.addr)
            self.assertEqual(c.url, s.addr)

            echo_str = 'test_echo'
            self.assertEqual(c.request(cmd='echo', arg=echo_str), echo_str)

            with self.assertRaises(RuntimeError):
                # no command in request
                c.request(cmd='')

            with self.assertRaises(RuntimeError):
                # command [no_registered_cmd] unknown
                c.request(cmd='no_registered_cmd')

        finally:

            if c:
                c.close()

        s.stop()
        s.wait()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestZMQClient()
    tc.test_client()

# ------------------------------------------------------------------------------

