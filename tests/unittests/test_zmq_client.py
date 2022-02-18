#!/usr/bin/env python3

# pylint: disable=no-value-for-parameter

import os

from unittest import mock, TestCase

from radical.utils.json_io import write_json

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

        write_json({'addr': s.addr}, 'server_local.cfg')
        c = Client(server='server_local')
        self.assertEqual(c.url, s.addr)
        c.close()
        os.unlink('server_local.cfg')

        c = Client(url=s.addr)
        self.assertEqual(c.url, s.addr)

        echo_str = 'test_echo'
        self.assertEqual(c.request(cmd='echo', arg=echo_str), echo_str)

        with self.assertRaises(RuntimeError) as e:
            # unknown attribute for command `echo`
            c.request(cmd='echo', wrong_attr=echo_str)
        self.assertIn('TypeError', str(e.exception))
        self.assertIn('unexpected keyword argument', str(e.exception))

        with self.assertRaises(RuntimeError) as e:
            # no command in request
            c.request(cmd='')
        self.assertIn('no command in request', str(e.exception))

        with self.assertRaises(RuntimeError) as e:
            # command [no_registered_cmd] unknown
            c.request(cmd='no_registered_cmd')
        self.assertIn('command [no_registered_cmd] unknown', str(e.exception))

        c.close()
        s.stop()
        s.wait()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestZMQClient()
    tc.test_client()

# ------------------------------------------------------------------------------

