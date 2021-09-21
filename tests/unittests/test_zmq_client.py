# pylint: disable=no-value-for-parameter

import msgpack

from unittest import mock, TestCase

from radical.utils.zmq import Client, Request, Response, Server


# ------------------------------------------------------------------------------
#
class TestZMQClient(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_request(self):

        # initialization
        r = Request()
        self.assertIsNone(r.cmd)
        self.assertIsNone(r.arg)

        # init with `from_dict` class method
        test_req = {'cmd': 'test', 'arg': [1, 2, 3]}
        r = Request.from_dict(req=test_req)
        self.assertEqual(r.cmd, test_req['cmd'])
        self.assertEqual(r.arg, test_req['arg'])

        # get packed instance
        packed_r = r.packb()
        self.assertIsInstance(packed_r, bytes)

    # --------------------------------------------------------------------------
    #
    def test_response(self):

        # initialization
        r = Response()
        self.assertIsNone(r.res)
        self.assertIsNone(r.err)
        self.assertIsInstance(r.exc, list)
        self.assertFalse(r.exc)  # empty list

        # init with `from_dict` class method
        test_msg = {'res': 'ok', 'err': '', 'exc': ['1', '2', '3']}
        r = Response.from_dict(msg=test_msg)
        self.assertEqual(r.res, test_msg['res'])
        self.assertEqual(r.err, test_msg['err'])
        self.assertEqual(r.exc, test_msg['exc'])

        # init with `from_msg` class method
        packed_msg = msgpack.packb(test_msg)
        r = Response.from_msg(msg=packed_msg)
        self.assertEqual(r.res, test_msg['res'])
        self.assertEqual(r.err, test_msg['err'])
        self.assertEqual(r.exc, test_msg['exc'])

        # `repr` & `str`
        self.assertIn('res: ', str(r))
        self.assertNotIn('err: ', str(r))
        self.assertIn('res: ', repr(r))
        self.assertIn('exc: ', repr(r))

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

        try:
            c = Client(url=s.addr)
            self.assertEqual(c.url, s.addr)

            test_arg = ['test_arg']
            resp = c.request(cmd='echo', arg=test_arg)

            self.assertIsInstance(resp, Response)
            self.assertEqual(resp.res, test_arg)

            with self.assertRaises(RuntimeError):
                # no command in request
                c.request(cmd='')

            with self.assertRaises(RuntimeError):
                # command [no_registered_cmd] unknown
                c.request(cmd='no_registered_cmd')

            c.close()
        except:
            pass

        s.stop()
        s.wait()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestZMQClient()
    tc.test_request()
    tc.test_response()
    tc.test_client()

# ------------------------------------------------------------------------------
