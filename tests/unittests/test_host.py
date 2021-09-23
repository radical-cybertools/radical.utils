#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import socket

os.environ['RADICAL_BASE'] = '/tmp'

import glob
import shutil

import radical.utils as ru

from unittest import mock, TestCase

TEST_CASES_PATH = 'tests/test_cases/host.*.json'


# ------------------------------------------------------------------------------
#
class HostTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        cls._base_dir = ru.get_radical_base('utils')

        cls._test_cases = []
        for f in glob.glob(TEST_CASES_PATH):
            cls._test_cases.extend(ru.read_json(f))

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, '_base_dir') and os.path.isdir(cls._base_dir):
            try:
                shutil.rmtree(cls._base_dir)
            except OSError as e:
                print('[ERROR] %s - %s' % (e.filename, e.strerror))

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.host.socket')
    def test_get_hostname(self, mocked_socket):
        """Check caching wrapper for gethostname."""

        # Note that ru.get_hostname() actually uses getfqdn()
        fq_hostname = socket.getfqdn()
        mocked_socket.gethostname.return_value = socket.gethostname()
        mocked_socket.getfqdn.return_value = fq_hostname

        ru.host._hostname = None                         # pylint: disable=W0212
        self.assertFalse(mocked_socket.gethostname.called)
        self.assertFalse(mocked_socket.getfqdn.called)
        self.assertEqual(ru.get_hostname(), fq_hostname)
        self.assertTrue(mocked_socket.getfqdn.called)

        mocked_socket.getfqdn.reset_mock()
        ru.get_hostname()  # System level hostname lookup is not called 2nd time
        self.assertFalse(mocked_socket.gethostname.called)
        self.assertFalse(mocked_socket.getfqdn.called)

        # reset
        ru.host._hostname = None                         # pylint: disable=W0212

    # --------------------------------------------------------------------------
    #
    def test_create_hostfile(self):

        for test_case in self._test_cases:

            if 'hostlist' not in test_case['input']:
                continue
            hostlist = test_case['input']['hostlist']
            sep      = test_case['input'].get('separator', ' ')

            filename = ru.create_hostfile(self._base_dir, 'tc', hostlist, sep)

            with open(filename, 'r', encoding='utf8') as f:
                hostfile_lines = f.readlines()
            self.assertEqual(test_case['result']['lines'], hostfile_lines)

    # --------------------------------------------------------------------------
    #
    def test_compress_hostlist(self):

        for test_case in self._test_cases:

            if 'hostlist' not in test_case['input']:
                continue
            hostlist = test_case['input']['hostlist']

            self.assertEqual(test_case['result']['hostlist_compressed'],
                             ru.compress_hostlist(hostlist))

    # --------------------------------------------------------------------------
    #
    def test_get_hostlist_by_range(self):

        for test_case in self._test_cases:

            if 'hoststring_range' not in test_case['input']:
                continue
            hoststring = test_case['input']['hoststring_range']

            if test_case['result'] == 'ValueError':

                with self.assertRaises(ValueError):
                    # non numeric set of ranges OR incorrect range format
                    ru.get_hostlist_by_range(hoststring)

            else:

                prefix = test_case['input'].get('prefix', '')
                width  = test_case['input'].get('width', 0)

                self.assertEqual(test_case['result']['hostlist'],
                                 ru.get_hostlist_by_range(hoststring=hoststring,
                                                          prefix=prefix,
                                                          width=width))

    # --------------------------------------------------------------------------
    #
    def test_get_hostlist(self):

        for test_case in self._test_cases:

            if 'hoststring' not in test_case['input']:
                continue
            hoststring = test_case['input']['hoststring']

            if test_case['result'] == 'ValueError':

                with self.assertRaises(ValueError):
                    ru.get_hostlist(hoststring)

            else:

                self.assertEqual(test_case['result']['hostlist'],
                                 ru.get_hostlist(hoststring=hoststring))


# ------------------------------------------------------------------------------
