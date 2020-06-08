#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os
os.environ['RADICAL_BASE'] = '/tmp'

import glob
import shutil
import time
from uuid import UUID

import radical.utils as ru

from unittest import TestCase

TEST_CASES_PATH = 'tests/test_cases/ids.*.json'


class IdsTestClass(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        cls._base_dir = ru.get_radical_base('utils')
        cls._pid_str = '%06d' % os.getpid()
        cls._user = None
        try:
            import getpass
            cls._user = getpass.getuser()
        except:
            cls._user = 'nobody'

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
    def setUp(self):
        pass

    # --------------------------------------------------------------------------
    #
    def tearDown(self):
        ru.reset_id_counters(prefix=None, reset_all_others=True)

    # --------------------------------------------------------------------------
    #
    def test_generate_id(self):

        for test_case in self._test_cases:

            # check possible exceptions firstly
            if 'exception' in test_case['result']:
                if test_case['result']['exception'] == 'TypeError':
                    _exc = TypeError
                elif test_case['result']['exception'] == 'ValueError':
                    _exc = ValueError
                else:
                    _exc = Exception

                self.assertRaises(_exc, ru.generate_id, **test_case['input'])
                continue

            # generate ID
            id_str = ru.generate_id(**test_case['input'])

            # if it is assumed that associated file will be created
            file_path = None
            if 'file_name_template' in test_case['result']:
                file_path_parts = [self._base_dir]
                if test_case['input'].get('ns'):
                    file_path_parts.append(test_case['input']['ns'])
                file_path_parts.append(
                    # here is a dict with all possible parameters that might be
                    # in the template name of the corresponding file (counter)
                    test_case['result']['file_name_template'] % {
                        'user': self._user,
                        'day_counter': int(time.time() / (60 * 60 * 24))})
                file_path = os.path.join(*file_path_parts)

            # start to go through test cases
            if not test_case['input'].get('mode') or \
                    test_case['input']['mode'] == ru.ID_SIMPLE:
                self.assertEqual(id_str, test_case['result']['id'])

            elif test_case['input']['mode'] == ru.ID_UNIQUE:
                self.assertTrue(id_str.endswith(test_case['result']['id_end']))
                self.assertIn(self._pid_str, id_str)

            elif test_case['input']['mode'] == ru.ID_UUID:
                uuid_str = id_str.split('%s.' % test_case['input']['prefix'])[1]
                self.assertIsInstance(UUID(uuid_str), UUID)

            elif test_case['input']['mode'] == ru.ID_PRIVATE:
                self.assertIn(self._user, id_str)
                # check that corresponding file was created
                self.assertTrue(os.path.isfile(file_path))

            elif test_case['input']['mode'] == ru.ID_CUSTOM:

                if '%(day_counter)' in test_case['input']['prefix']:
                    # check that created file contains the correct value
                    with open(file_path) as fd:
                        # file with counter contains the number of the next ID
                        # (ID was generated and the counter was increased)
                        counter = '%d' % (int(fd.readline()) - 1)
                    self.assertIn(counter, id_str)

                elif '%(item_counter)' in test_case['input']['prefix']:
                    # check that corresponding file was created
                    self.assertTrue(os.path.isfile(file_path))

    def test_generate_id_2nd_run(self):
        # check that counters got reset
        self.test_generate_id()

# ------------------------------------------------------------------------------
