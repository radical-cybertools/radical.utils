#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2012-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
os.environ['RADICAL_BASE'] = '/tmp'

import shutil

import radical.utils as ru

from unittest import TestCase


# ------------------------------------------------------------------------------
#
class LoggerTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        cls._base_dir = ru.get_radical_base('utils')

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
    def test_init(self):

        logger_name = 'log_name_default'
        l = ru.Logger(logger_name)
        log_path = os.path.join(os.getcwd(), '%s.log' % logger_name)
        self.assertTrue(os.path.isfile(log_path))

        self.assertEqual(l.name,    logger_name)
        self.assertEqual(l.ns,      logger_name)
        # default values
        self.assertEqual(l.path,    os.getcwd())
        self.assertEqual(l.level,   'ERROR')
        self.assertEqual(l.targets, ['.'])

        os.remove(log_path)
        self.assertFalse(os.path.isfile(log_path))
        l.close()

        # set log directory as an input parameter `path`
        l = ru.Logger(name='log_base', path=self._base_dir)
        log_path = os.path.join(self._base_dir, 'log_base.log')
        self.assertTrue(os.path.isfile(log_path))
        l.close()

        # set log file using a corresponding environment variable
        log_env_tgt_saved = os.environ.get('RADICAL_UTILS_LOG_TGT', -1)
        log_path = os.path.join(self._base_dir, 'log_base_tgt.log')
        os.environ['RADICAL_UTILS_LOG_TGT'] = log_path
        l = ru.Logger('log_base_tgt', ns='radical.utils')
        self.assertTrue(os.path.isfile(log_path))
        l.close()
        if log_env_tgt_saved == -1:
            del os.environ['RADICAL_UTILS_LOG_TGT']
        else:
            os.environ['RADICAL_UTILS_LOG_TGT'] = log_env_tgt_saved

    # --------------------------------------------------------------------------
    #
    def test_log_levels(self):

        log_path = os.path.join(self._base_dir, 'log_lvl.log')
        l = ru.Logger('log_lvl', targets=[log_path])

        l.setLevel('INFO')

        l.debug('debug')  # the one record that will not be recorded
        l.info('info')
        l.warning('warning')
        l.error('error')
        l.fatal('fatal')
        l.critical('critical')

        with open(log_path) as fd:
            log_records = fd.readlines()

        debug_found, non_debug_found = False, False
        for log_record in log_records:
            if 'debug' in log_record:
                debug_found = True
            if         'info'     in log_record \
                    or 'warning'  in log_record \
                    or 'error'    in log_record \
                    or 'fatal'    in log_record \
                    or 'critical' in log_record:
                non_debug_found = True
            else:
                non_debug_found = False

        self.assertFalse(debug_found)
        self.assertTrue(non_debug_found)
        l.close()

    # --------------------------------------------------------------------------
    #
    def test_debug_level(self):

        log_path = os.path.join(self._base_dir, 'log_debug_lvl.log')
        l = ru.Logger('log_debug_lvl', targets=[log_path], level='DEBUG_2')

        l.debug_1('debug_1')
        l.debug_2('debug_2')
        l.debug_3('debug_3')

        with open(log_path) as fd:
            log_records = fd.readlines()

        debug_3_found = False
        for log_record in log_records:
            self.assertTrue('debug_' in log_record)
            if 'debug_3' in log_record:
                debug_3_found = True

        self.assertFalse(debug_3_found)
        l.close()

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = LoggerTestCase()
    tc.test_init()
    tc.test_log_levels()
    tc.test_debug_level()


# ------------------------------------------------------------------------------
