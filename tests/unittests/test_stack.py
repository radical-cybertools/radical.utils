#!/usr/bin/env python

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import shutil

import radical.utils as ru

from unittest import TestCase


# ------------------------------------------------------------------------------
#
class StackTestClass(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        ns_mod              = ru.import_module('radical')
        cls._radical_ns_dir = ns_mod.__path__._path[0]
        # create a directory inside "radical" namespace (false package)
        ru.rec_makedir(cls._radical_ns_dir + '/dummy')

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls):
        # delete false package
        try:
            shutil.rmtree(cls._radical_ns_dir + '/dummy')
        except OSError as e:
            print('[ERROR] %s - %s' % (e.filename, e.strerror))

    # --------------------------------------------------------------------------
    #
    def test_stack(self):

        # - "radical" namespace (default) -
        stack_output = ru.stack()
        self.assertIn('sys', stack_output)
        self.assertIn('radical', stack_output)
        for module in ['pilot', 'saga', 'utils']:
            self.assertIn('radical.%s' % module, stack_output['radical'])
        self.assertEqual(stack_output['radical']['radical.dummy'], '?')

        os.environ['RADICAL_DEBUG'] = 'TRUE'
        stack_output = ru.stack()
        self.assertTrue(stack_output['radical']['radical.dummy'].endswith(
            "no attribute 'version_detail'"))

        # - "requests" - not a namespace, but a package-
        stack_output = ru.stack(ns=['radical', 'requests'])
        self.assertFalse(stack_output['requests'])

        # - non existed namespace/package -
        with self.assertRaises(ModuleNotFoundError):
            _ = ru.stack(ns='no_valid_pkg')

# ------------------------------------------------------------------------------
