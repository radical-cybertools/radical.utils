#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from unittest import TestCase


# ------------------------------------------------------------------------------
#
class TestDict(ru.TypedDict):
    pass


# ------------------------------------------------------------------------------
#
class ModulesTC(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_get_type(self):

        for tname, t in [('str', str), ('int', int), ('float', float),
                         ('list', list), ('tuple', tuple), ('dict', dict)]:
            self.assertIs(ru.get_type(tname), t)

        class LocalTestClass:
            pass

        self.assertIs(ru.get_type('LocalTestClass'), LocalTestClass)

    # --------------------------------------------------------------------------
    #
    def test_load_class(self):

        fpath = '/tmp/file_with_py_class.py'

        with ru.ru_open(fpath, mode='w') as fd:
            fd.write("""
class NewClass:

    def hello(self):
        return 'hello'

""")
        obj = ru.load_class(fpath=fpath, cname='NewClass')()
        self.assertEqual(obj.hello(), 'hello')

        os.unlink(fpath)

    # --------------------------------------------------------------------------
    #
    def test_load_class_with_type(self):

        f = ru.load_class(fpath=__file__, cname='TestDict', ctype=ru.TypedDict)
        self.assertIsInstance(f(), ru.TypedDict)

# ------------------------------------------------------------------------------

