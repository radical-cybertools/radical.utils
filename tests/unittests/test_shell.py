#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os

from unittest import TestCase

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class ShellTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_sh_quote(self):

        os.environ['FOO'] = '=foo='
        os.environ['BAR'] = '=bar='

        test_cases = [
            ['echo %s' % ru.sh_quote('$FOO $BAR'),         '=foo= =bar=\n'],
            ['echo %s' % ru.sh_quote('`echo $FOO $BAR`'),  '=foo= =bar=\n'],
            ['echo %s' % ru.sh_quote('$(echo $FOO $BAR)'), '=foo= =bar=\n']]

        for cmd, out in test_cases:

            ret = ru.sh_callout(cmd, shell=True)
            assert ret[0] == out and not ret[1] and not ret[2], [cmd, out, ret]


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = ShellTestCase()
    tc.test_sh_quote()


# ------------------------------------------------------------------------------

