#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_sh_quote():

    os.environ['FOO'] = '=foo='
    os.environ['BAR'] = '=bar='

    ret = ru.sh_callout('echo %s' % ru.sh_quote('$FOO $BAR'), shell=True)
    assert(ret[0] == '=foo= =bar=\n' and not ret[1] and not ret[2]), ret

    ret = ru.sh_callout('echo `echo %s`' % ru.sh_quote('$FOO $BAR'), shell=True)
    assert(ret[0] == '=foo= =bar=\n' and not ret[1] and not ret[2]), ret

    ret = ru.sh_callout('echo $(echo %s)' % ru.sh_quote('$FOO $BAR'), shell=True)
    assert(ret[0] == '=foo= =bar=\n' and not ret[1] and not ret[2]), ret


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_sh_quote()


# ------------------------------------------------------------------------------

