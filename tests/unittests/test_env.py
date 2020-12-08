#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_prep_env():

    try   : del(os.environ['TEST'])
    except: pass

    src = dict(os.environ)
    ret = ru.env_prep(src)
    only_src, only_ret, changed = ru.env_diff(src, ret)
    assert(not only_ret), only_ret
    assert(not changed), changed

    src = dict(os.environ)

    os.environ['FOO'] = 'foo'
    src       ['BAR'] = 'bar'

    ret = ru.env_prep(src=src, tgt='/tmp/test.env')
    only_src, only_ret, changed = ru.env_diff(src, ret)
    assert(not only_ret), only_ret
    assert(not changed),  changed

    out, err, ret = ru.sh_callout('export FOO=x; . /tmp/test.env; echo $FOO',
                                  shell=True)
    out = out.strip()
    err = err.strip()
    assert(not out), out
    assert(not ret), ret

    out, err, ret = ru.sh_callout('unset BAR; . /tmp/test.env; echo $BAR',
                                  shell=True)
    out = out.strip()
    err = err.strip()
    assert(out == 'bar'), out
    assert(not ret), ret


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_prep_env()


# ------------------------------------------------------------------------------

