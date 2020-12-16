#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os

import radical.utils as ru

# test environment specific
BLACKLIST_LOCAL_SYS = ['XPC_SERVICE_NAME']


# ------------------------------------------------------------------------------
#
def test_prep_env():

    try   : del(os.environ['TEST'])
    except: pass

    # clean `os.environ`
    for env_name in BLACKLIST_LOCAL_SYS:
        if env_name in os.environ:
            del os.environ[env_name]

    env = dict(os.environ)
    ret = ru.env_prep(env)
    _, only_ret, changed = ru.env_diff(env, ret)
    assert(not only_ret), only_ret
    assert(not changed), changed

    env = dict(os.environ)

    os.environ['BZZ'] = 'foo'
    env       ['BAR'] = 'bar'

    ret = ru.env_prep(environment=env, script_path='/tmp/test.env')
    _, only_ret, changed = ru.env_diff(env, ret)
    assert(not only_ret), only_ret
    assert(not changed),  changed

    out, _, ret = ru.sh_callout('export BZZ=x; . /tmp/test.env; echo $BZZ',
                                  shell=True)
    out = out.strip()
    assert(not out), out
    assert(not ret), ret

    out, _, ret = ru.sh_callout('unset BAR; . /tmp/test.env; echo $BAR',
                                  shell=True)
    out = out.strip()
    assert(out == 'bar'), out
    assert(not ret), ret


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_prep_env()


# ------------------------------------------------------------------------------

