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
def test_env_prep():

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
def test_env_read():

    fname = '/tmp/env.%d' % os.getpid()
    os.system('/bin/sh -c "env > %s"' % fname)
    try:
        env = ru.env_read(fname)

        for k,v in env.items():
            assert(os.environ[k] == v)

        for k,v in os.environ.items():
            if k not in ru.env.BLACKLIST:
                assert(env[k] == v)

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_env_eval():

    fname = '/tmp/env.%d' % os.getpid()
    try:
        cmd   = "sh -c \"env " \
                "| sed -e \\\"s/^/export /\\\" "\
                "| sed -e \\\"s/=/='/\\\" " \
                "| sed -e \\\"s/$/'/\\\" "\
                "> %s\"" % fname
        os.system(cmd)
        env = ru.env_eval(fname)

        for k,v in env.items():
            if k not in ru.env.BLACKLIST:
                assert(os.environ[k] == v), [os.environ[k], v]

        for k,v in os.environ.items():
            if k not in ru.env.BLACKLIST:
                assert(env[k] == v)

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_env_prep()
    test_env_read()
    test_env_eval()


# ------------------------------------------------------------------------------

