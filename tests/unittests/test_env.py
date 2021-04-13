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

    os.environ['OS_ONLY']  = 'os_only'
    os.environ['SHARED']   = 'os_shared'
    env       ['SHARED']   = 'env_shared'
    env       ['ENV_ONLY'] = 'env_only'

    ret = ru.env_prep(environment=env, unset=['OS_ONLY'], script_path='/tmp/test.env')
    assert('OS_ONLY' not in ret), ret['OS_ONLY']
    assert(ret['ENV_ONLY'] == 'env_only')
    assert(ret['SHARED'] == 'env_shared')

    only_env, only_ret, changed = ru.env_diff(env, ret)
    assert(not only_ret), [only_env, only_ret, changed]
    assert(not changed),  changed

    out, _, ret = ru.sh_callout('export OS_ONLY=x; . /tmp/test.env; echo $OS_ONLY',
                                 shell=True)
    out = out.strip()
    assert(not out), out
    assert(not ret), ret

    out, _, ret = ru.sh_callout('unset ENV_ONLY; . /tmp/test.env; echo $ENV_ONLY',
                                  shell=True)
    out = out.strip()
    assert(out == 'env_only'), out
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

