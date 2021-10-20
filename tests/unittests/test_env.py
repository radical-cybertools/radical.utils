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
    only_env, only_ret, changed = ru.env_diff(env, ret)
    assert(not only_ret), only_ret
    assert(not changed), changed

    env = dict(os.environ)

    os.environ['BASH_FUNC_foo%%']  = '() {echo foo}'
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

    out, err, ret = ru.sh_callout('export OS_ONLY=x; . /tmp/test.env; echo $OS_ONLY',
                                 shell=True)
    import pprint
    print('=== ret')
    pprint.pprint(ret)
    print('=== out')
    pprint.pprint(out)
    print('=== err')
    pprint.pprint(err)
    assert(not out.strip()), out
    assert(not err.strip()), err
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
    os.system('/bin/bash -c "env > %s"' % fname)
    try:
        env = ru.env_read(fname)

        for k,v in env.items():
            assert(os.environ[k] == v), [k, os.environ[k], v]

        for k,v in os.environ.items():
            if k not in ru.env.BLACKLIST:
                if k.startswith('BASH_FUNC_'):
                    # bash funcs are not exported to other shells
                    continue
                assert(env[k] == v)

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_env_write():

    fname     = '/tmp/env.%d' % os.getpid()
    env       = {'TEST_ENV': 'test_env'}
    unset_env = ['UNSET_ENV', '1_NON_VALID']
    try:
        ru.env_write(fname, env, unset_env)

        with open(fname) as fd:
            file_content = ''.join(fd.readlines())

        assert ("export TEST_ENV='test_env'" in file_content)
        assert ('UNSET_ENV'                  in file_content)
        assert ('1_NON_VALID'            not in file_content)
        assert ('# pre_exec'             not in file_content)

    finally:
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_env_proc():

    key = 'TEST_SHARED'
    val = 'env_shared'

    env = {key: val}
    env_proc = ru.EnvProcess(env=env)

    with env_proc:
        env_proc.put(ru.sh_callout('echo -n $%s' % key, shell=True))
    out = str(env_proc.get())

    assert(isinstance(out, str))
    assert(key not in os.environ)
    assert(env[key] in out)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_env_prep()
    test_env_read()
    test_env_write()
    test_env_proc()


# ------------------------------------------------------------------------------

