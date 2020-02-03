#!/usr/bin/env python

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import copy
import pytest

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_import_file():

    syms = ru.import_file('%s/data/import_file.py' % os.path.dirname(__file__))

    assert(syms['functions']['foo'](1))
    assert(syms['functions']['foo'](4, 4))

    f = syms['classes']['Foo']()
    assert(f.foo(1))
    assert(f.foo(4, 4))


# ------------------------------------------------------------------------------
#
def test_round_to_base():

    assert(ru.round_to_base(1.5, 2) == 2)
    assert(ru.round_to_base(3.5, 2) == 4)
    assert(ru.round_to_base(4.5, 2) == 4)

    assert(ru.round_to_base(11.5, 20) == 20)
    assert(ru.round_to_base(23.5, 20) == 20)
    assert(ru.round_to_base(34.5, 20) == 40)


# ------------------------------------------------------------------------------
#
def test_round_upper_bound():

    assert(ru.round_upper_bound(0.5) ==  1)
    assert(ru.round_upper_bound(1.5) ==  2)
    assert(ru.round_upper_bound(2.5) ==  5)
    assert(ru.round_upper_bound(4.5) ==  5)
    assert(ru.round_upper_bound(5.5) == 10)
    assert(ru.round_upper_bound(9.5) == 10)

    assert(ru.round_upper_bound( 5000) ==  10000)                   # noqa: E201
    assert(ru.round_upper_bound(15000) ==  20000)
    assert(ru.round_upper_bound(25000) ==  50000)
    assert(ru.round_upper_bound(45000) ==  50000)
    assert(ru.round_upper_bound(55000) == 100000)
    assert(ru.round_upper_bound(95000) == 100000)


# ------------------------------------------------------------------------------
#
def test_sh_callout():

    out, err, ret = ru.sh_callout('echo TRUE')
    assert(out == 'TRUE\n'),  out
    assert(err == ''),        err
    assert(ret == 0),         ret

    out, err, ret = ru.sh_callout('false')
    assert(out == ''),        out
    assert(err == ''),        err
    assert(ret == 1),         ret

    out, err, ret = ru.sh_callout('echo FALSE 1>&2; exit 2', shell=True)
    assert(out == ''),        out
    assert(err == 'FALSE\n'), err
    assert(ret == 2),         ret


# ------------------------------------------------------------------------------
#
def test_sh_callout_async():

    pass

#     import t ime
#     t_0 = time.time()
#     p   = ru.sh_callout_async('echo TRUE && sleep 1', shell=True, stdout=True)
#
#     assert(p.stdout.get() == 'TRUE')
#     assert(p.state        == ru.RUNNING)
#
#     t_1 = time.time()
#
#     assert(p.stdout.get() is None)
#     assert(p.state        == ru.DONE)
#
#     t_2 = time.time()
#
#     assert(t_1 - t_0 < 0.1)
#     assert(t_2 - t_0 > 1.0)


# ------------------------------------------------------------------------------
#
def test_get_env_ns():

    os.environ['RADICAL_UTILS_LOG_LVL'] = 'DEBUG'
    os.environ['RADICAL_LOG_TGT']       = '/dev/null'

    for ns in ['radical.utils.test', 'radical.utils']:

        assert(ru.get_env_ns('LOG_LVL', ns) == 'DEBUG')
        assert(ru.get_env_ns('log.tgt', ns) == '/dev/null')
        assert(ru.get_env_ns('LOG.TGT', ns) == '/dev/null')
        assert(ru.get_env_ns('LOG_TGT', ns) == '/dev/null')
        assert(ru.get_env_ns('TGT_LOG', ns) is None)


# ------------------------------------------------------------------------------
#
def test_expand_env():

    noenv = {'BIZ' : 'biz'}
    env   = {'BAR' : 'bar'}

    os.environ['BAR'] = 'bar'
    os.environ['BIZ'] = 'biz'

    bar = os.environ.get('BAR')
    biz = os.environ.get('BIZ')

    tc = {'${BAR}'             : [bar,                  # os.environ
                                  'bar',                # env
                                  None],                # noenv
          'foo_${BAR}_baz'     : ['foo_%s_baz' % bar,
                                  'foo_bar_baz',
                                  'foo__baz'   ],
          'foo_${BAR:buz}_baz' : ['foo_%s_baz' % bar,
                                  'foo_bar_baz',
                                  'foo_buz_baz'],
          'foo_${BAR:$BIZ}_baz': ['foo_%s_baz' % bar,
                                  'foo_bar_baz',
                                  'foo_%s_baz' % biz],
         }

    # test string expansion (and also create list and dict for other tests
    l = list()
    d = dict()
    i = 0
    for k,v in tc.items():
        assert(ru.expand_env(k       ) == v[0])
        assert(ru.expand_env(k,   env) == v[1])
        assert(ru.expand_env(k, noenv) == v[2])
        l.append(k)
        d[i] = k
        i   += 1

    # test list expansion
    l0 = copy.deepcopy(l)
    l1 = copy.deepcopy(l)
    l2 = copy.deepcopy(l)

    ru.expand_env(l0)
    ru.expand_env(l1, env)
    ru.expand_env(l2, noenv)

    for i,v in enumerate(l):
        assert(l0[i] == tc[v][0])
        assert(l1[i] == tc[v][1])
        assert(l2[i] == tc[v][2])

    # test dict expansion
    d0 = copy.deepcopy(d)
    d1 = copy.deepcopy(d)
    d2 = copy.deepcopy(d)

    ru.expand_env(d0)
    ru.expand_env(d1, env)
    ru.expand_env(d2, noenv)

    for k,v in d0.items(): assert(v == tc[d[k]][0])
    for k,v in d1.items(): assert(v == tc[d[k]][1])
    for k,v in d2.items(): assert(v == tc[d[k]][2])

    # test `ignore_missing` flag
    env = {'BAR' : 'bar'}
    src = 'foo${FIZ}.baz'
    tgt = 'foo.baz'
    assert(ru.expand_env(src, env                     ) == tgt)
    assert(ru.expand_env(src, env, ignore_missing=True) == tgt)

    with pytest.raises(ValueError):
        ru.expand_env(src, env, ignore_missing=False)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_import_file()
    test_round_to_base()
    test_round_upper_bound()
    test_sh_callout()
    test_sh_callout_async()
    test_get_env_ns()
    test_expand_env()


# ------------------------------------------------------------------------------

