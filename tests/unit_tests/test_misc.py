
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import time
import os

import radical.utils as ru


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

    assert(ru.round_upper_bound( 5000) ==  10000)
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
    assert(err == ''),      err
    assert(ret == 0),       ret

    out, err, ret = ru.sh_callout('false')
    assert(out == ''),      out
    assert(err == ''),      err
    assert(ret == 1),       ret

    out, err, ret = ru.sh_callout('echo FALSE 1>&2; exit 2', shell=True)
    assert(out == ''),      out
    assert(err == 'FALSE\n'), err
    assert(ret == 2),       ret


# ------------------------------------------------------------------------------
#
def test_sh_callout_async():

    pass

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

    import os

    noenv = {'FOO' : 'foo'}
    env   = {'BAR' : 'bar'}

    val = os.environ.get('BAR')
    if val is None: tmp = 'buz'
    else          : tmp = val
    if val is None: val = ''

    assert(ru.expand_env('foo_$BAR.baz'             ) == 'foo_%s.baz' % val)
    assert(ru.expand_env('foo_${BAR}_baz'           ) == 'foo_%s_baz' % val)
    assert(ru.expand_env('foo_${BAR:buz}_baz'       ) == 'foo_%s_baz' % tmp)

    assert(ru.expand_env('foo_$BAR.baz'      , noenv) == 'foo_.baz'   )
    assert(ru.expand_env('foo_${BAR}_baz'    , noenv) == 'foo__baz'   )
    assert(ru.expand_env('foo_${BAR:buz}_baz', noenv) == 'foo_buz_baz')

    assert(ru.expand_env('foo_$BAR.baz'      , env  ) == 'foo_bar.baz')
    assert(ru.expand_env('foo_${BAR}_baz'    , env  ) == 'foo_bar_baz')
    assert(ru.expand_env('foo_${BAR:buz}_baz', env  ) == 'foo_bar_baz')


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_round_to_base()
    test_round_upper_bound()
    test_sh_callout()
    test_sh_callout_async()
    test_get_env_ns()
    test_expand_env()



# ------------------------------------------------------------------------------

