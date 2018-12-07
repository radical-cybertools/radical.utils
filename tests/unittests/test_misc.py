
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


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
def test_get_env_ns():

    import os

    os.environ['RADICAL_UTILS_VERBOSE'] = 'DEBUG'
    os.environ['RADICAL_LOG_TGT']       = '/dev/null'

    ns = 'radical.utils'

    assert(ru.get_env_ns(ns, 'VERBOSE') == 'DEBUG')
    assert(ru.get_env_ns(ns, 'log.tgt') == '/dev/null')
    assert(ru.get_env_ns(ns, 'LOG.TGT') == '/dev/null')
    assert(ru.get_env_ns(ns, 'LOG_TGT') == '/dev/null')
    assert(ru.get_env_ns(ns, 'TGT_LOG') is None)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_round_to_base()
    test_round_upper_bound()
    test_sh_callout()
    test_get_env_ns()

# ------------------------------------------------------------------------------

