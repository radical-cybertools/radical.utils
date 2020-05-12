#!/usr/bin/env python

import os

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_locks():
    '''
    Test debug lock wrappers
    '''

    os.environ['RADICAL_DEBUG'] = 'True'

    l  = ru.Lock()
    rl = ru.RLock(name='bar')

    assert(not l.waits)
    assert(not rl.waits)

    with l:
        with rl:
            assert(not l.waits)
            assert(not rl.waits)

    assert(l.name  in ru.debug._debug_helper.locks)                       # noqa
    assert(rl.name in ru.debug._debug_helper.rlocks)                      # noqa

    ru.debug._debug_helper.unregister_lock(l.name)                        # noqa
    ru.debug._debug_helper.unregister_rlock(rl.name)                      # noqa

    assert(l.name  not in ru.debug._debug_helper.locks)                   # noqa
    assert(rl.name not in ru.debug._debug_helper.rlocks)                  # noqa


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_locks()


# ------------------------------------------------------------------------------

