#!/usr/bin/env python

__author__    = 'Radical.Utils Development Team (Andre Merzky, Ole Weidner)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'

import radical.utils as ru                                                # noqa


# ------------------------------------------------------------------------------
#
class _Test (metaclass=ru.Singleton) :
    '''
    singleton helper class
    '''


# ------------------------------------------------------------------------------
#
def test_singleton () :
    '''
    test if singleton instances are identical
    '''

    t1 = _Test ()
    t2 = _Test ()

    assert (t1 == t2), '%s != %s' % (t1, t2)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_singleton ()


# ------------------------------------------------------------------------------

