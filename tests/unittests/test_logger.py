#!/usr/bin/env python

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_logger():
    '''
    Print out some messages with different log levels
    '''

    tmp = ru.Logger('engine')
    tmp = ru.Logger('engine')
    tmp.setLevel('DEBUG')

    tmp.debug('debug')
    tmp.info('info')
    tmp.warning('warning')
    tmp.error('error')
    tmp.fatal('fatal')


# ------------------------------------------------------------------------------
#
def test_env():
    '''
    Print out some messages with different log levels
    '''

    tmp = ru.Logger('engine')
    tmp = ru.Logger('engine')
    tmp.setLevel('DEBUG')

    tmp.debug('debug')
    tmp.info('info')
    tmp.warning('warning')
    tmp.error('error')
    tmp.fatal('fatal')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_logger()
    test_env()


# ------------------------------------------------------------------------------

