
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_singleton():
    '''
    Test if the logger behaves like a singleton
    '''

    # make sure singleton works
    assert ru.get_logger('radical.utils') == ru.get_logger('radical.utils')


# ------------------------------------------------------------------------------
#
def test_logger():
    '''
    Print out some messages with different log levels
    '''

    tmp = ru.get_logger('engine')
    tmp = ru.get_logger('engine')
    tmp.setLevel('DEBUG')

    tmp.debug('debug')
    tmp.info('info')
    tmp.warn('warning')
    tmp.error('error')
    tmp.fatal('fatal')


# ------------------------------------------------------------------------------

