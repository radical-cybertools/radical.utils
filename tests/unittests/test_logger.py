
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
    assert ru.ObjectCache()                == ru.ObjectCache()
    assert ru.ObjectCache('radical.utils') == ru.ObjectCache('radical.utils')


# ------------------------------------------------------------------------------
#
def test_logger():
    """
    Print out some messages with different log levels
    """
    cl = ru.get_logger('radical.utils.test')
    cl = ru.get_logger('radical.utils.test')
    cl.setLevel('DEBUG')

    assert cl is not None
    cl.debug('debug')
    cl.info('info')
    cl.warn('warning')
    cl.error('error')
    cl.fatal('fatal')


# ------------------------------------------------------------------------------

