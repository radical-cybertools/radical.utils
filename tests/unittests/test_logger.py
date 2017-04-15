
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Unit tests for ru.get_logger(name)
'''

import radical.utils as ru


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    """ Test if the logger behaves like a singleton
    """
    # make sure singleton works
    assert ru.get_logger()                == ru.get_logger()
    assert ru.get_logger('radical.utils') == ru.get_logger('radical.utils')

def test_logger():
    """ Print out some messages with different log levels
    """
    cl = ru.get_logger('engine')
    cl = ru.get_logger('engine')
    cl.setLevel('DEBUG')
    
    assert cl is not None
    cl.debug('debug')
    cl.info('info')
    cl.warn('warning')
    cl.error('error')
    cl.fatal('fatal')



