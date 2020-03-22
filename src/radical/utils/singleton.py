
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import threading

from .atfork import atfork


_singleton_lock = threading.RLock ()


''' Provides a Singleton metaclass.  '''


# ------------------------------------------------------------------------------
#
class Singleton (type):
    '''
    A metaclass to 'tag' other classes as singleton::

        class MyClass(BaseClass):
            __metaclass__ = Singleton

    '''
    _instances = {}

    def __call__(cls, *args, **kwargs):

        with _singleton_lock:

            if  cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args,
                                                                     **kwargs)

            return cls._instances[cls]


# ------------------------------------------------------------------------------
#
def _atfork_prepare():
    pass


def _atfork_parent():
    pass


def _atfork_child():
    # release lock
    global _singleton_lock
    _singleton_lock = threading.RLock()


# lock cleaning can be disabled by setting RADICAL_UTILS_NO_ATFORK
if 'RADICAL_UTILS_NO_ATFORK' not in os.environ:
    atfork(_atfork_prepare, _atfork_parent, _atfork_child)


# ------------------------------------------------------------------------------


