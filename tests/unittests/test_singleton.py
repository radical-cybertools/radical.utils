
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for radical.utils.singleton.py
"""

from radical.utils.singleton import *

class _MyClass():
    __metaclass__ = Singleton


def test_Singleton():
    """ Test if singleton instances are identical
    """ 
    assert _MyClass() == _MyClass()



