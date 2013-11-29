
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
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



