
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
class _Test () :
    """
    singleton helper class
    """

    __metaclass__ = ru.Singleton


# ------------------------------------------------------------------------------
def test_singleton () :
    """ 
    test if singleton instances are identical
    """ 

    t1 = _Test ()
    t2 = _Test ()

    assert (t1 == t2), "%s != %s" % (t1, t2)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_singleton ()

# ------------------------------------------------------------------------------

