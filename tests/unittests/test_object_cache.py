
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import time

import radical.utils as ru


# ------------------------------------------------------------------------------
#
# helper state
#
_state = 0


# ------------------------------------------------------------------------------
def test_object_cache () :
    """
    Test object cache
    """

    global _state


    # --------------------------------------------------------------------------
    class _Test (object) :

        def __init__ (self) :

            global _state
            _state += 1


        def __del__ (self) :

            global _state
            _state -= 1


    # --------------------------------------------------------------------------
    def _test_1 (oc) :

        # create/cache two instances
        t1_1 = oc.get_obj ('id_1', _Test); assert (_state == 1), "%d" % _state
        t1_2 = oc.get_obj ('id_1', _Test); assert (_state == 1), "%d" % _state
        t2_1 = oc.get_obj ('id_2', _Test); assert (_state == 2), "%d" % _state
        t2_2 = oc.get_obj ('id_2', _Test); assert (_state == 2), "%d" % _state

        # we now have two different instances
        assert (t1_1 == t1_2)
        assert (t2_1 == t2_2)
        assert (t1_1 != t2_1)

        # one remove does not delete first instance
        assert (True == oc.rem_obj (t1_1))
        time.sleep (0.2);
        assert (_state == 2), "%d" % _state

        # second remove triggers *delayed* delete
        assert (True == oc.rem_obj (t1_2))
        assert (_state == 2), "%d" % _state

        # instances gow out of scope here - but still live in the object_cache
        return str(t2_1)


    # --------------------------------------------------------------------------
    def _test_2 (oc, uid) :

        t2_3 = oc.get_obj ('id_2', _Test); 
        assert (_state    ==   1), "%d" % _state
        assert (str(t2_3) == uid)

        assert (True == oc.rem_obj (t2_3)) # for t2_1
        assert (True == oc.rem_obj (t2_3)) # for t2_2
        assert (True == oc.rem_obj (t2_3)) # for t2_3


    # --------------------------------------------------------------------------

    # initial state
    assert (_state == 0), "%d" % _state
    oc  = ru.ObjectCache (timeout=0.1)

    uid = _test_1 (oc)


    time.sleep (0.2);
    assert (_state == 1), "%d" % _state

    _test_2 (oc, uid)

    assert (_state == 1), "%d" % _state
    time.sleep (0.2);
    assert (_state == 0), "%d" % _state


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_object_cache ()

# ------------------------------------------------------------------------------

