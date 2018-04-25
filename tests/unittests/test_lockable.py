
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import time
import threading
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_lockable () :
    """
    Test lockable decorator
    """

    # --------------------------------------------------------------------------
    @ru.Lockable
    class Test (object) :

        # ----------------------
        def __init__ (self) :

            self.val = False


        # ----------------------
        def test (self) :

            t.lock ()         # lock before spawning thread
            thread_1 = threading.Thread (target=self.test_1)
            thread_1.start () # thread will run until lock check
            time.sleep (0.1)  # enough time to trigger lock violation
            self.val = False  # set a bogus value
            t.unlock ()       # only now thread can set True
            thread_1.join ()  # make sure the value was set
            assert (self.val) # make sure the value was set correctly

            t.lock ()         # lock before spawning thread
            thread_2 = threading.Thread (target=self.test_2)
            thread_2.start () # thread will run until lock check
            time.sleep (0.1)  # enough time to trigger lock violation
            self.val = False  # set a bogus value
            t.unlock ()       # only now thread can set True
            thread_2.join ()  # make sure the value was set
            assert (self.val) # make sure the value was set correctly


        # ----------------------
        def test_1 (self) :

            with self :
                self.val = True


        # ----------------------
        def test_2 (self) :

            self.lock ()
            self.val = True
            self.unlock ()


    # --------------------------------------------------------------------------
    t = Test ()

    # check lock with resource manager
    with t : 
        pass
    
    # check explicit and recursive lock/unlock
    t.lock   (); assert (    t.locked())
    t.unlock (); assert (not t.locked())
    t.lock   (); assert (    t.locked())
    t.lock   (); assert (    t.locked())
    t.unlock (); assert (    t.locked())
    t.unlock (); assert (not t.locked())

    # check locking over threads
    t.test ()


    # --------------------------------------------------------------------------
    # check double unlock
    try                      : t.unlock (); assert (not t.locked())
    except RuntimeError as e : pass 
    except Exception    as e : assert (False), "RuntimeError != %s" % type(e)
    else                     : assert (False), "expected RuntimeError Exception, got nothing"


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_lockable ()

# ------------------------------------------------------------------------------

