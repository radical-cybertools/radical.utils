__author__    = "Radical.Utils Development Team (Mark Santcroos)"
__copyright__ = "Copyright 2015, RADICAL@Rutgers"
__license__   = "MIT"


import time
import sys
import threading
import radical.utils as ru


def debug(*arg):
    #print arg
    pass
# ------------------------------------------------------------------------------
#
def test_lease_manager() :
    """
    Test LeaseManager
    """

    # --------------------------------------------------------------------------
    class Test (object) :

        # ----------------------
        def __init__ (self) :

            self.val = False


        # ----------------------
        def test(self) :

            self.lm = ru.LeaseManager()
            self.lm2 = ru.LeaseManager()

            iw_thread_1 = threading.Thread(target=self.iw_thread, kwargs={'id': 1, 'pool': 'pool1'})
            iw_thread_1.start() # thread will run until lock check

            iw_thread_2 = threading.Thread(target=self.iw_thread, kwargs={'id': 2, 'pool': 'pool2'})
            iw_thread_2.start() # thread will run until lock check

            ow_thread_1 = threading.Thread(target=self.ow_thread, kwargs={'id': 1, 'pool': 'pool3'})
            ow_thread_1.start() # thread will run until lock check

            ow_thread_2 = threading.Thread(target=self.ow_thread, kwargs={'id': 2, 'pool': 'pool4'})
            ow_thread_2.start() # thread will run until lock check

            mon_thread = threading.Thread(target=self.mon_thread, kwargs={'pool': 'pool5'})
            mon_thread.start() # thread will run until lock check

            # mon_thread.join()
            # iw_thread_1.join()
            # iw_thread_2.join()
            # ow_thread_1.join()
            # ow_thread_2.join()

        # -----------------
        def iw_thread(self, id, pool):
            name = "IW Thread-%d: lease()" % id

            while True:
                debug("%s: lease()" % name)
                lease = self.lm.lease(pool, dict)
                debug("%s: leased()" % name)
                lease.obj['name'] = name
                time.sleep(.1)
                #self.lm.release('lease')
                self.lm.release(lease)
                #self.lm.release(lease, delete=True)
                #self.lm2.release(lease)
                debug("%s: released()" % name)


        # ----------------------
        def ow_thread(self, id, pool) :

            name = "OW Thread-%d: lease()" % id

            while True:
                debug("%s: lease()" % name)
                lease = self.lm.lease(pool, dict)
                debug("%s: leased()" % name)
                lease.obj['name'] = name
                time.sleep(.1)
                self.lm.release(lease)
                debug("%s: released()" % name)


        # ----------------------
        def mon_thread(self, pool) :

            name = "Mon Thread: lease()"

            while True:
                debug("%s: lease()" % name)
                lease = self.lm.lease(pool, dict)
                debug("%s: leased()" % name)
                lease.obj['name'] = name
                time.sleep(.1)
                self.lm.release(lease)
                debug("%s: released()" % name)
                #sys.exit(1)


    # --------------------------------------------------------------------------
    t = Test()

    t.test()


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_lease_manager()

# ------------------------------------------------------------------------------

