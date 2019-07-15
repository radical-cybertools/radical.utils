__author__    = "Radical.Utils Development Team (Mark Santcroos)"
__copyright__ = "Copyright 2015, RADICAL@Rutgers"
__license__   = "MIT"


import time
import threading
import radical.utils as ru

SIZE = 10
MAX  =  3
ITER = 10


# ------------------------------------------------------------------------------
#
def test_lease_manager():
    """
    Test LeaseManager
    """

    # --------------------------------------------------------------------------
    class Test(object):

        # ----------------------------------------------------------------------
        def __init__(self):

            self.val = False
            self.lm  = None

        # ----------------------------------------------------------------------
        def test(self):

            self.lm = ru.LeaseManager(SIZE)

            iw_thread_1 = threading.Thread(target=self.iw_thread,
                                           kwargs={'uid': 1, 'pool': 'pool1'})
            iw_thread_2 = threading.Thread(target=self.iw_thread,
                                           kwargs={'uid': 2, 'pool': 'pool2'})
            ow_thread_1 = threading.Thread(target=self.ow_thread,
                                           kwargs={'uid': 1, 'pool': 'pool1'})
            ow_thread_2 = threading.Thread(target=self.ow_thread,
                                           kwargs={'uid': 2, 'pool': 'pool2'})
            mon_thread  = threading.Thread(target=self.mon_thread,
                                           kwargs={'uid': 1, 'pool': 'pool1'})
            iw_thread_1.start()
            iw_thread_2.start()
            ow_thread_1.start()
            ow_thread_2.start()
            mon_thread.start()

            iw_thread_1.join()
            iw_thread_2.join()
            ow_thread_1.join()
            ow_thread_2.join()
            mon_thread.join()

        # ----------------------------------------------------------------------
        def iw_thread(self, uid, pool):

            name = "IWo Thread-%d (%s)" % (uid, pool)

            for _ in range(ITER):
                leases = list()
                for _ in range(MAX):
                    lease = self.lm.lease(pool, dict)
                    lease.obj['name'] = name
                    leases.append(lease)

                time.sleep(.1)

                for lease in leases:
                    self.lm.release(lease)

        # ----------------------------------------------------------------------
        def ow_thread(self, uid, pool):

            name = "OWo Thread-%d (%s)" % (uid, pool)

            for _ in range(ITER):
                leases = list()
                for _ in range(MAX):
                    lease = self.lm.lease(pool, dict)
                    lease.obj['name'] = name
                    leases.append(lease)

                time.sleep(.1)

                for lease in leases:
                    self.lm.release(lease)

        # ----------------------------------------------------------------------
        def mon_thread(self, uid, pool):

            name = "Mon Thread-%d (%s)" % (uid, pool)

            for _ in range(ITER):
                leases = list()
                for _ in range(MAX):
                    lease = self.lm.lease(pool, dict)
                    lease.obj['name'] = name
                    leases.append(lease)

                time.sleep(.1)

                for lease in leases:
                    self.lm.release(lease)
        # ----------------------------------------------------------------------

    t = Test()
    t.test()


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_lease_manager()


# ------------------------------------------------------------------------------

