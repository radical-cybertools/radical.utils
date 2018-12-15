
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru
import os


# ------------------------------------------------------------------------------
def test_which () :
    """ 
    Test if 'which' can find things
    """

    shell_date = os.path.normpath (os.popen ("which date").read().strip())
    utils_date = ru.which ('date')

    assert (shell_date == utils_date), "'%s' != '%s'" % (shell_date, utils_date)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_which ()

# ------------------------------------------------------------------------------

