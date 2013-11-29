
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


from radical.utils.which import *

def test_which():
    """ Test if 'which' can find things
    """
    assert which('doesnotexistatall') is None
    if os.path.isfile('/usr/bin/date'):
        assert which('date') == '/usr/bin/date'
    if os.path.isfile('/bin/date'):
        assert which('date') == '/bin/date'




