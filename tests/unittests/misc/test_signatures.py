
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


""" Unit tests for radical.utils.signatures
"""

import radical.utils.signatures as rus

@rus.takes   (basestring, int, rus.optional (float))
@rus.returns (int)
def sigtest (string, intger, float=3.1415926) :
    return 1

def test_signatures () :
    """ Test if signature violations are flagged """ 

    try :
        ret = sigtest ('string', 2.4, 'hallo')
        assert False, "should have seen a TypeError exception"
    except TypeError as e :
        assert True
    except Exception as e : 
        assert False, "should have seen a TypeError exception, not %s" % e


    try :
        ret = sigtest ('string', 2, 1.1414)
        assert True
    except Exception as e : 
        assert False, "should have seen no exception, found %s" % e



