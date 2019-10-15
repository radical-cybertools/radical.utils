
__author__    = 'Radical.Utils Development Team (Andre Merzky, Ole Weidner)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


# import radical.utils.signatures as rus


# # ----------------------------------------------------------------------------
# @rus.takes   (basestring, int, rus.optional (float))
# @rus.returns (int)
# def sigtest (string, intger, float=3.1415926) :
#     return 1
#
# # ----------------------------------------------------------------------------
# def test_signatures () :
#     '''
#     Test if signature violations are flagged
#     '''
#
#     try                  : ret = sigtest ('string', 2.4, 'hallo')
#     except TypeError as e: pass
#     except Exception as e: assert(0), 'TypeError != %s (%s)' % (type(e), e)
#     else                 : assert(0), 'expected TypeError exception, got none'
#
#     try                  : ret = sigtest ('string', 2, 1.1414)
#     except Exception as e: assert(0), 'exception %s: %s' % (type(e), e)
#
#
# # ----------------------------------------------------------------------------
# # run tests if called directly
# if __name__ == '__main__':
#
#     test_signatures ()
#
#
# # ----------------------------------------------------------------------------

