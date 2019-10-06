#!/usr/bin/env python

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_re_string():
    '''
    Test regex matching
    '''

    txt   = ru.ReString ("The quick brown fox jumps over the lazy dog")
    tgt_l = [' qu', 'ick brown fox jumps']
    tgt_d = {'x'  : 'ick brown fox jumps'}

    with txt // r'(\s.u)(?P<x>.*?j\S+)' as res:
        assert (res)
        assert (len(res) == len(tgt_l))
        for a,b in zip(res,tgt_l):
            assert(a == b)
        assert (res      == tgt_l), "%s != %s" % (str(res), str(tgt_l))
        assert (res[0]   == tgt_l[0])
        assert (res[1]   == tgt_l[1])
        assert (res['x'] == tgt_d['x'])
        assert (res.x    == tgt_d['x'])

        for i, r in enumerate (res):
            assert (r    == tgt_l[i])

    if  txt // '(rabbit)':
        assert (False)

    elif  txt // r'((?:\s).{12,15}?(\S+))':
        assert (True)

    else:
        assert (False)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_re_string ()


# ------------------------------------------------------------------------------

