
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_dict_mixin () :
    """
    Test dict mixin
    """

    # --------------------------------------------------------------------------
    @ru.Lockable
    class Test (ru.DictMixin) :

        # ----------------------
        def __init__ (self) :

            self._d     = dict ()
            self['val'] = 1

        def __getitem__(self, key):
            return self._d.__getitem__ (key)

        def __setitem__(self, key, val):
            return self._d.__setitem__ (key, val)

        def __delitem__(self, key):
            return self._d.__delitem__ (key)

        def keys(self):
            return self._d.keys ()


    # --------------------------------------------------------------------------
    t = Test ()

    assert (t['val']        == 1       )
    assert (t.keys()        == ['val'] )

    assert ('val'       in t)
    assert ('test1' not in t)
    assert ('test2' not in t)

    t['test1'] =  'test'
    t['test2'] = ['test']

    assert ('val'       in t)
    assert ('test1'     in t)
    assert ('test2'     in t)

    assert (t['val']        == 1       )
    assert (t['test1']      == 'test'  )
    assert (t['test2']      == ['test'])
    assert (t.keys().sort() == ['val', 'test1', 'test2'].sort()), "%s" % str(t.keys())

    del t['test1']

    assert (t['val']        == 1       )
    assert (t['test2']      == ['test'])
    assert (t.keys().sort() == ['val', 'test2'].sort()), "%s" % str(t.keys())

    assert ('val'       in t)
    assert ('test1' not in t)
    assert ('test2'     in t)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_dict_mixin ()

# ------------------------------------------------------------------------------

