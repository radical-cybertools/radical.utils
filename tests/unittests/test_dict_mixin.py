#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import pytest

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_dict_mixin():
    """
    Test dict mixin
    """

    # --------------------------------------------------------------------------
    @ru.Lockable
    class Test(ru.DictMixin):

        def __init__(self):
            self._d     = dict()
            self['val'] = 1

        def __getitem__(self, key):
            return self._d.__getitem__(key)

        def __setitem__(self, key, val):
            return self._d.__setitem__(key, val)

        def __delitem__(self, key):
            return self._d.__delitem__(key)

        def keys(self):
            return list(self._d.keys())


    # --------------------------------------------------------------------------
    t = Test()

    assert (t['val']      == 1)
    assert (set(t.keys()) == set(['val']))

    assert ('val'       in t)
    assert ('test1' not in t)
    assert ('test2' not in t)

    t['test1'] =  'test'
    t['test2'] = ['test']

    assert ('val'       in t)
    assert ('test1'     in t)
    assert ('test2'     in t)

    assert (t['val']      == 1       )
    assert (t['test1']    == 'test'  )
    assert (t['test2']    == ['test'])
    assert (set(t.keys()) == set(['val', 'test1', 'test2']))

    del t['test1']

    assert (t['val']      == 1       )
    assert (t['test2']    == ['test'])
    assert (set(t.keys()) == set(['val', 'test2']))

    assert ('val'       in t)
    assert ('test1' not in t)
    assert ('test2'     in t)


# ------------------------------------------------------------------------------
#
def test_dict_merge():

    dict_1 = {'key_shared': 'val_shared_1',
              'key_orig_1': 'val_orig_1'}
    dict_2 = {'key_shared': 'val_shared_2',
              'key_orig_2': 'val_orig_2'}

    try:
        ru.dict_merge(dict_1, dict_2)
        assert (False), 'expected ValueError exception'
    except ValueError:
        pass
    except Exception as e:
        assert (False), 'expected ValueError exception, not %s' % e


    ru.dict_merge(dict_1, dict_2, policy='preserve')

    assert (set(dict_1.keys())   == set(['key_shared', 'key_orig_1',
                                                       'key_orig_2']))
    assert (dict_1['key_shared'] == 'val_shared_1')
    assert (dict_1['key_orig_1'] == 'val_orig_1')
    assert (dict_1['key_orig_2'] == 'val_orig_2')


    ru.dict_merge(dict_1, dict_2, policy='overwrite')

    assert (set(dict_1.keys())   == set(['key_shared', 'key_orig_1',
                                                       'key_orig_2']))
    assert (dict_1['key_shared'] == 'val_shared_2')
    assert (dict_1['key_orig_1'] == 'val_orig_1')
    assert (dict_1['key_orig_2'] == 'val_orig_2')


# ------------------------------------------------------------------------------
#
def test_dict_stringexpand():

    target = {'workdir' : '%(home)s/work/',
              'resource': '%(resource)s'}
    source = {'user'    : 'peer_gynt',
              'protocol': 'ssh',
              'host'    : 'localhost',
              'home'    : '/home/%(user)s',
              'resource': '%(protocol)s://%(host)s/'}

    ru.dict_stringexpand(target, source)

    assert (set(target.keys()) == set(['workdir', 'resource']))
    assert (target['workdir']  == '/home/peer_gynt/work/')
    assert (target['resource'] == 'ssh://localhost/')


# ------------------------------------------------------------------------------
#
def test_dict_diff():

    a  = {'foo': 'bar',
          'baz': {'k1': 1,
                  'k2': [2, -2]}
         }
    b  = {'foo': 'bar',
          'baz': {'k1': 1,
                  'k2': [-2],
                  'k3': 3}
         }
    d1 = ru.dict_diff(a, b)
    d2 = {'baz': {'k2': ['len(1) != len(2)'],
                  'k3': {'a': 3}}}

    assert(d1 == d2)

    with pytest.raises(AssertionError):
        ru.iter_diff(a, b)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_dict_diff()
    test_dict_mixin()
    test_dict_merge()
    test_dict_stringexpand()


# ------------------------------------------------------------------------------

