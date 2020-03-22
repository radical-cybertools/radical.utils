#!/usr/bin/env python3

import pytest
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class _TestDescr(ru.Description):

    _schema = {'exe'  : str,
               'procs': int,
               'env'  : {str: int},
               'pre'  : [str],
               'post' : (str, ),
               'meta' : None,
               'items': [int],
               'pop'  : [int],
               '_foo' : str,
               '_data': str,
               'unset': int,
    }

    def _verify(self):

        if 'exe' not in self:
            raise ValueError('exe not defined')


# ------------------------------------------------------------------------------
#
def test_description():

    from_dict = {'procs': '3',
                 'env'  : {3: '4'},
                 'pre'  : True,
                 'post' : False,
                 'meta' : 3.4,
                 'items': [3.4, '3'],
                 'pop'  : [3.4, '3'],
                 '_foo' : ['bar'],
                 '_data': 4,
    }

    td = _TestDescr()
    td = _TestDescr(from_dict=from_dict)

    with pytest.raises(ValueError):
        td.verify()

    td.exe = '/bin/date'
    assert(td.verify())

    assert(isinstance(td.exe,   str))
    assert(isinstance(td.procs, int))
    assert(isinstance(td.env,   dict))
    assert(isinstance(td.pre,   list))
    assert(isinstance(td.pop,   list))
    assert(isinstance(td._foo,  str))                                     # noqa

    assert(isinstance(td['_data'], str))
    assert(isinstance(td['items'], list))

    assert(td.meta)

    assert(td.exe      == '/bin/date')
    assert(td.procs    == 3)
    assert(td.env      == {'3': 4})
    assert(td.pre      == ['True'])
    assert(td.post     == ('False',))
    assert(td.pop      == [3, 3])
    assert(td._foo     == "['bar']")                                      # noqa

    assert(td['_data'] == '4')
    assert(td['items'] == [3, 3])

    assert(td.meta     == 3.4)

    import copy
    c = copy.deepcopy(td)
    assert(c.as_dict() == td.as_dict())


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_description()


# ------------------------------------------------------------------------------

