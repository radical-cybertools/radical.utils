#!/usr/bini/env python

__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import json
import pytest
import tempfile

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def _write_json(data):

    data    = str.encode(data)
    comment = b'# comments are ignored, right?\n'

    [tmpfile, tmpname] = tempfile.mkstemp()
    os.write(tmpfile, comment)
    os.write(tmpfile, data)
    return tmpname


# ------------------------------------------------------------------------------
#
def _write_raw(data):

    [tmpfile, tmpname] = tempfile.mkstemp()
    os.write(tmpfile, data)
    return tmpname


# ------------------------------------------------------------------------------
def test_read_json():
    '''
    Test json parser
    '''

    # --------------------------------------------------------------------------
    # default xcase
    data = {'test_1': 1,
            'test_2': 'one',
            'test_3': [1, 'one']}

    filename  = _write_json(json.dumps(data))
    data_copy = ru.read_json(filename)

    assert(data_copy)

    for key in data:
        assert(key in data_copy)
        assert(data[key] == data_copy[key])

    for key in data_copy:
        assert(key in data)
        assert(data[key] == data_copy[key])


    # ---------------------------------------------------------------------------
    # string read
    data_copy = ru.read_json_str(filename)
    assert(isinstance(data_copy['test_2'], str))


    # ---------------------------------------------------------------------------
    # arg switching
    ru.write_json(filename, data_copy)
    ru.write_json(data_copy, filename)
    data_copy = ru.read_json_str(filename)
    assert(len(data_copy) == 3)

    os.unlink(filename)


    # --------------------------------------------------------------------------
    # manual parse
    data = '''{
                  "test_1": 1,
                  "test_2": "one",
                  "test_3": [1, "one"]
              }'''
    data_copy = ru.parse_json(data, filter_comments=False)
    assert(len(data_copy) == 3)
    assert(data_copy['test_2'] == 'one')


    # --------------------------------------------------------------------------
    # forced str conversion on manual parse
    data_copy = ru.parse_json_str(data)
    assert(len(data_copy) == 3)
    assert(isinstance(data_copy['test_2'], str))


    # ---------------------------------------------------------------------------
    # faulty json file
    filename = _write_raw(b'{"foo": [False]}')
    with pytest.raises(ValueError):
        ru.read_json(filename)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_read_json()


# ------------------------------------------------------------------------------

