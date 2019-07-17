#!/usr/bini/env python

__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import json
import tempfile

import radical.utils as ru


# ------------------------------------------------------------------------------
#
_state = 0


# ------------------------------------------------------------------------------
def test_read_json():
    '''
    Test json parser
    '''

    # --------------------------------------------------------------------------
    def _write_json(data):

        data    = str.encode(data)
        comment = b'# comments are ignored, right?\n'

        [tmpfile, tmpname] = tempfile.mkstemp()
        os.write(tmpfile, comment)
        os.write(tmpfile, data)
        return tmpname

    # --------------------------------------------------------------------------
    def _read_json(filename):

        try    : return ru.read_json(filename)
        finally: os.unlink(filename)

    # --------------------------------------------------------------------------
    # initial state
    data = {'test_1': 1,
            'test_2': 'one',
            'test_3': [1, 'one']}

    filename  = _write_json(json.dumps(data))
    data_copy = _read_json(filename)

    assert(data_copy)

    for key in data:
        assert(key in data_copy)
        assert(data[key] == data_copy[key])

    for key in data_copy:
        assert(key in data)
        assert(data[key] == data_copy[key])


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_read_json()


# ------------------------------------------------------------------------------

