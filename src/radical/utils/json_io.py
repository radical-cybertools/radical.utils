# pylint: disable=unused-import,import-error,no-name-in-module,eval-used,unspecified-encoding

__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import re

from .serialize import to_json, from_json
from .misc      import as_string, ru_open


# ------------------------------------------------------------------------------
#
def read_json(fname, filter_comments=True):
    '''
    Comments line in the form of

        # some json data or text

    are stripped from json before parsing:

        import pprint
        pprint.pprint(read_json("my_file.json"))
    '''

    with ru_open(fname) as f:

        try:
            output = parse_json(f.read(), filter_comments)
        except ValueError as e:
            raise ValueError('error parsing %s: %s' % (fname, e)) from e

    return output


# ------------------------------------------------------------------------------
#
def read_json_str(filename, filter_comments=True):
    '''
    same as read_json, but converts unicode strings and byte arrays to ASCII
    strings.
    '''

    return as_string(read_json(filename, filter_comments))


# ------------------------------------------------------------------------------
#
def write_json(data, fname):
    '''
    thin wrapper around python's json write, for consistency of interface

    '''

    if isinstance(fname, dict) and isinstance(data, str):
        # arguments were switched: accept anyway
        tmp   = data
        data  = fname
        fname = tmp

    with ru_open(fname, 'w') as f:
        f.write(to_json(data))
        f.write('\n')
        f.flush()


# ------------------------------------------------------------------------------
#
def dumps_json(data):
    '''
    thin wrapper around python's json write, for consistency of interface

    '''

    return to_json(data)


# ------------------------------------------------------------------------------
#
def parse_json(json_str, filter_comments=True):
    '''
    Comment lines in the form of

        # some json data or text

    are stripped from json before parsing
    '''

    if filter_comments:
        json_str = '\n'.join([re.sub(r'^\s*#.*$', '', line)
                         for line in json_str.split('\n')])

    return from_json(json_str)


# ------------------------------------------------------------------------------
#
def parse_json_str(json_str):
    '''
    same as parse_json, but converts unicode strings to simple strings
    '''

    return as_string(parse_json(json_str))


# ------------------------------------------------------------------------------

