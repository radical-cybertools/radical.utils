
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import re
import json

from .misc import as_string


# ------------------------------------------------------------------------------
#
def read_json(fname):
    '''
    Comments line in the form of

        # some json data or text

    are stripped from json before parsing:

        import pprint
        pprint.pprint(read_json("my_file.json"))
    '''

    with open(fname) as f:

        try:
            return parse_json(f.read())

        except ValueError as e:
            raise ValueError('error parsing %s: %s' % (fname, e))


# ------------------------------------------------------------------------------
#
def read_json_str(filename):
    '''
    same as read_json, but converts unicode strings and byte arrays to ASCII
    strings.
    '''

    return as_string(read_json(filename))


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

    with open(fname, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4, ensure_ascii=False)
        f.write('\n')
        f.flush()


# ------------------------------------------------------------------------------
#
def parse_json(json_str, filter_comments=True):
    '''
    Comment lines in the form of

        # some json data or text

    are stripped from json before parsing
    '''

    if not filter_comments:
        return json.loads(json_str)

    else:
        content = ''
        for line in json_str.split('\n'):
            content += re.sub(r'^\s*#.*$', '', line)
            content += '\n'

        return json.loads(content)


# ------------------------------------------------------------------------------
#
def parse_json_str(json_str):
    '''
    same as parse_json, but converts unicode strings to simple strings
    '''

    return as_string(parse_json(json_str))


# ------------------------------------------------------------------------------

