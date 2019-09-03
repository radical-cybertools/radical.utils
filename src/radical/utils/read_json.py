
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import re
import json


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

    return _to_strings(read_json(filename))


# ------------------------------------------------------------------------------
#
def write_json(data, filename):
    '''
    thin wrapper around python's json write, for consistency of interface

    '''


    with open(filename, 'w') as f:
        json.dump(_to_bytes(data), f, sort_keys=True, indent=4, ensure_ascii=False)
        f.write('\n')


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

    return _to_strings(parse_json(json_str))


# ------------------------------------------------------------------------------
#
def _to_strings(data):

    if isinstance(data, dict):
        return {_to_strings(k): _to_strings(v) for k,v in data.items()}

    elif isinstance(data, list):
        return [_to_strings(e) for e in data]

    elif isinstance(data, bytes):
        return bytes.decode(data, 'utf-8')

    else:
        return data


# ------------------------------------------------------------------------------
# thanks to
# http://stackoverflow.com/questions/956867/#13105359
def _to_bytes(data):

    if isinstance(data, dict):
        return {_to_bytes(k): _to_bytes(v) for k,v in data.items()}

    elif isinstance(data, list):
        return [_to_bytes(e) for e in data]

    elif isinstance(data, str):
        return str.encode(data, 'utf-8')

    else:
        return data


# ------------------------------------------------------------------------------

