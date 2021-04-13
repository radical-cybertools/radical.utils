# pylint: disable=unused-import,eval-used

__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import re
import json

from .misc import as_string


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

    with open(fname) as f:

        try:
            return parse_json(f.read(), filter_comments)

        except ValueError as e:
            raise ValueError('error parsing %s: %s' % (fname, e)) from e


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
#
def metric_expand(data):
    '''
    iterate through the given dictionary, and when encountering a key string of
    the form `ru.XYZ` or `rp.ABC`, expand them to their actually defined values.
    This the following dict:

        {
            "ru.EVENT" : "foo"
        }

    becomes:


        {
            2 : "foo"
        }

    '''

    try   : import radical.pilot as rp                              # noqa: F401
    except: pass
    try   : import radical.saga  as rs                              # noqa: F401
    except: pass
    try   : import radical.utils as ru                              # noqa: F401
    except: pass

    if isinstance(data, str):

        if data.count('.') == 1:
            elems = data.split('.')
            if len(elems[0]) == 2 and elems[0][0] == 'r':
                try:
                    data = eval(data)
                finally:

                    pass
        return data

    elif isinstance(data, list):
        return [metric_expand(elem) for elem in data]

    elif isinstance(data, dict):
        return {metric_expand(k) : metric_expand(v) for k,v in data.items()}

    else:
        return data


# ------------------------------------------------------------------------------

