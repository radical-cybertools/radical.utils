
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import re
import json


# ------------------------------------------------------------------------------
#
def read_json (filename) :
    """
    Comments in the form of
        # rest of line
    are stripped from json before parsing

    use like this::

        import pprint
        pprint.pprint (read_json (sys.argv[1]))

    """

    with open (filename) as f:

        content = ''

        # weed comments
        for line in f.readlines () :
            content += re.sub (r'#.*', '', line)


        json_data = json.loads (content)
    
        return json_data


# ------------------------------------------------------------------------------


