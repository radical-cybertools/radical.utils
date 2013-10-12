

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
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

