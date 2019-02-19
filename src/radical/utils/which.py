
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os


# ------------------------------------------------------------------------------
#
def which(names):
    '''
    Takes a (list of) name(s) and looks for an executable in the path.  It
    will return the first match found, or `None` if none of the given names
    is found. 
    '''

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    if not isinstance(names, list):
        names = [names]

    for name in names:

        if is_exe(name):
            return name

        for path in os.environ.get('PATH', '').split(':'):
            fpath = '%s/%s' % (path, name)
            if is_exe(fpath):
                return fpath

    return None


# ------------------------------------------------------------------------------

