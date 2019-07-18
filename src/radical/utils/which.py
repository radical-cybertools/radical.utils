
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os


# ------------------------------------------------------------------------------
#
def which(program):
    '''
    Accept a string or list of strings specifying executable names.  We search
    for the respective full path of the given executable in `$PATH`, and return
    the first match.  If a single string is given, the match is returned as
    string.  If a list is given, matches are returned as list of the same
    length.  Executables which have not been found in `$PATH` are returned as
    `None`.

    parts are taken from:
    http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
    '''

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    ret      = list()
    ret_list = True
    if not isinstance(program, list):
        ret_list = False
        program  = [program]

    for p in program:

        fpath, fname = os.path.split(p)

        # if the given program has a path component, then we do not search $PATH
        # but check the given complete name
        if fpath:
            if is_exe(p):
                ret.append(p)
            else:
                ret.append(None)
        else:
            found = False
            for path in os.environ["PATH"].split(os.pathsep):
                exe_file = os.path.join(path, p)
                if is_exe(exe_file):
                    found = True
                    ret.append(exe_file)
                    break
            if not found:
                ret.append(None)

    if ret_list: return ret
    if ret     : return ret[0]


# ------------------------------------------------------------------------------
#
def which_of(names):
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

