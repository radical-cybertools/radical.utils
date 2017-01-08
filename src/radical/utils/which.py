
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os

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
    else       : return ret[0]


# ------------------------------------------------------------------------------

