

import sys


# ------------------------------------------------------------------------------
#
class Reporter (object) :

    # Define terminal colors for the reporter
    HEADER  = '\033[95m'
    INFO    = '\033[94m'
    OK      = '\033[92m'
    WARN    = '\033[93m'
    ERROR   = '\033[91m'
    ENDC    = '\033[0m'

    DOTTED_LINE = '........................................................................\n'
    SINGLE_LINE = '------------------------------------------------------------------------\n'
    DOUBLE_LINE = '========================================================================\n'
    HASHED_LINE = '########################################################################\n'

    # --------------------------------------------------------------------------
    #
    def __init__ (self, title=None) :

        self._title = title

        if  self._title :
            self._out (HEADER, "\n")
            self._out (HEADER, HASHED_LINE)
            self._out (HEADER, "%s\n" % title)
            self._out (HEADER, HASHED_LINE)
            self._out (HEADER, "\n")
    

    # --------------------------------------------------------------------------
    #
    def __del__ (self, title=None) :

        if  self._title :
            self._out (HEADER, "\n")
            self._out (HEADER, HASHED_LINE)
            self._out (HEADER, "\n")
    

    # --------------------------------------------------------------------------
    #
    def _out (color, msg) :
        sys.stdout.write (color)
        sys.stdout.write (msg)
        sys.stdout.write (ENDC)
    

    # --------------------------------------------------------------------------
    #
    def header (self, msg) :
        self._out (HEADER, "\n\n%s\n" % msg)
        self._out (HEADER, DOUBLE_LINE)


    # --------------------------------------------------------------------------
    #
    def info (self, msg) :
        self._out (INFO, "\n%s\n" % msg)
        self._out (INFO, SINGLE_LINE)


    # --------------------------------------------------------------------------
    #
    def ok (self, msg) :
        self._out (OK, "%s\n" % msg)


    # --------------------------------------------------------------------------
    #
    def warn (self, msg) :
        self._out (WARN, "%s\n" % msg)


    # --------------------------------------------------------------------------
    #
    def error (self, msg) :
        self._out (ERROR, "%s\n" % msg)


# ------------------------------------------------------------------------------

