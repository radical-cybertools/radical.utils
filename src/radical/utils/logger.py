
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import logging
import threading
import colorama

from .misc import import_module


# ------------------------------------------------------------------------------
#
class ColorStreamHandler(logging.StreamHandler):
    """ 
    A colorized output SteamHandler 
    """

    colours = {'DEBUG'    : colorama.Fore.CYAN,
               'INFO'     : colorama.Fore.GREEN,
               'WARN'     : colorama.Fore.YELLOW,
               'WARNING'  : colorama.Fore.YELLOW,
               'ERROR'    : colorama.Fore.RED,
               'CRITICAL' : colorama.Back.RED + colorama.Fore.WHITE
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, target):

        logging.StreamHandler.__init__(self, target)
        self._tty  = self.stream.isatty()
        self._term = getattr(self, 'terminator', '\n')

    # --------------------------------------------------------------------------
    #
    def emit(self, record):

        # only write in color when using a tty
        if self._tty:
            self.stream.write(self.colours[record.levelname] \
                             + self.format(record)           \
                             + colorama.Style.RESET_ALL      \
                             + self._term)
        else:
            self.stream.write(self.format(record) + self._term)

      # self.flush()



# ------------------------------------------------------------------------------
#
def getLogger(name, tag=None):
    # deprecated, but retained for backward compatinbility
    return get_logger(name)

def get_logger(name, target=None, level=None):
    """
    Get a logging handle.

    'name'   is used to identify log entries on this handle.
    'target' is a comma separated list (or Python list) of specifiers, where
             specifiers are:
             '-'      : stdout
             '1'      : stdout
             'stdout' : stdout
             '='      : stderr
             '2'      : stderr
             'stderr' : stderr
             '.'      : logfile named ./<name>.log
             <string> : logfile named <string>
    'level'  log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """

    logger = logging.getLogger(name)
    logger.propagate = False   # let messages not trickle upward

    if logger.handlers:
        # we already conifgured that logger in the past -- just reuse it
        return logger

    # --------------------------------------------------------------------------
    # unconfigured loggers get configured.  We try to get the log level and
    # target from the environment.  We try env vars like this:
    #     name  : radical.saga.pty
    #     level : RADICAL_SAGA_PTY_VERBOSE
    #             RADICAL_SAGA_VERBOSE
    #             RADICAL_VERBOSE
    #     target: RADICAL_SAGA_PTY_LOG_TARGET
    #             RADICAL_SAGA_LOG_TARGET
    #             RADICAL_LOG_TARGET
    # whatever is found first is applied.  Well, we actually try the way around
    # and then apply the last one -- but whatever ;)  
    # Default level  is 'CRITICAL', 
    # default target is '-' (stdout).


    env_name = name.upper().replace('.', '_')
    elems    = env_name.split('_')

    if not level:
        level  = 'CRITICAL'
        for i in range(1,len(elems)):
            env_test = '_'.join(elems[:i+1]) + '_VERBOSE'
            level    = os.environ.get(env_test, level).upper()

    # backward compatible interpretation of SAGA_VERBOSE
    if env_name.startswith('RADICAL_SAGA'):
        level = os.environ.get('SAGA_VERBOSE', level).upper()

    level_warning = None
    if level not in ['DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'CRITICAL']:
        level_warning = "log level '%s' not supported -- reset to 'ERROR'" % level
        level = 'ERROR'

    if not target:
        target = '-'
        for i in range(1,len(elems)):
            env_test = '_'.join(elems[:i+1]) + '_LOG_TARGET'
            target   = os.environ.get(env_test, target)

    if not isinstance(target, list):
        target = target.split(',')

    formatter = logging.Formatter('%(asctime)s: ' \
                                  '%(name)-20s: ' \
                                  '%(processName)-32s: ' \
                                  '%(threadName)-15s: ' \
                                  '%(levelname)-8s: ' \
                                  '%(message)s')

    # add a handler for each targets (using the same format)
    for t in target:
        if t in ['-', '1', 'stdout']:
            handle = ColorStreamHandler(sys.stdout)
        elif t in ['=', '2', 'stderr']:
            handle = ColorStreamHandler(sys.stderr)
        elif t in ['.']:
            handle = logging.StreamHandler("./%s.log" % name)
        else:
            handle = logging.FileHandler(t)
        handle.setFormatter(formatter)
        logger.addHandler(handle)

    logger.setLevel(level)

    if level_warning:
        logger.error(level_warning)

    # if the given name points to a version or version_detail, log those
    try:
        logger.info("%-20s version: %s", 'python.interpreter', ' '.join(sys.version.split()))
        tmp = import_module(name)
        if hasattr(tmp, 'version_detail'):
            logger.info("%-20s version: %s", name, getattr(tmp, 'version_detail'))
        elif hasattr(tmp, 'version'):
            logger.info("%-20s version: %s", name, getattr(tmp, 'version'))
    except:
        pass

    try:
        logger.info("%-20s pid: %s", '', os.getpid())
        logger.info("%-20s tid: %s", '', threading.current_thread().name)
    except:
        pass

    return logger


# -----------------------------------------------------------------------------

