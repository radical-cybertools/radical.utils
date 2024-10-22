

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
"""
Using the RU logging module can lead to deadlocks when used in multiprocess and
multithreaded environments.  This is due to the native python logging library
not being thread save.  See [1] for a Python bug report on the topic from 2009.
As of 2016, a patch is apparently submitted, but not yet accepted, for Python
3.5.  So we have to solve the problem on our own.

The fix is encoded in the 'after_fork()' method.  That method *must* be called
immediately after a process fork.  A fork will carry over all locks from the
parent process -- but it will not carry over any threads.  That means that, if
a fork happens while a thread owns the lock, the child process will start with
a locked lock -- but since there are no threads, there is actually nobody who
can unlock the lock, thus badaboom.

Since after the fork we *know* the logging locks should be unset (after all, we
are not in a logging call right now, are we?), we pre-emtively unlock them here.
But, to do so we need to know what locks exist in the first place.  For that
purpose, we create a process-singleton of all the loggers we hand out via
'get_logger()'.

Logging locks are 'threading.RLock' instances.  As such they can be locked
multiple times (from within the same thread), and we have to unlock them that
many times.  We use a shortcut, and create a new, unlocked lock.
"""


# ------------------------------------------------------------------------------
#
# NOTE: ForkingPickler does not like lambdas nor local functions, thus use
#       a module level function to disable loggers.
#
def _noop(*args, **kwargs):
    pass


# ------------------------------------------------------------------------------
#
import os
import sys
import threading
import colorama
import logging

from typing import Dict

from   .atfork    import atfork
from   .misc      import get_env_ns       as ru_get_env_ns
from   .modules   import import_module    as ru_import_module
from   .config    import DefaultConfig
from   .singleton import Singleton


CRITICAL = logging.CRITICAL
ERROR    = logging.ERROR
WARNING  = logging.WARNING
WARN     = logging.WARNING
INFO     = logging.INFO
DEBUG    = logging.DEBUG
OFF      = 60


# ------------------------------------------------------------------------------
#
class _LoggerRegistry(object, metaclass=Singleton):

    from .singleton import Singleton

    def __init__(self):
        self._registry = list()

    def add(self, logger):
        self._registry.append(logger)

    def release_all(self):
        for logger in self._registry:
            while logger:
                for handler in logger.handlers:
                    handler.lock = threading.RLock()
                  # handler.reset()
                logger = logger.parent

    def close_all(self):

        for logger in self._registry:
            while logger:
                for handler in logger.handlers:
                    handler.close()
                    logger.removeHandler(handler)
                logger = logger.parent
        self._registry = list()


# ------------------------------------------------------------------------------
#
_logger_registry = _LoggerRegistry()


# ------------------------------------------------------------------------------
def _after_fork():

    _logger_registry.release_all()
    logging._lock = threading.RLock()         # pylint: disable=protected-access


# ------------------------------------------------------------------------------
#
def _atfork_prepare():
    pass


# ------------------------------------------------------------------------------
#
def _atfork_parent():
    pass


# ------------------------------------------------------------------------------
#
def _atfork_child():
    _after_fork()


# ------------------------------------------------------------------------------
#
atfork(_atfork_prepare, _atfork_parent, _atfork_child)


# ------------------------------------------------------------------------------
#
class ColorStreamHandler(logging.StreamHandler):
    """
    A colorized output SteamHandler
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, target):

        logging.StreamHandler.__init__(self, target)
        self._tty    = self.stream.isatty()
        self._term   = getattr(self, 'terminator', '\n')
        self.colours = {'DEBUG'    : colorama.Fore.CYAN,
                        'INFO'     : colorama.Fore.GREEN,
                        'WARN'     : colorama.Fore.YELLOW,
                        'WARNING'  : colorama.Fore.YELLOW,
                        'ERROR'    : colorama.Fore.RED,
                        'CRITICAL' : colorama.Back.RED + colorama.Fore.WHITE,
                        'RESET'    : colorama.Style.RESET_ALL + self._term
                       }


    # --------------------------------------------------------------------------
    #
    def emit(self, record):

        # only write in color when using a tty
        if self._tty:
            self.stream.write('%s%s%s' % (self.colours[record.levelname],
                                          self.format(record),
                                          self.colours['RESET']))
        else:
            self.stream.write(self.format(record) + self._term)
        self.stream.flush()


# ------------------------------------------------------------------------------
#
class FSHandler(logging.FileHandler):

    def __init__(self, target):

        try:
            os.makedirs(os.path.abspath(os.path.dirname(target)))
        except:
            pass  # exists

        logging.FileHandler.__init__(self, target, delay=True)


# ------------------------------------------------------------------------------
#
class Logger(object):

    '''
    Logger documentation
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, ns=None, path=None, targets=None, level=None,
                 debug=None, verbose=False):
        """
        Get a logging handle.

        `name`    is used to identify log entries on this handle.
        `ns`      is used as environment name space to check for the log level
        `targets` is a comma separated list (or Python list) of specifiers,
                  where specifiers are:

                    `0`      : /dev/null
                    `null`   : /dev/null
                    `-`      : stdout
                    `1`      : stdout
                    `stdout` : stdout
                    `=`      : stderr
                    `2`      : stderr
                    `stderr` : stderr
                    `.`      : logfile named ./<name>.log
                    <string> : logfile named <string>

        `path`    file system location to write logfiles to (created as needed)
        `level`   log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, OFF)
        `debug`   debug level (0-9)

        If `ns` is, for example, set to `radical.utils`, then the following
        environment variables are evaluated:

            RADICAL_UTILS_LOG_LVL
            RADICAL_LOG_LVL

            RADICAL_UTILS_LOG_TGT
            RADICAL_LOG_TGT

        The first found variable of each pair is then used for the respective
        settings.
        """

        if name is None:
            raise ValueError('logger name must be specified and not `None`')

        self._name        = name
        self._ns          = ns
        self._path        = path
        self._targets     = targets
        self._level       = level
        self._debug_level = debug
        self._debug       = debug
        self._verbose     = verbose
        self._num_level   = 0
        self._logger      = None


    # --------------------------------------------------------------------------
    #
    def _ensure_handler(self):

        if self._logger:
            return

        ru_def = DefaultConfig()

        # make sure handlers are attached.  If they are not, attach them!
        self._logger = logging.getLogger(self._name)
        self._logger.propagate = False   # let messages not trickle upward
        self._logger.name      = self._name

        # otherwise configure this logger
        if not self._path:
            self._path = ru_def['log_dir']

        if not self._ns:
            self._ns = self._name

        if not self._targets:
            self._targets = ru_get_env_ns('log_tgt', self._ns)
            if not self._targets:
                self._targets = ru_def['log_tgt']

        if isinstance(self._targets, str):
            self._targets = self._targets.split(',')

        if not isinstance(self._targets, list):
            self._targets = [self._targets]

        if not self._level:
            self._level = ru_get_env_ns('log_lvl', self._ns)

        if not self._level:
            # backward compatibility
            self._level = ru_get_env_ns('verbose', self._ns)

        if not self._level:
            self._level = ru_def['log_lvl']

        if self._level in [OFF, 'OFF']:
            self._targets = ['null']

        try:
            self._level = int(self._level)
        except:
            pass

        self._debug_level = self._debug or 0
        if isinstance(self._level, int):
            if self._level < 10:
                self._debug_level = 10 - self._level
                self._level = 'DEBUG'

        elif self._level.upper().startswith('DEBUG_'):
            self._debug_level = int(self._level.split('_', 1)[1])
            self._level = 'DEBUG'

        if self._level != 'DEBUG':
            self._debug_level = 0


        # translate numeric levels into upper case symbolic ones
        levels   = {'60' : 'OFF',
                    '50' : 'CRITICAL',
                    '40' : 'ERROR',
                    '30' : 'WARNING',
                    '20' : 'INFO',
                    '10' : 'DEBUG',
                     '0' :  ru_def['log_lvl']}


        self._level   = levels.get(str(self._level), str(self._level)).upper()
        self._warning = None
        if self._level not in list(levels.values()):
            self._warning = "invalid loglevel '%s', use '%s'" \
                                      % (self._level, ru_def['log_lvl'])
            self._level   = ru_def['log_lvl']

        formatter = logging.Formatter('%(created).3f : '
                                      '%(name)-20s : '
                                      '%(process)-5d : '
                                      '%(thread)-5d : '
                                      '%(levelname)-8s : '
                                      '%(message)s')

      # print('%-30s -> %-10s %d' % (name, level, debug_level))

        # add a handler for each targets (using the same format)
        if not self._logger.handlers:
            p = self._path
            n = self._name
            for t in self._targets:

                if   t in ['0', 'null']       : h = logging.NullHandler()
                elif t in ['-', '1', 'stdout']: h = ColorStreamHandler(sys.stdout)
                elif t in ['=', '2', 'stderr']: h = ColorStreamHandler(sys.stderr)
                elif t in ['.']               : h = FSHandler("%s/%s.log" % (p, n))
                elif t.startswith('/')        : h = FSHandler(t)
                else                          : h = FSHandler("%s/%s"     % (p, t))

                h.setFormatter(formatter)
                h.name = self._logger.name
                self._logger.addHandler(h)

            if self._level != 'OFF':
                self._logger.setLevel(self._level)

            if self._warning:
                self._logger.warning(self._warning)

        # if `name` points to module, try to log its version info
        if self._verbose:

            self._ensure_handler()

            try:
                self._logger.info("%-20s version: %s", 'python.interpreter',
                                  ' '.join(sys.version.split()))

                mod = ru_import_module(self._name)
                if hasattr(mod, 'version_detail'):
                    self._logger.info("%-20s version: %s",
                                      self._name, getattr(mod, 'version_detail'))

                elif hasattr(mod, 'version'):
                    self._logger.info("%-20s version: %s",
                                      self._name, getattr(mod, 'version'))
            except:
                pass

            # also log pid and tid
            try:
                self._logger.info("%-20s pid/tid: %s/%s", '', os.getpid(),
                            threading.current_thread().name)
            except:
                pass


        # keep the handle a round, for cleaning up on fork
        _logger_registry.add(self._logger)

        self._numerics : Dict[str, int] = {'off'     : 60,
                                           'critical': 50,
                                           'error'   : 40,
                                           'warning' : 30,
                                           'info'    : 20,
                                           'debug'   : 10}

        # store properties
        self._num_level = self._numerics.get(self._level.lower(), 0)

        if self._level == 'DEBUG':
            self._num_level -= self._debug_level

        # backward compatibility
        self._logger.warn = self._logger.warning

        # treat `self.debug_1()`, `self.debug_2()` etc. the same as
        # `self.debug()` if the respective `debug_level` is set, and ignore
        # otherwise.  All other unknown method calls are forwarded to the nativ
        # logger instance.  This is basically inheritance, but since the logger
        # class has no constructor, we do it this way.

        # disable all loggers
        self.critical = _noop
        self.error    = _noop
        self.warn     = _noop
        self.warning  = _noop
        self.info     = _noop
        self.debug    = _noop
        self.debug_1  = _noop
        self.debug_2  = _noop
        self.debug_3  = _noop
        self.debug_4  = _noop
        self.debug_5  = _noop
        self.debug_6  = _noop
        self.debug_7  = _noop
        self.debug_8  = _noop
        self.debug_9  = _noop

        # enable the ones we are configured for:
        if self._num_level <= 50: self.critical = self._logger.critical
        if self._num_level <= 40: self.error    = self._logger.error
        if self._num_level <= 30: self.warn     = self._logger.warn
        if self._num_level <= 30: self.warning  = self._logger.warning
        if self._num_level <= 20: self.info     = self._logger.info
        if self._num_level <= 10: self.debug    = self._logger.debug
        if self._num_level <=  9: self.debug_1  = self._logger.debug
        if self._num_level <=  8: self.debug_2  = self._logger.debug
        if self._num_level <=  7: self.debug_3  = self._logger.debug
        if self._num_level <=  6: self.debug_4  = self._logger.debug
        if self._num_level <=  5: self.debug_5  = self._logger.debug
        if self._num_level <=  4: self.debug_6  = self._logger.debug
        if self._num_level <=  3: self.debug_7  = self._logger.debug
        if self._num_level <=  2: self.debug_8  = self._logger.debug
        if self._num_level <=  1: self.debug_9  = self._logger.debug



    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name

    @property
    def ns(self):
        return self._ns

    @property
    def path(self):
        return self._path

    @property
    def level(self):
        return self._level

    @property
    def debug_level(self):
        return self._debug_level

    @property
    def num_level(self):
        return self._num_level

    @property
    def targets(self):
        return self._targets


    # --------------------------------------------------------------------------
    #
    # All unknown method calls are forwarded to the nativ logger instance.
    # This is basically inheritance, but since the logger class has no
    # constructor, we do it this way.
    #
    def __getattr__(self, name):

        self._ensure_handler()
        try:
            return getattr(self, name)
        except:
            return getattr(self._logger, name)


    # --------------------------------------------------------------------------
    #
    # Add a close method to make sure we can close file handles etc.  This also
    # closes handles on all parent loggers (if those exist).
    #
    def close(self):

        logger = self._logger
        while logger:
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            logger = logger.parent


# ------------------------------------------------------------------------------

