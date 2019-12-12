
__author__    = 'Radical.Utils Development Team (Andre Merzky, Ole Weidner)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import sys


from .debug   import import_module
from .json_io import read_json


# ------------------------------------------------------------------------------
#
# when running under `pytest`, `sys.exit()` will trigger a test error, even if
# it is an expected call.  Mocking or monkeypatching `sys.exit()` removes the
# error, but will alter semantics so that the test becomes meaningless.  We thus
# switch to `pytest.exit()` if running under pytest.
#
# It is not easy to determine if we are running under pytest.  We thus assume
# that we are the only instance checking for that, and that the `pytest` module
# is imported somewhere in the interpreter before `ru` is imported.
_pytest_active = False

# check if pytest is loaded
for m in sys.modules:
    if 'pytest' in str(m):
        _pytest_active = True
        break

# check if we can import pytest
if _pytest_active:
    try:
        import pytest
    except ImportError:
        _pytest_active = False


def sys_exit(ret):
    '''
    call `pytest.exit(ret)` when running under pytest, `sys.exit(ret) otherwise
    '''

    global _pytest_active

    if _pytest_active:
        pytest.exit(ret)

    else:
        sys.exit(ret)

    assert(False)


# ------------------------------------------------------------------------------
#
# The currently active test configuration
#
_test_config = dict()


# ------------------------------------------------------------------------------
#
def get_test_config():
    '''
    If a test config is currently set, return it.  If not, attempt to load atest
    config for the given namespace, set is as active, and return it.
    '''

    return _test_config


# ------------------------------------------------------------------------------
#
def set_test_config(ns, cfg_name=None, cfg_section=None):
    '''
    Set a test config.  All subsequent calls to `get_test_config()` will
    retrieve the same configuration, until a new config is explicitly set with
    this method again.
    '''

    global _test_config                                  # pylint: disable=W0603
    _test_config = TestConfig(ns, cfg_name, cfg_section)


# ------------------------------------------------------------------------------
#
def add_test_config(ns, cfg_name, cfg_section=None):
    '''
    To an existing active config, add the contents of an additional test
    configuration.
    '''

    if not _test_config:
        raise RuntimeError('cannot add config, no active config is set')

    _test_config.add_config(ns, cfg_name, cfg_section)


# ------------------------------------------------------------------------------
#
class TestConfig(dict):
    '''
    This class represents a set of configurations for a test suite.  It usually
    contains parameters which tweak the tests toward specific environments and
    test cases, such as specific remote systems to interact with, specific
    backends to use, etc.

    The class expects test configurations to be deployed in the current Python
    installation (virtualenv, condaenv, etc.).  The namespace passed on
    construction is used to search for configurations under
    `$namespace/configs/tests/`, configuration files are expected to follow the
    naming scheme `test_$cfg_name.json`.  The json file is read and this object
    then behaves like a dictionary for those json data, but all top level keys
    in the dictionary are also exposed as object attributes.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, ns, cfg_name=None, cfg_section=None):

        if not cfg_name:
            cfg_name = 'default'

        self._ns  = ns
        self._cfg = self._load_config(ns, cfg_name, cfg_section)

        dict.__init__(self, self._cfg)

        self._initialized = True  # see self.__setattr__()


    # --------------------------------------------------------------------------
    #
    def _load_config(self, ns, cfg_name, cfg_section):

        mod   = import_module(ns)
        path  = '%s/configs/tests/' % os.path.dirname(mod.__file__)
        fname = '%s/test_%s.json'   % (path, cfg_name)

        if not os.path.isfile(fname):
            raise ValueError('no such config file %s' % fname)

        cfg = read_json(fname)

        if cfg_section:

            if cfg_section not in cfg:
                raise ValueError('no such config section %s' % cfg_section)

            cfg = cfg[cfg_section]

        return cfg


    # --------------------------------------------------------------------------
    #
    def add_config(self, ns, cfg_name, cfg_section):
        '''
        To the current config dict contents, add the content of the specified
        additional config file
        '''

        if ns != self._ns:
            raise ValueError('cannot merge configs from different name spaces')

        cfg = self._load_config(ns, cfg_name, cfg_section)

        for k,v in cfg.items():
            self._cfg[k] = v

        dict.__init__(self, self._cfg)


    # --------------------------------------------------------------------------
    #
    def __getattr__(self, item):
        '''
        default to None instead of raising
        '''
        try:
            return self.__getitem__(item)

        except KeyError:
            return None


    # --------------------------------------------------------------------------
    #
    def __setattr__(self, item, value):
        '''
        Maps attributes to values
        '''

        # allow attributes to be set in the __init__ method
        if self._initialized:
            return dict.__setattr__(self, item, value)

        # known attributes
        elif item in self.__dict__:
            dict.__setattr__(self, item, value)

        # default
        else:
            self.__setitem__(item, value)


# ------------------------------------------------------------------------------

