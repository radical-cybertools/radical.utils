
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys

import read_json as rj


# ------------------------------------------------------------------------------
#
_test_config = dict()


# ------------------------------------------------------------------------------
#
def get_test_config():
    return _test_config


# ------------------------------------------------------------------------------
#
def set_test_config(cfg):
    global _test_config
    _test_config = cfg


# ------------------------------------------------------------------------------
#
class TestConfig(dict):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg_file, cfg_name=None):

        self._cfg = rj.read_json(cfg_file)

        if cfg_name and cfg_name in self._cfg:
            self._cfg = self._cfg[cfg_name]

        dict.__init__(self, self._cfg)

        self.__initialized = True  # see self.__setattr__()

        set_test_config(self)

    # --------------------------------------------------------------------------
    #
    def __getattr__(self, item):
        """
        default to None instead of raising
        """
        try:
            return self.__getitem__(item)
        except KeyError:
            return None

    # --------------------------------------------------------------------------
    #
    def __setattr__(self, item, value):
        """Maps attributes to values.
        Only if we are initialised
        """

        # allow attributes to be set in the __init__ method
        if '_D__initialized' in self.__dict__:
            return dict.__setattr__(self, item, value)

        # known attributes
        elif item in self.__dict__:
            dict.__setattr__(self, item, value)

        # default
        else:
            self.__setitem__(item, value)


# ------------------------------------------------------------------------------
#
class Testing(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, modname, basedir):
        """
        The given basedir is assumed to be `tests/` beneath the module tree to
        be tested.  We further assume a directory `tests/unittests/` to exist
        and to contain the actual set of test scripts.
        """

        self._testdir = "%s/unittests/" % os.path.dirname(os.path.realpath(basedir))

        try:
            module = __import__(modname)
        
        except Exception, e:
            srcdir = "%s/../" % os.path.dirname(os.path.realpath(basedir))
            sys.path.insert(0, os.path.abspath(srcdir))
            module = __import__(modname)


        print "______________________________________________________________________"
        print "Using %s from: %s" % (modname, module.__path__[0])
        print "______________________________________________________________________"

        try:
            import nose
        except ImportError:
            msg  = " \n\nnose is not available -- install radical.utils with: \n\n"
            msg += "  (1) pip install --upgrade -e '.[nose]'\n"
            msg += "  (2) pip install --upgrade    'radical.utils[nose]'\n\n"
            msg += "to resolve that dependency (or install nose manually).\n"
            msg += "The first version will work for local installation, \n"
            msg += "the second one for installation from pypi.\n\n"
            raise ImportError (msg)



    # --------------------------------------------------------------------------
    #
    def run(self, cfg=None):
        """
        This method runs a set of unit tests, which are organized in a directory
        structure under ``tests/unittests/`` -- each sub-directory in that
        hierarchy represents a test suite to be run.

        The unit tests in the individual test suites will have access to the
        same configuration info (parameter test_config), and will use the given
        parameters to set up the test environment.  For example, the saga-python
        api/job/test_service.py unit test will use the following::

            import saga.utils.test_config as sutc
            ...

            tc = sutc.TestConfig('test.cfg')
            js = saga.job.Service(tc['js_url'], tc['session'])

        The :class:`saga.utils.test_config.TestConfig` class will expose the currently
        active test configuration -- which is activated in the *run_unittests* script
        as follows::

            tc = sutc.TestConfig(cfg_name)

        Since :class:`saga.utils.test_config.TestConfig` is
        a :class:`saga.utils.singleton.Singleton`, the thusly set state will be shared
        with the test suites as shown.

        """

        import nose

        if cfg: tc = cfg
        else  : tc = get_test_config()

        results = list()

        # fall back to no subtree structure if no test suites are specified
        if not tc.get('test_suites'):
            tc['test_suites'] = ['.']

        verbosity_env  = os.environ.get('NOSE_VERBOSE', 
                         os.environ.get('RADICAL_VERBOSE', "3"))
        verbosity_nose = {'1': 1, 'ERROR'   : 1,
                          '2': 2, 'WARNING' : 2,
                          '3': 3, 'INFO'    : 3, 
                          '4': 4, 'DEBUG'   : 4}[verbosity_env]

        # run all test suites from the config
        for test_suite in tc['test_suites']:

            # configure the unit test framework
            config = nose.config.Config()

            config.verbosity  = verbosity_nose
            config.workingDir = self._testdir + '/' + test_suite
            config.stream     = sys.stderr

            # and run tests
            print "____________________________________________________________"
            print "%s" % test_suite
            print "____________________________________________________________"
            rc = nose.core.run(argv=[sys.argv[0]], config=config)
            results.append(rc)
            print "RC: %s" % rc
            print "____________________________________________________________"

        # if we get a 'false' results it means that something went wrong. in
        # that case we return a non-zero exit code
        if False in results: return -1
        else               : return  0


# --------------------------------------------------------------------

