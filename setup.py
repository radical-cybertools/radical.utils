
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess

from setuptools              import setup, Command
from distutils.command.sdist import sdist


#-----------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - short_version:  1.2.3 - is used for installation
#   - long_version:  v1.2.3-9-g0684b06  - is used as runtime (ru.version)
#   - both are derived from the last git tag
#   - the file radical/utils/VERSION is created with the long_version, und used
#     by ru.__init__.py to provide the runtime version information. 
#
def get_version():

    short_version = None  # 0.4.0
    long_version  = None  # 0.4.0-9-g0684b06

    try:
        import subprocess as sp
        import re

        VERSION_MATCH = re.compile (r'(([\d\.]+)\D.*)')

        # attempt to get version information from git
        p   = sp.Popen (['git', 'describe', '--tags', '--always'],
                        stdout=sp.PIPE, stderr=sp.STDOUT)
        out = p.communicate()[0]


        if  p.returncode != 0 or not out :

            # the git check failed -- its likely that we are called from
            # a tarball, so use ./VERSION instead
            out=open (os.path.dirname (os.path.abspath (__file__)) + "/VERSION", 'r').read().strip()


        # from the full string, extract short and long versions
        v = VERSION_MATCH.search (out)
        if v:
            long_version  = v.groups ()[0]
            short_version = v.groups ()[1]


        # sanity check if we got *something*
        if  not short_version or not long_version :
            sys.stderr.write ("Cannot determine version from git or ./VERSION\n")
            import sys
            sys.exit (-1)


        # make sure the version file exists for the runtime version inspection
        open ('radical/utils/VERSION', 'w').write (long_version+"\n")


    except Exception as e :
        print 'Could not extract/set version: %s' % e
        import sys
        sys.exit (-1)

    return short_version, long_version

short_version, long_version = get_version ()

#-----------------------------------------------------------------------------
# check python version. we need > 2.5, <3.x
if  sys.hexversion < 0x02050000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("SAGA requires Python 2.x (2.5 or higher)")


#-----------------------------------------------------------------------------
class our_test(Command):
    def run(self):
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = subprocess.call([sys.executable, 
                                   '%s/run_tests.py'          % testdir,
                                   '%s/configs/basetests.cfg' % testdir])
        raise SystemExit(retval)


#-----------------------------------------------------------------------------
setup_args = {
    'name'             : "radical.utils",
    'version'          : short_version,
    'description'      : "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'long_description' : "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'author'           : "The RADICAL Group",
    'author_email'     : "andre@merzky.net",
    'maintainer'       : "Andre Merzky",
    'maintainer_email' : "andre@merzky.net",
    'url'              : "https://www.github.com/saga-project/radical.utils/",
    'license'          : "MIT",
    'classifiers'      : [
        'Development Status   :: 5 - Production/Stable',
        'Intended Audience    :: Developers',
        'Environment          :: Console',                    
        'Programming Language :: Python',
        'License              :: OSI Approved :: MIT License',
        'Topic                :: Utilities',
        'Topic                :: System :: Distributed Computing',
        'Topic                :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System     :: MacOS :: MacOS X',
        'Operating System     :: POSIX',
        'Operating System     :: Unix'
    ],
    'packages' : [
        "radical",
        "radical.utils",
        "radical.utils.config",
        "radical.utils.plugins",
        "radical.utils.plugins.unittests",
        "radical.utils.logger",
        "radical.utils.contrib",
    ],
    'zip_safe'             : False,
    'scripts'              : [],
    'package_data'         : {'' : ['VERSION']},
    'cmdclass'             : {
        'test'         : our_test,
      # 'sdist'        : our_sdist,
    },
    'install_requires' : ['setuptools', 'colorama'],
    'tests_require'    : ['setuptools', 'nose'],
}

#-----------------------------------------------------------------------------

setup(**setup_args)

#-----------------------------------------------------------------------------

