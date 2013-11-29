
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip.
"""

import os
import sys

from setuptools import setup, Command
from distutils.command.install_data import install_data
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

#-----------------------------------------------------------------------------
# check python version. we need > 2.5
if sys.hexversion < 0x02050000:
    raise RuntimeError("radical.utils requires Python 2.5 or higher")

#-----------------------------------------------------------------------------
# 
class our_install_data(install_data):

    def finalize_options(self): 
        self.set_undefined_options ('install',
                                    ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)

#-----------------------------------------------------------------------------
# 
class our_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)

class our_test(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys
        import subprocess
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        errno = subprocess.call([sys.executable, '%s/run_tests.py' % testdir,
                                '--config=%s/configs/basetests.cfg' % testdir])
        raise SystemExit(errno)


short_version, long_version = get_version ()

setup_args = {
    'name': "radical.utils",
    'version': short_version,
    'description': "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'long_description': "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'author': "The RADICAL Group",
    'author_email': "andre@merzky.net",
    'maintainer': "Andre Merzky",
    'maintainer_email': "andre@merzky.net",
    'url': "https://www.github.com/saga-project/radical.utils/",
    'license': "MIT",
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: AIX',
        'Operating System :: POSIX :: BSD',
        'Operating System :: POSIX :: BSD :: BSD/OS',
        'Operating System :: POSIX :: BSD :: FreeBSD',
        'Operating System :: POSIX :: BSD :: NetBSD',
        'Operating System :: POSIX :: BSD :: OpenBSD',
        'Operating System :: POSIX :: GNU Hurd',
        'Operating System :: POSIX :: HP-UX',
        'Operating System :: POSIX :: IRIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: Other',
        'Operating System :: POSIX :: SCO',
        'Operating System :: POSIX :: SunOS/Solaris',
        'Operating System :: Unix'
    ],
    'packages': [
        "radical",
        "radical.utils",
        "radical.utils.config",
        "radical.utils.plugins",
        "radical.utils.plugins.unittests",
        "radical.utils.logger",
        "radical.utils.contrib",
    ],
    'package_data': {'': ['*.sh', 'radical/utils/VERSION']},
    'zip_safe': False,
    'scripts': [],
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("radical/utils/VERSION", [])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist,
        'test': our_test
    },
    'install_requires': ['setuptools', 'colorama'],
    'tests_require': ['setuptools', 'nose']
}

setup(**setup_args)
