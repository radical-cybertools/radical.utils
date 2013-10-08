
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
# figure out the current version
def update_version():

    version = "latest"

    try:
        cwd = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(cwd, 'radical/utils/VERSION')
        version = open(fn).read().strip()
    except IOError:
        from subprocess import Popen, PIPE, STDOUT
        import re

        VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

        try:
            p = Popen(['git', 'describe', '--tags', '--always'],
                      stdout=PIPE, stderr=STDOUT)
            out = p.communicate()[0]

            if (not p.returncode) and out:
                v = VERSION_MATCH.search(out)
                if v:
                    version = v.group()
        except OSError:
            pass

    return version

#-----------------------------------------------------------------------------
# check python version. we need > 2.5
if sys.hexversion < 0x02050000:
    raise RuntimeError("radical.utils requires Python 2.5 or higher")

#-----------------------------------------------------------------------------
# 
class our_install_data(install_data):

    def finalize_options(self): 
        self.set_undefined_options('install',
                                   ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)
        # ensure there's a radical/utils/VERSION file
        fn = os.path.join(self.install_dir, 'radical/utils', 'VERSION')
        open(fn, 'w').write(update_version())
        self.outfiles.append(fn)

#-----------------------------------------------------------------------------
# 
class our_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)

        fn = os.path.join(base_dir, 'radical/utils', 'VERSION')
        open(fn, 'w').write(update_version())


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


setup_args = {
    'name': "radical.utils",
    'version': update_version(),
    'description': "A collectyion of python utilities for the various radical projects.",
    'long_description': "A collectyion of python utilities for the various radical projects.",
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
    ],
    'package_data': {'': ['*.sh']},
    'zip_safe': False,
    'scripts': [],
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("radical.utils", [])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist,
        'test': our_test
    },
    'install_requires': ['setuptools', 'colorama'],
    'tests_require': ['setuptools', 'nose']
}

setup(**setup_args)
