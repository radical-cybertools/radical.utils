
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess
from   setuptools  import setup, Command


#-----------------------------------------------------------------------------
# setup.py can use ru version detection, as ru do not import any other
# dependencies (which would otherwise be specified only below, ie.. chicken/egg)
import radical.utils as ru
short_version, version, branch = ru.short_version, ru.version, ru.branch


#-----------------------------------------------------------------------------
# check python version. we need > 2.5, <3.x
if  sys.hexversion < 0x02050000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("Radical.Utils requires Python 2.x (2.5 or higher)")


#-----------------------------------------------------------------------------
class our_test(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = subprocess.call([sys.executable, 
                                   '%s/run_tests.py'        % testdir,
                                   '%s/configs/default.cfg' % testdir])
        raise SystemExit(retval)


#-----------------------------------------------------------------------------
#
def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()


#-----------------------------------------------------------------------------
setup_args = {
    'name'             : "radical.utils",
    'version'          : short_version,
    'description'      : "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),    
    'author'           : 'RADICAL Group at Rutgers University',
    'author_email'     : "radical@rutgers.edu",
    'maintainer'       : "Andre Merzky",
    'maintainer_email' : "andre@merzky.net",
    'url'              : "https://www.github.com/saga-project/radical.utils/",
    'license'          : "MIT",
    'keywords'         : "radical pilot job saga",
    'classifiers'      : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
    ],
    'packages'         : [
        "radical",
        "radical.utils",
        "radical.utils.config",
        "radical.utils.plugins",
        "radical.utils.plugins.unittests_1",
        "radical.utils.plugins.unittests_2",
        "radical.utils.logger",
        "radical.utils.contrib",
    ],
    'scripts'          : ['bin/dump_mongodb.py', 
                          'bin/radical_copyright.pl',
                         ],
    'package_data'     : {'' : ['*.sh', 'VERSION']},
    'cmdclass'         : {
        'test'         : our_test,
    },
    'install_requires' : ['colorama', 'pymongo'],
    'tests_require'    : ['nose'],
    'zip_safe'         : False,
}

#-----------------------------------------------------------------------------

setup (**setup_args)

#-----------------------------------------------------------------------------

