
__author__    = "RADICAL Team"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess as sp

from setuptools import setup, Command

name     = 'radical.utils'
mod_root = 'radical/utils'

#-----------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - version:          1.2.3            - is used for installation
#   - version_detail:  v1.2.3-9-g0684b06 - is used for debugging
#   - version is read from VERSION file in src root, which is on installation
#     copied into the module dir.
#   - version_detail is derived from the git tag, and only available when
#     installed from git -- this is stored in VERSION.git, in the same
#     locations, on install.
#   - both files, VERSION and VERSION.git are used to provide the runtime 
#     version information. 
#
def get_version (mod_root):
    """
    mod_root
        a VERSION and VERSION.git file containing the version strings is created
        in mod_root, during installation.  Those files are used at runtime to
        get the version information.

    """

    try:

        version        = None
        version_detail = None

        # get version from './VERSION'
        src_root = os.path.dirname (__file__)
        if  not src_root :
            src_root = '.'

        with open (src_root + "/VERSION", "r") as f :
            version = f.readline ().strip()


        # attempt to get version detail information from git
        p   = sp.Popen ('cd %s ; '\
                        'tag=`git describe --tags --always` 2>/dev/null ; '\
                        'branch=`git branch | grep -e "^*" | cut -f 2 -d " "` 2>/dev/null ; '\
                        'echo $tag@$branch'  % src_root,
                        stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
        version_detail = p.communicate()[0].strip()

        if  p.returncode   !=  0  or \
            version_detail == '@' or \
            'fatal'        in version_detail :
            version_detail =  "v%s" % version

        print 'version: %s (%s)'  % (version, version_detail)


        # make sure the version files exist for the runtime version inspection
        path = "%s/%s" % (src_root, mod_root)
        print 'creating %s/VERSION' % path

        with open (path + "/VERSION",     "w") as f : f.write (version        + "\n") 
        with open (path + "/VERSION.git", "w") as f : f.write (version_detail + "\n")

        return version, version_detail

    except Exception as e :
        raise RuntimeError ("Could not extract/set version: %s" % e)


#-----------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail = get_version (mod_root)


#-----------------------------------------------------------------------------
# check python version. we need > 2.6, <3.x
if  sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("%s requires Python 2.x (2.6 or higher)" % name)


#-----------------------------------------------------------------------------
class our_test(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = sp.call([sys.executable,
                          '%s/run_tests.py'               % testdir,
                          '%s/configs/default.cfg'        % testdir])
        raise SystemExit(retval)


#-----------------------------------------------------------------------------
#
def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()


#-----------------------------------------------------------------------------
setup_args = {
    'name'               : name,
    'namespace_packages' : ["radical"],
    'version'            : version,
    'description'        : "Shared code and tools for various Radical Group (http://radical.rutgers.edu) projects.",
    'long_description'   : (read('README.md') + '\n\n' + read('CHANGES.md')),    
    'author'             : 'RADICAL Group at Rutgers University',
    'author_email'       : "radical@rutgers.edu",
    'maintainer'         : "Andre Merzky",
    'maintainer_email'   : "andre@merzky.net",
    'url'                : "https://www.github.com/radical-cybertools/radical.utils/",
    'license'            : "MIT",
    'keywords'           : "radical pilot job saga",
    'classifiers'        : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : [
        "radical",
        "radical.utils",
        "radical.utils.config",
        "radical.utils.plugins",
        "radical.utils.plugins.unittests_1",
        "radical.utils.plugins.unittests_2",
        "radical.utils.logger",
        "radical.utils.contrib",
    ],
    'scripts'            : ['bin/dump_mongodb.py'],
    'package_data'       : {'' : ['*.sh', 'VERSION', 'VERSION.git']},
    'cmdclass'           : {
        'test'           : our_test,
    },
    'install_requires'   : ['colorama', 'pymongo', 'nose'],
    'tests_require'      : [],
    'zip_safe'           : False,
#   'build_sphinx'       : {
#       'source-dir'     : 'docs/',
#       'build-dir'      : 'docs/build',
#       'all_files'      : 1,
#   },
#   'upload_sphinx'      : {
#       'upload-dir'     : 'docs/build/html',
#   }
}

#-----------------------------------------------------------------------------

setup (**setup_args)

#-----------------------------------------------------------------------------

