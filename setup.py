#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2013-23, The RADICAL-Cybertools Team'
__license__   = 'MIT'


''' Setup script, only usable via pip. '''

import re
import os

import subprocess as sp

from glob       import glob
from setuptools import setup, Command, find_namespace_packages


# ------------------------------------------------------------------------------
base     = 'utils'
name     = 'radical.%s'      % base
mod_root = 'src/radical/%s/' % base

scripts  = list(glob('bin/*'))
root     = os.path.dirname(__file__) or '.'
descr    = 'Utilities for RADICAL-Cybertools projects'

data     = [('share/%s/examples/'    % name, glob('examples/*.{py,cfg}'  )),
            ('share/%s/examples/zmq' % name, glob('examples/zmq/*.md'    )),
            ('share/%s/examples/zmq' % name, glob('examples/zmq/queue/*' )),
            ('share/%s/examples/zmq' % name, glob('examples/zmq/pubsub/*'))]


# ------------------------------------------------------------------------------
#
def sh_callout(cmd):
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    stdout, stderr = p.communicate()
    ret            = p.returncode
    return stdout, stderr, ret


# ------------------------------------------------------------------------------
#
def get_version(_mod_root):
    '''
    a VERSION file containes the version strings is created in mod_root,
    during installation.  That file is used at runtime to get the version
    information.
    '''

    out = None
    err = None
    ret = None
    try:
        version_path   = '%s/%s' % (root, _mod_root)
        version_base   = None
        version_detail = None

        # get version_base from './VERSION'
        with open('%s/VERSION' % root, 'r', encoding='utf-8') as fin:
            version_base = fin.readline().strip()

        # get version detail from git
        out, err, ret = sh_callout(
            'cd %s                               && '
            'tag=$(git describe --tags --always) && '
            'branch=$(git branch --show-current) && '
            'echo $tag@$branch' % root)
        version_detail = out.strip()
        version_detail = version_detail.decode()
        version_detail = version_detail.replace('detached from ', 'detached-')
        version_detail = re.sub('[/ ]+', '-', version_detail)
        version_detail = re.sub('[^a-zA-Z0-9_+@.-]+', '', version_detail)

        # make sure the version files exist for the runtime version inspection
        with open(version_path + '/VERSION', 'w', encoding='utf-8') as fout:
            fout.write(version_base   + '\n')
            fout.write(version_detail + '\n')

        return version_base, version_detail, version_path

    except Exception as e:
        msg = 'Could not extract/set version: %s' % e
        if ret:
            msg += '\n' + out + '\n\n' + err
        raise RuntimeError(msg) from e


# ------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail, path = get_version(mod_root)


# ------------------------------------------------------------------------------
#
class RunTwine(Command):
    user_options = []
    def initialize_options(self): pass
    def finalize_options(self):   pass
    def run(self):
        _, _, _ret = sh_callout('python3 setup.py sdist upload -r pypi')
        raise SystemExit(_ret)


# ------------------------------------------------------------------------------
#
with open('%s/requirements.txt' % root, encoding='utf-8') as freq:
    requirements = freq.readlines()


# ------------------------------------------------------------------------------
#
setup_args = {
    'name'               : name,
    'version'            : version,
    'description'        : descr,
    'author'             : 'RADICAL Group at Rutgers University',
    'author_email'       : 'radical@rutgers.edu',
    'maintainer'         : 'The RADICAL Group',
    'maintainer_email'   : 'radical@rutgers.edu',
    'url'                : 'http://radical-cybertools.github.io/%s/' % name,
    'project_urls'       : {
        'Documentation': 'https://radical%s.readthedocs.io/en/latest/' % base,
        'Source'       : 'https://github.com/radical-cybertools/%s/'   % name,
        'Issues' : 'https://github.com/radical-cybertools/%s/issues'   % name,
    },
    'license'            : 'MIT',
    'keywords'           : 'radical utils',
    'python_requires'    : '>=3.7',
    'classifiers'        : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : find_namespace_packages('src', include=['radical.*']),
    'package_dir'        : {'': 'src'},
    'scripts'            : scripts,
    'package_data'       : {'': ['*.txt', '*.sh', '*.json', '*.gz', '*.c',
                                 '*.md', 'VERSION']},
    'install_requires'   : requirements,
    'zip_safe'           : False,
    'data_files'         : data,
    'cmdclass'           : {'upload': RunTwine},
}


# ------------------------------------------------------------------------------
#
setup(**setup_args)


# ------------------------------------------------------------------------------
# clean temporary files from source tree
os.system('rm -vrf src/%s.egg-info' % name)
os.system('rm -vf  %s/VERSION'      % path)


# ------------------------------------------------------------------------------

