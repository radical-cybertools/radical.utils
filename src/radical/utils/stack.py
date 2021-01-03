
import os
import sys
import glob

from .shell   import sh_callout
from .modules import import_module


# ------------------------------------------------------------------------------
#
def stack():
    '''
    returns a dict with information about the currently active python
    interpreter and all radical modules (incl. version details)
    '''

    exe = sh_callout('which python3', shell=True)[0].strip()
    ret = {'sys'     : {'python'     : exe,
                        'version'    : sys.version.split()[0],
                        'pythonpath' : os.environ.get('PYTHONPATH',  ''),
                        'virtualenv' : os.environ.get('VIRTUAL_ENV', '') or
                                       os.environ.get('CONDA_DEFAULT_ENV','')},
           'radical' : dict()
          }

    import radical
    path = radical.__path__
    if isinstance(path, list):
        path = path[0]

    if isinstance(path, str):
        rpath = path
    else:
        rpath = path._path                               # pylint: disable=W0212

    if isinstance(rpath, list):
        rpath = rpath[0]

    for mpath in glob.glob('%s/*' % rpath):

        if os.path.isdir(mpath):

            mbase = os.path.basename(mpath)
            mname = 'radical.%s' % mbase

            if mbase.startswith('_'):
                continue

            try:
                ret['radical'][mname] = import_module(mname).version_detail
            except Exception as e:
                if 'RADICAL_DEBUG' in os.environ:
                    ret['radical'][mname] = str(e)
                else:
                    ret['radical'][mname] = '?'

    return ret


# ------------------------------------------------------------------------------

