
import os
import sys
import glob

from .misc    import as_list
from .shell   import sh_callout
from .modules import import_module


# ------------------------------------------------------------------------------
#
def stack(ns='radical'):
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
          }

    for namespace in as_list(ns):

        ret[namespace] = dict()

        ns_mod = import_module(namespace)
        path   = ns_mod.__path__
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
                mname = '%s.%s' % (namespace, mbase)

                if mbase.startswith('_'):
                    continue

                try:
                    ret[namespace][mname] = import_module(mname).version_detail
                except Exception as e:
                    if '%s_DEBUG' % namespace.upper() in os.environ:
                        ret[namespace][mname] = str(e)
                    else:
                        ret[namespace][mname] = '?'

    return ret


# ------------------------------------------------------------------------------

