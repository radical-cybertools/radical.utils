
import builtins
import inspect
import os
import importlib

from typing import Any, Union, Optional

_id_cnt = 0


# ------------------------------------------------------------------------------
#
# to keep RU 2.6 compatible, we provide import_module which works around some
# quirks of __import__ when being used with dotted names. This is what the
# python docs recommend to use.  This basically steps down the module path and
# loads the respective submodule until arriving at the target.
#
def import_module(name):

    mod = __import__(name)
    for s in name.split('.')[1:]:
        mod = getattr(mod, s)
    return mod


# ------------------------------------------------------------------------------
#
# as import_module, but without the import part :-P
#
def find_module(name):

    package = importlib.util.find_spec(name)

    if not package:
        return None

    if '_NamespaceLoader' in str(package.loader):
        # since Python 3.5, loaders differ between modules and namespaces
        return package._path._path[0]                    # pylint: disable=W0212
    else:
        return os.path.dirname(package.loader.get_filename())


# ------------------------------------------------------------------------------
#
# a helper to load functions and classes from user provided source file which
# are *not* installed as modules.  All symbols from that file are loaded, and
# returned is a dictionary with the following structure:
#
# symbols = {'classes'  : {'Foo': <class 'mod_0001.Foo'>,
#                          'Bar': <class 'mod_0001.Bar'>,
#                          ...
#                         },
#            'functions': {'foo': <function foo at 0x7f532d241d40>,
#                          'bar': <function bar at 0x7f532d241d40>,
#                          ...
#                         }
#           }
#
def import_file(path):

    global _id_cnt
    _id_cnt += 1

    uid  = 'mod_%d' % _id_cnt
    spec = importlib.util.spec_from_file_location(uid, path)
    mod  = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(mod)

    symbols = {'functions': dict(),
               'classes'  : dict()}

    for k,v in mod.__dict__.items():
        if not k.startswith('__'):
            if inspect.isclass(v):    symbols['classes'  ][k] = v
            if inspect.isfunction(v): symbols['functions'][k] = v

    return symbols


# ------------------------------------------------------------------------------
#
def get_type(type_name: str) -> Optional[type]:
    '''
    get a type object from a type name (str)
    '''

    # check builtin types
    ret = getattr(builtins, type_name, None)
    if isinstance(ret, type):
        return ret

    # check global types
    ret = globals().get(type_name)
    if isinstance(ret, type):
        return ret

    # check local types of the calling frame
    ret = inspect.currentframe().f_back.f_locals.get(type_name)
    if isinstance(ret, type):
        return ret


# ------------------------------------------------------------------------------
#
def load_class(fpath: str,
               cname: str,
               ctype: Optional[Union[type,str]] = None) -> Optional[Any]:
    '''
    load class `cname` from a source file at location `fpath`
    and return it (the class, not an instance).
    '''

    if not os.path.isfile(fpath):
        raise ValueError('no source file at [%s]' % fpath)

    pname  = os.path.splitext(os.path.basename(fpath))[0]
    spec   = importlib.util.spec_from_file_location(pname, fpath)
    plugin = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(plugin)

    ret = getattr(plugin, cname)

    if ctype:

        if isinstance(ctype, str):
            ctype_name = ctype
            ctype = get_type(ctype_name)

            if not ctype:
                raise ValueError('cannot type check %s' % ctype_name)

        if not issubclass(ret, ctype):
            return None

    return ret


# ------------------------------------------------------------------------------

