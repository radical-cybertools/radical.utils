
import os
import inspect
import pkgutil

import importlib.util


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

    package = pkgutil.get_loader(name)

    if not package:
        return None

    if '_NamespaceLoader' in str(package):
        # since Python 3.5, loaders differ between modules and namespaces
        return package._path._path[0]                    # pylint: disable=W0212
    else:
        return os.path.dirname(package.get_filename())


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

