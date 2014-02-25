
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


# import utility classes
from object_cache   import ObjectCache
from plugin_manager import PluginManager
from singleton      import Singleton
from threads        import Thread, RLock, NEW, RUNNING, DONE, FAILED
from url            import Url
from dict_mixin     import DictMixin, dict_merge, dict_stringexpand
from lockable       import Lockable
from registry       import Registry, READONLY, READWRITE
from regex          import ReString, ReSult
from reporter       import Reporter

# import utility methods
from ids            import generate_id, ID_SIMPLE, ID_UNIQUE
from read_json      import read_json
from read_json      import read_json_str
from read_json      import parse_json
from read_json      import parse_json_str
from tracer         import trace, untrace
from which          import which
from misc           import split_dburl
from get_version    import get_version

# import sub-modules
# from config         import Configuration, Configurable, ConfigOption, getConfig


# ------------------------------------------------------------------------------


import os

pwd     = os.path.dirname (__file__)
root    = "%s/../.." % pwd
short_version, long_version, branch = get_version ([root, pwd])
version = long_version


# ------------------------------------------------------------------------------

