# pylint: disable=reimported

# allow star import, unused symbols
# flake8: noqa: F401

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

# ------------------------------------------------------------------------------
#
import os            as _os


# ------------------------------------------------------------------------------
#
# we want atfork imported first, specifically before os and logging
#
from .atfork         import *

# import constants
from .constants      import *

# import utility classes
from .object_cache   import ObjectCache
from .plugin_manager import PluginManager, PluginBase
from .singleton      import Singleton
from .heartbeat      import Heartbeat, PWatcher
from .threads        import is_main_thread, is_this_thread, cancel_main_thread
from .threads        import main_thread, this_thread, get_thread_name, gettid
from .threads        import set_cancellation_handler, unset_cancellation_handler
from .threads        import raise_in_thread, ThreadExit, SignalRaised
from .futures        import Future
from .futures        import NEW, RUNNING, DONE, FAILED, CANCELED
from .url            import Url
from .host           import get_hostip, get_hostname
from .host           import get_hostlist, get_hostlist_by_range
from .host           import create_hostfile, compress_hostlist, is_localhost
from .lockable       import Lockable
from .lockfile       import Lockfile
from .registry       import Registry, READONLY, READWRITE
from .ru_regex       import ReString, ReSult
from .lease_manager  import LeaseManager
from .daemon         import Daemon, daemonize
from .poll           import Poller, POLLIN, POLLOUT, POLLERR, POLLALL
from .poll           import POLLNVAL, POLLPRI, POLLHUP
from .shell          import sh_quote
from .shell          import sh_callout, sh_callout_bg, sh_callout_async
from .testing        import sys_exit
from .testing        import TestConfig
from .testing        import set_test_config, add_test_config, get_test_config
from .env            import env_read, env_write, env_read_lines, env_eval
from .env            import env_prep, env_diff, EnvProcess, env_dump
from .stack          import stack
from .modules        import import_module, find_module, import_file
from .modules        import get_type, load_class

from .dict_mixin     import DictMixin, dict_merge, dict_stringexpand, dict_diff
from .dict_mixin     import PRESERVE, OVERWRITE, iter_diff
from .typeddict      import TypedDict, TypedDictMeta, as_dict
from .config         import Config, DefaultConfig

from .zmq            import Message
from .zmq            import Bridge
from .zmq            import Queue,  Putter,    Getter
from .zmq            import PubSub, Publisher, Subscriber
from .zmq            import Server, Client

from .flux           import FluxHelper

from .logger         import DEBUG, INFO, WARNING, WARN, ERROR, CRITICAL, OFF
from .logger         import Logger
from .reporter       import Reporter
from .profile        import Profiler, timestamp, event_to_label
from .profile        import read_profiles, combine_profiles, clean_profile
from .profile        import TIME, EVENT, COMP, TID, UID, STATE, MSG, ENTITY
from .profile        import PROF_KEY_MAX

from .json_io        import read_json, read_json_str, write_json
from .json_io        import parse_json, parse_json_str, dumps_json
from .which          import which
from .tracer         import trace, untrace
from .get_version    import get_version

from .serialize      import to_json, from_json, to_msgpack, from_msgpack
from .serialize      import register_serializable


# import various utility methods
from .ids            import *
from .debug          import *
from .misc           import *
from .algorithms     import *

# import decorators
from .timing         import timed_method, epoch, dt_epoch, Time

# import sub-modules
from . import scheduler
from . import config
from . import zmq


# ------------------------------------------------------------------------------
#
# get version info
#
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = get_version(_mod_root)
version      = version_short
__version__  = version_detail


# ------------------------------------------------------------------------------

