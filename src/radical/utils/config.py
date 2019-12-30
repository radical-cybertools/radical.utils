
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
# We provide a json based config file parser with following properties
#
#   - system config files will be merged with user configs (if those exist)
#   - python style comments are filtered out before parsing
#   - after parsing, `${ABC:default}`-style values are set or expanded via
#     `os.environ`
#   - the returned class exposes settings as dicts or attributes
#     cfg['foo']['bar'] == cfg.foo.bar
#
#
# Config Names and Locations
# --------------------------
#
# We assume two basic locations for config files: one is installed within the
# scope of a Python module, the other one is under user control, and usually in
# the users home directory.  The  config reader accepts the following parameters
# to derive the exact locations:
#
#   - module: name of module under which the config is installed
#   - path  : config file path relative to the module home
#   - name  : config file name relative to the path
#
# The `module` string is interpreted as follows:
#
#   m = __import__('module')
#   sys_config_dir = "%s/configs" % os.path.dirname(m.__file__)
#   usr_config_dir = "%s/.%s/"    % (os.environ['HOME'], m.replace('.', '/'))
#
# so the location of the module's `__init__.py` is used to derive the location
# of the installed system config files, and the module name is used to derive
# the location of the user provided config files.
#
# For example, the module `radical.utils` will have the following config dirs:
#
#   sys_config_dir = /tmp/ve/lib/python2.7/site-packages/radical/utils/configs/
#   usr_config_dir = /home/merzky/.radical/utils/
#
# After loading the system level config file, any existing user level config
# file is merged into it, via
#
#   radical.utils.dict_merge(user_cgf, system_cfg, mode='overwrite')
#
# so that the user config settings supercede the system config settings.
#
# Both path and name specifiers can contain `*` as wildcard, which is then
# interpreted as by `glob()`.  If that wildcard exist, then all matching config
# files are read into *one* configuration dict, where each root key is set to
# the value the '*' expands to (minus the `.json` extension).
#
# For example, the name `radical.pilot.resource_*` with the following config
# files:
#
#   /tmp/ve/[...]/radical/pilot/configs/resource_xsede.json
#   /tmp/ve/[...]/radical/pilot/configs/resource_ncsa.json
#
# will result in a config dict like:
#
#   {
#     'xsede' : { 'foo' : 'bar' },
#     'ncsa'  : { 'fiz' : 'baz' }
#   }
#
#
# Queries
# -------
#
# We support two types of queries on the resulting parsed configs:
#
#   - dict like queries (via `ru.DictMixin`)
#   - the `query(key)` method returns a single value, or 'None' if not found.
#
# In the latter `query()` case, the `key` can be specified as dot-separated
# path, so that the following two snippets are equivalent (assuming that a
# `foo.bar` section exists):
#
#   val = cfg['foo']['bar'].get('baz')
#   val = cfg.query('foo.bar.baz')
#
#
# Environment
# -----------
#
# Towards `os.environ` completion, we support the following syntax in all string
# *values* (not keys):
#
#   '${RADICAL_UTILS_ENV:default_value}
#
# which will be replaced by
#
#   `os.environ.get('RADICAL_UTILS_ENV', 'default_value')`
#
# The default value is optional, an empty string is used if no default value is
# given.  Env evaluation is only performed at time of parsing, not at time of
# query.  RU attempts to convert env variables to float and int - if that fails,
# values are stored as strings.
#
#
# Validation
# ----------
#
# It probably makes sense to switch to a json schema validator at some point,
# see for example https://pypi.python.org/pypi/json-schema-validator. For now
# this implementation remains schema-less, and will thus, in a very pythonesque
# way, only fail once values are queried or used.
#
#
# Implementation
# --------------
#
# This implementation is based on typed dictionaries which are accessed as
# `munch`'ed object hierarchy.
#
# ------------------------------------------------------------------------------

import os
import glob
import munch

from .debug      import find_module
from .misc       import is_string
from .misc       import expand_env as ru_expand_env
from .json_io    import read_json, write_json
from .dict_mixin import dict_merge

from .singleton  import Singleton


# ------------------------------------------------------------------------------
#
class Config(munch.Munch):

    # FIXME: we should cache config files after reading, so that repeated
    #        instance creations do not trigger a new (identical) round of
    #        parsing.

    # --------------------------------------------------------------------------
    #
    def __init__(self, module=None, category=None, name=None, cfg=None,
                       path=None, expand=True, env=None, _internal=False):
        '''
        Load a config (json) file from the module's config tree, and overload
        any user specific config settings if found.

        module:   used to determine the module's config file location
                  - default: `radical.utils`
        category: name of config to be loaded from module's config path
        name:     specify a specific configuration to be used
        cfg:      application configuration to be used for initialization
        path:     path to app configuration to be used for initialization
        expand:   enable / disable environment var expansion
                  - default: True
        env:      environment dictionary to be used for expansion
                  - default: `os.environ`

        The naming of config files follows this rule:

          `<category>_<name>.json`

        For example, if the following is used in a system python installation:

            ru.Config('radical.pilot', category='session', name='minimal')

        it would attempt to load (depending on system details):

            /usr/lib/python3/site-packages/radical/pilot/\
                                                   config/session_mininmal.json

        NOTE: Keys containing an underscore are not exposed via the API.
              Keys containing dots are split and interpreted as paths in the
              configuration hierarchy.
        '''

        self._expand = expand
        self._env    = env

        if path and cfg:
            raise ValueError('conflicting initializers (path, cfg)')

        if path:
            cfg = read_json(path)

        # if a category has dot limited elements and no module is given,
        # interpret the first part as module
        # radical.pilot.session -> [radical.pilot, session]
        if not module and category and '.' in category:
            elems    = category.split('.')
            module   = '.'.join(elems[:-1])
            category = elems[-1]

        # do the same if module is given but not the category
        if not category and module and '.' in module:
            elems    = module.split('.')
            module   = '.'.join(elems[:-1])
            category = elems[-1]

        # if a category starts with a module prefix, strip that prefix
        if category and category.startswith('%s.' % module):
            category = category[len(module) + 1:]

        name_orig = name
        if not name:
            # by default, load the default config
            name = 'default'

        # purge module and category parts from name
        if module and name.startswith(module + '.'):
            name = name[len(module) + 1:]

        if category and name.startswith(category + '.'):
            name = name[len(category) + 1:]

        if not cfg:
            # just use config files
            cfg = dict()

        if name.startswith('/'):

            # load config from abs path
            sys_fspec = name
            usr_fspec = None

            if '*' in name: starred = True
            else          : starred = False

        elif module and category:

            # construct cfg file paths
            modpath = find_module(module)
            if not modpath:
                raise ValueError("Cannot find module %s" % module)

            home    = os.environ.get('HOME', '/tmp')
            home    = os.environ.get('RADICAL_CONFIG_USER_DIR', home)
            sys_dir = "%s/configs"     % (modpath)
            usr_dir = "%s/.%s/configs" % (home, module.replace('.', '/'))
            fname   = '%s_%s.json'     % (category.replace('.', '/'), name)

            sys_fspec = '%s/%s' % (sys_dir, fname)
            usr_fspec = '%s/%s' % (usr_dir, fname)

            if '*' in fname: starred = True
            else           : starred = False
            self._tmp = [starred, fname]

        else:
            # we can't load a config file - just use the app config
            sys_fspec = None
            usr_fspec = None
            starred   = False

        sys_cfg = dict()
        usr_cfg = dict()
        app_cfg = cfg

        if _internal:
            # no need to look at the FS, just convert the given cfg dict
            sys_fspec = None
            usr_fspec = None

        nfiles = 0
        if not starred:

            if sys_fspec:

                sys_fname = sys_fspec

                if not os.path.isfile(sys_fname):
                    sys_fname += '.json'

                if os.path.isfile(sys_fname):
                    sys_cfg = read_json(sys_fname)
                    nfiles += 1

            if usr_fspec:

                usr_fname = usr_fspec

                if not os.path.isfile(usr_fname):
                    usr_fname += '.json'

                if os.path.isfile(usr_fname):
                    usr_cfg = read_json(usr_fname)
                    nfiles += 1

        else:

            # wildcard mode: whatever the '*' expands into is used as root dict
            # entry, and the respective content of the config file is stored
            # underneath it.
            if sys_fspec:

                postfix_len = len('.json')                      # ' .json'
                prefix_len  = len(sys_fspec) - postfix_len - 1  # '*.json'

                for sys_fname in glob.glob(sys_fspec):

                    base = sys_fname[prefix_len:-postfix_len]
                    scfg = read_json(sys_fname)
                    sys_cfg[base] = scfg
                    nfiles += 1

            if usr_fspec:

                postfix_len = len('.json')                      # ' .json'
                prefix_len  = len(usr_fspec) - postfix_len - 1  # '*.json'

                for usr_fname in glob.glob(usr_fspec):

                    base = usr_fname[prefix_len:-postfix_len]
                    ucfg = read_json(usr_fname)
                    usr_cfg[base] = ucfg
                    nfiles += 1

        # if we did not find *any* file, and the original `name` was None,
        # then try to load config files w/o name
        # Example: if there is no `registry_default.json`, then try to load
        # `registry.json`.
        if nfiles == 0 and name_orig is None and not _internal and category:

            fname     = '%s.json' % (category.replace('.', '/'))
            sys_fname = '%s/%s'   % (sys_dir, fname)
            usr_fname = '%s/%s'   % (usr_dir, fname)

            if os.path.isfile(sys_fname): sys_cfg = read_json(sys_fname)
            if os.path.isfile(usr_fname): usr_cfg = read_json(usr_fname)


        # merge sys, usr and app cfg before expansion
        cfg_dict = dict()
        cfg_dict = dict_merge(cfg_dict, sys_cfg, policy='overwrite')
        cfg_dict = dict_merge(cfg_dict, usr_cfg, policy='overwrite')
        cfg_dict = dict_merge(cfg_dict, app_cfg, policy='overwrite')

        if cfg_dict:

            # ------------------------------------------------------------------
            def to_config(data):
                for k,v in data.items():
                    if isinstance(v, dict):
                        data[k] = Config(cfg=v, expand=False, _internal=True)
                return data
            # ------------------------------------------------------------------

            self.update(to_config(cfg_dict))

        if expand:
            ru_expand_env(self, env=env)


    # --------------------------------------------------------------------------
    #
    # cfg['foo']        == cfg.foo
    # cfg['foo']['bar'] == cfg.foo.bar
    #
    def __getattr__(self, k):
        return self.get(k, None)

    def __setattr__(self, k, v):

        if self._expand:
            ru_expand_env(v, env=self._env)

        if isinstance(v, dict):
            self[k] = Config(cfg=v, expand=False)
        else:
            self[k] = v


    # --------------------------------------------------------------------------
    #
    # don't list private class attributes (starting with `_`) as dict entries
    #
    def __iter__(self):
        for k in dict.__iter__(self):
            if str(k)[0] != '_':
                yield k

    def items(self):
        for k in dict.__iter__(self):
            if str(k)[0] != '_':
                yield k, self[k]

    def keys(self):
        return [x for x in self]

    def __len__(self):
        return len(self.keys())


    # --------------------------------------------------------------------------
    #
    def merge(self, cfg, expand=True, env=None, policy='overwrite', log=None):
        '''
        merge the given config into the existing config settings, overwriting
        any values which already existed
        '''

        dict_merge(self, cfg, policy=policy, log=log)

        if expand:
            ru_expand_env(cfg, env=env)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def __repr__(self):
        return str(self)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):

        return self.toDict()


    # --------------------------------------------------------------------------
    #
    def write(self, fname):

        write_json(self.as_dict(), fname)


    # --------------------------------------------------------------------------
    #
    def query(self, key, default=None):
        '''
        For a query like

            config.query('some.path.to.key', 'foo')

        this method behaves like:

            config['some']['path']['to'].get('key', default='foo')
        '''

        if is_string(key): elems = key.split('.')
        else             : elems = key

        if not elems:
            raise ValueError('empty key on query')

        pos  = self
        path = list()
        for elem in elems:

            if not isinstance(pos, dict):
                raise KeyError('no such key [%s]' % '.'.join(path))

            if elem in pos: pos = pos[elem]
            else          : pos = None

            path.append(elem)

        if pos is None:
            pos = default

        return pos


# ------------------------------------------------------------------------------
#
class DefaultConfig(Config, metaclass=Singleton):
    '''
    The settings in this default config are, unsurprisingly, used as default
    values for various RU classes and methods, as for example for log file
    locations, log levels, profile locations, etc.
    '''

    def __init__(self):

        cfg = {'log_lvl'    : '${RADICAL_DEFAULT_LOG_LVL:ERROR}',
               'log_tgt'    : '${RADICAL_DEFAULT_LOG_TGT:.}',
               'log_dir'    : '${RADICAL_DEFAULT_LOG_DIR:$PWD}',
               'report'     : '${RADICAL_DEFAULT_REPORT:TRUE}',
               'report_tgt' : '${RADICAL_DEFAULT_REPORT_TGT:stderr}',
               'report_dir' : '${RADICAL_DEFAULT_REPORT_DIR:$PWD}',
               'profile'    : '${RADICAL_DEFAULT_PROFILE:TRUE}',
               'profile_dir': '${RADICAL_DEFAULT_PROFILE_DIR:$PWD}',
               }

        super(DefaultConfig, self).__init__(module='radical.utils', cfg=cfg)


# ------------------------------------------------------------------------------


