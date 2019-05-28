
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
#   - name  : config file name relative to the module home
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
# The remaining two arguments are exclusive (exactly *one* must be specified).  
# If `path` is given, it is interpreted as a path under those locations.  
# If `name` is given, then the same `. -> /` replacement as on the module name 
# is performed, and the result is interpreted like `path` again.  
#
# In both cases, we add the file extension `.json` if no match is found without 
# it.  It is not an error if the so specified config files do not exist -- in
# that case, the config is considered empty.
#
# After loading the system level config file, any existing user level config
# file is merged into it, via
#
#   radical.utils.dict_merge(user_cgf, system_cfg, mode='overwrite')
#
# so that the user config settings supercede the system config settings.
#
# Both path and name specifiers can contain `*` as wildcard, which is then
# interpreted as by `glob()`.  If that wirldcard exist, then all matching config
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
# query.
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
# ------------------------------------------------------------------------------

import glob
import os

from .misc       import find_module, is_str
from .misc       import expand_env as ru_expand_env
from .read_json  import read_json
from .dict_mixin import dict_merge, DictMixin

from .singleton  import Singleton


# ------------------------------------------------------------------------------
#
class Config(object, DictMixin):

    # FIXME: we should do some magic on values, like, convert to into, float,
    #        bool, list of those, after env expansion.  For now, typing is the
    #        repsonsibility of the consumer.
    # FIXME: we should cache config files after reading, so that repeated
    #        instance creations do not trigger a new (identical) round of
    #        parsing.
    # FIXME: ensure that deepcopy is working (or add `from_dict` c'tor)


    # identify as dictionary
    # FIXME: why is this not inherited from DictMixin?
    # FIXME: we also want to identify as ru.Config!
    @property
    def __class__(self):
        return dict


    # --------------------------------------------------------------------------
    #
    def __init__(self, module, path=None, name=None, cfg=None,
                       expand=True, env=None):
        '''
        expand:  enable / disable environment var expansion.  When disabled, the
                 consumer should expand manually upon use of config entries.
        env:     environment dictionary to be used for expansion
                 defaults to `os.environ`
        '''

        modpath = find_module(module)
        if not modpath:
            raise ValueError("Cannot find module %s" % module)

        home    = os.environ.get('HOME', '/tmp')
        home    = os.environ.get('RADICAL_CONFIG_USER_DIR', home)
        sys_dir = "%s/configs" % (modpath)
        usr_dir = "%s/.%s"     % (home, module.replace('.', '/'))

        if path and name:
            raise ValueError("'path' and 'name' parameters are exclusive")

        # if a name starts with a module prefix, strip that prefix
        if name and name.startswith('%s.' % module):
            name = name[len(module) + 1:]

        # if a path starts with a module prefix, strip that prefix
        if path and path.startswith('%s/' % module.replace('.', '/')):
            path = path[len(module) + 1:]

        if not path and not name:
            # Default to `name='*.json'`
            name = '*.json'

        if not cfg:
            cfg = dict()


        if path: path = path
        else   : path = name.replace('.', '/')

        if '*' in path: starred = True
        else          : starred = False

        if starred and path.count('*') > 1:
            raise ValueError('only one wildcard allowed in config path')

        if path.startswith('/'):
            sys_fspec = path
            usr_fspec = None
        else:
            sys_fspec = '%s/%s' % (sys_dir, path)
            usr_fspec = '%s/%s' % (usr_dir, path)

        app_cfg = cfg
        sys_cfg = dict()
        usr_cfg = dict()

        if not starred:

            if sys_fspec:
                sys_fname = sys_fspec
                if not os.path.isfile(sys_fname): sys_fname += '.json' 
                if     os.path.isfile(sys_fname): sys_cfg = read_json(sys_fname)

            if usr_fspec:
                usr_fname = usr_fspec
                if not os.path.isfile(usr_fname): usr_fname += '.json'
                if     os.path.isfile(usr_fname): usr_cfg = read_json(usr_fname)

        else: 

            # wildcard mode: whatever the '*' expands into is used as root dict
            # entry, and the respective content of the config file is stored
            # underneath it.
            if sys_fspec:

                prefix_len  = sys_fspec.find('*')
                postfix_len = len(sys_fspec) - prefix_len - 1

                for sys_fname in glob.glob(sys_fspec):

                    if postfix_len: base = sys_fname[prefix_len:-postfix_len]
                    else          : base = sys_fname[prefix_len:]

                    scfg = read_json(sys_fname)
                    sys_cfg[base] = scfg


            if usr_fspec:

                prefix_len  = usr_fspec.find('*')
                postfix_len = len(usr_fspec) - prefix_len - 1

                for usr_fname in glob.glob(usr_fspec):
                    base = usr_fname[prefix_len:-postfix_len]
                    ucfg = read_json(usr_fname)
                    usr_cfg[base] = ucfg


        # merge sys, app, and user cfg before expansion
        self._cfg = dict()
        self._cfg = dict_merge(self._cfg, sys_cfg, policy='overwrite')
        self._cfg = dict_merge(self._cfg, app_cfg, policy='overwrite')
        self._cfg = dict_merge(self._cfg, usr_cfg, policy='overwrite')

        if expand:
            self.expand_env(env)


    # --------------------------------------------------------------------------
    #
    def expand_env(self, env):

        # expand environment
        def _expand_env(d):
            if isinstance(d, dict):
                for k,v in d.iteritems():
                    d[k] = _expand_env(v)
            elif isinstance(d, list):
                for i,v in enumerate(d):
                    d[i] = _expand_env(v)
            elif isinstance(d, basestring):
                d = ru_expand_env(d, env)
            return d

        _expand_env(self._cfg)


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        import pprint
        return pprint.pformat(self._cfg)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):

        return self._cfg


    # --------------------------------------------------------------------------
    #
    # first level definitions should be implemented for the dict mixin
    #
    def __getitem__(self, key):
        if key not in self._cfg:
            raise KeyError('no such key [%s]' % key)
        return self._cfg[key]

    def __setitem__(self, key, value):
        self._cfg[key] = value

    def __delitem__(self, key):
        del(self._cfg[key])

    def keys(self):
        return self._cfg.keys()


    # --------------------------------------------------------------------------
    #
    def query(self, key, default=None):
        '''
        For a query like

            config.query('some.path.to.key', 'foo')

        this method behaves like:

            config['some']['path']['to'].get('key', default='foo')

        '''

        if is_str(key): elems = key.split('.')
        else          : elems = key

        if not elems:
            raise ValueError('empty key on query')

        pos  = self._cfg
        path = list()
        for elem in elems:

            if not isinstance(pos, dict):
                raise KeyError('no such key [%s]' % '.'.join(path))

            if elem in pos: pos = pos[elem]
            else          : pos = default

            path.append(elem)

        return pos


# ------------------------------------------------------------------------------
#
class DefaultConfig(Config):
    '''
    The settings in this default config are, unsurprisingly, used as default
    values for various RU classes and methods, as for example for log file
    locations, log levels, profile locations, etc.
    '''

    __metaclass__ = Singleton

    def __init__(self):

        pwd = os.getcwd()

        cfg = {'ns'         : '${RADICAL_DEFAULT_NS:radical}',
               'log_lvl'    : '${RADICAL_DEFAULT_LOG_LVL:ERROR}',
               'log_tgt'    : '${RADICAL_DEFAULT_LOG_TGT:.}',
               'log_dir'    : '${RADICAL_DEFAULT_LOG_DIR:%s}'          % pwd,
               'report'     : '${RADICAL_DEFAULT_REPORT:TRUE}',
               'report_tgt' : '${RADICAL_DEFAULT_REPORT_TGT:stderr}',
               'report_dir' : '${RADICAL_DEFAULT_REPORT_DIR:%s}'       % pwd,
               'profile'    : '${RADICAL_DEFAULT_PROFILE:TRUE}',
               'profile_dir': '${RADICAL_DEFAULT_PROFILE_DIR:%s}'      % pwd,
               }

        super(DefaultConfig, self).__init__(module='radical.utils', cfg=cfg)


# ------------------------------------------------------------------------------


