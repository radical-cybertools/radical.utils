
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
# We provide a json based config file parser with several custom extensions
#
#   - system config files will be merged with user configs (if those exist)
#   - python style comments are filtered out before parsing 
#   - after parsing, values are set or expanded via `os.environ`
#
#
# Config Names and Locations
# --------------------------
#
# We assume two basic locations for config files: on is installed within the
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
# For example, the module `radical.pilot` will have the following config dirs:
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
#     'ncsa'  : { 'foo' : 'baz' }
#   }
#
# User configuration files are expected to match that structure.
#
#
# Queries
# -------
#
# We support two types of queries on the resulting parsed configs:
#
#   - the `as_dict()`  method returns a Python dict representation
#   - the `query(key)` method returns a single value, or 'None' if not found.
#
# In the latter `query()` case, the `key` can be specified as dot-separated
# path, so that the following two snippets are equivalent:
#
#   cfg = config.as_dict()
#   val = cfg['foo']['bar'].get('baz')
#
#   val = config.query('foo.bar.baz')
#
# Note that `as_dict()` will return a deepcopy of the configuration, `query()`
# will operate on the *original* configuration, and thus may return references
# into the original cofig dict.
#
#
# Environment
# -----------
# 
# Towards `os.environ` completion, we support the following syntax in all string
# *values* (not keys):
#
#   '$env{RADICAL_UTILS_ENV:default_value}
#
# which will be replaced by 
#
#   `os.environ.get('RADICAL_UTILS_ENV', 'default_value)`
#
# The default value is optional.  Env evaluation is only performed at time of
# parsing, not at time of query.
#
#
# Validation
# ----------
#
# It probably makes sense to switch to a json schema validator at some point,
# see for example https://pypi.python.org/pypi/json-schema-validator. For we
# remain schema-less, and will thus, in a very pythonesque way, only fail once
# values are queried or used.
#
# ------------------------------------------------------------------------------

import pkgutil
import copy
import glob
import os
import re

from .dict_mixin import dict_merge
from .read_json  import read_json

# ------------------------------------------------------------------------------
#
class Config(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, module, path=None, name=None):

        modpkg  = pkgutil.get_loader(module)
        modfile = modpkg.filename

        home = os.environ.get('HOME', '/tmp')
        home = os.environ.get('RADICAL_UTILS_CONFIG_USR_DIR', home)

        sys_dir = "%s/configs" % (modfile)
        usr_dir = "%s/.%s"     % (home, module.replace('.', '/'))

        if path and name:
            raise ValueError("'path' and 'name' parameters are exclusive")

        if not path and not name:
            raise ValueError("'path' or 'name' parameter missing")

        if path: path = path
        else   : path = name.replace('.', '/')

        if '*' in path: starred = True
        else          : starred = False

        if path.count('*') > 1:
            raise ValueError('single wildcard allowed')

        sys_fspec = '%s/%s' % (sys_dir, path)
        usr_fspec = '%s/%s' % (usr_dir, path)

        sys_cfg = dict()
        usr_cfg = dict()

        if starred:

            star_idx    = path.find('*')
            prefix_len  = star_idx
            postfix_len = len(path) - star_idx - 1

            for sys_fname in glob.glob(sys_fspec):
                if sys_fname.endswith('.json'): post = postfix_len + 5
                else                          : post = postfix_len
                base = os.path.basename(sys_fname)[prefix_len:-post]
                sys_cfg[base] = read_json(sys_fname)

            for usr_fname in glob.glob(usr_fspec):
                if usr_fname.endswith('.json'): post = postfix_len + 5
                else                          : post = postfix_len
                base = os.path.basename(usr_fname)[prefix_len:-post]
                usr_cfg[base] = read_json(usr_fname)

        else: # not starred

            sys_fname = sys_fspec
            usr_fname = usr_fspec

            if not os.path.isfile(sys_fname): sys_fname += '.json' 
            if not os.path.isfile(usr_fname): usr_fname += '.json'

            if     os.path.isfile(sys_fname): sys_cfg = read_json(sys_fname)
            if     os.path.isfile(usr_fname): usr_cfg = read_json(usr_fname)


        # we have the usr and sys config - merge them before env expansion
        self._cfg = dict_merge(sys_cfg, usr_cfg, policy='overwrite')

        # env expand
        def _env_mixin(d):
            if isinstance(d, dict):
                for k,v in d.iteritems():
                    d[k] = _env_mixin(v)
            elif isinstance(d, basestring):
                out = ''
                while True:
                    res = re.search('\$env{(.*?)}', d)           # 'bar$env{FOO:foo}baz'
                    if not res:
                        out += d
                        break
                    match = res.group(1)
                    if not ':' in match:  
                        match += ':'                             # $env{FOO} -> $env(FOO:}
                    out += d[:res.start(0)]                      # out += 'bar'
                    out += os.environ.get(*match.split(':', 1))  # out += 'foo'
                    d    = d[res.end(0):]                        # d    = 'baz'
                d = out
            return d
        _env_mixin(self._cfg)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):

        return copy.deepcopy(self._cfg)


    # --------------------------------------------------------------------------
    #
    def query(self, key):

        elems = key.split('.')

        if not elems:
            raise ValueError('empty key on query')

        pos = self._cfg
        for elem in elems:

            if not isinstance(pos, dict):
                raise ValueError('key too long')

            pos = pos.get(elem)

            if None == pos:
                return None

        return pos



    # --------------------------------------------------------------------------
    #
    @classmethod
    def test(self):
    
        import pprint
        
        # store this in $HOME/radical/pilot/resource_yake.json
        #
        # { "grace": { "agent_launch_method": "$env{FOO:bar}" } }
        os.environ['FOO'] = 'GSISSH'
    
        cfg = Config(module='radical.pilot', name='resource_*')
        pprint.pprint(cfg.as_dict()['yale'])
        print cfg.query('yale.grace.agent_launch_method')
        print cfg.query('yale.grace.no_launch_method')


# ------------------------------------------------------------------------------

