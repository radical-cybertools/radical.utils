
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import sys
import glob
import pprint

from importlib  import util as imp

from .singleton import Singleton
from .logger    import Logger


# ------------------------------------------------------------------------------
#
class _PluginRegistry(dict, metaclass=Singleton):
    '''
    The plugin registry helper class avoids that plugins are loaded twice.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._registry = dict()


    # --------------------------------------------------------------------------
    #
    def register(self, namespace, plugins):

        if namespace not in self._registry:
            self._registry[namespace] = plugins


    # --------------------------------------------------------------------------
    #
    def retrieve(self, namespace):

        if namespace in self._registry:
            return self._registry[namespace]

        return None


# ------------------------------------------------------------------------------
#
class PluginManager(object):
    '''
    The PluginManager manages plugins of specific types: the manager can search
    for installed plugins, list and describe plugins found, load plugins, and
    instantiate the plugin for further use.

    Example:

        # try to load the 'echo' plugin from the 'radical' namespace
        plugin_type = 'echo'

        pm = radical.utils.PluginManager('radical')

        for plugin_name in pm.list(plugin_type):
            print plugin_name
            print pm.describe(plugin_type, plugin_name)

        default_plugin = pm.load('echo', 'default')

        default_plugin.init_plugin('world')
        default_plugin.run()  # prints 'hello default world'


    The plugins are expected to follow a specific naming and coding schema to be
    recognized by the plugin manager.  The naming schema is:

        [namespace].plugins.[ptype].plugin_[ptype]_[pname].py

    i.e. for the example above: `radical.plugins.echo.plugin_echo_default.py`

    The plugin code consists of two parts:  a plugin description, and a plugin
    class.  The description is a module level dictionary named
    `PLUGIN_DESCRIPTION`, the plugin class must have a class constructor
    `__init__(*args, **kwargs)` to create plugin instances for further use.

    At this point, we leave the definition of the exact plugin signatures open,
    but expect that to be more strictly defined per plugin type in the future.

    Note that the PluginManager construction is, at this point, not considered
    thread safe.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, namespace):
        '''
        namespace: name of module (plugins are expected in namespace/plugins/)
        '''

        # import here to avoid circular imports
        self._namespace = namespace
        self._log       = Logger('radical.utils')
        self._registry  = _PluginRegistry()  # singleton
        self._plugins   = self._registry.retrieve(self._namespace)

        # load adaptors if registry didn't have any registered, yet
        if not self._plugins:
            self._load_plugins()
            self._registry.register(self._namespace, self._plugins)


    # --------------------------------------------------------------------------
    #
    def _load_plugins(self):
        '''
        Load all plugins for the given namespace.  Previously loaded plugins
        are overloaded.
        '''

        # start wish a fresh plugin registry
        self._plugins = dict()

        self._log.info('loading plugins for namespace %s' % self._namespace)

        # avoid to load plugins twice in case of redundant sys paths
        seen = list()

        # search for plugins in all system module paths
        for spath in sys.path:

            # we only load plugins installed under the namespace hierarchy
            npath = self._namespace.replace('.', '/')
            ppath = '%s/%s/plugins/' %  (spath, npath)
            pglob = '*/plugin_*.py'

            # make sure the 'plugins' dir exists
            if not os.path.isdir(ppath):
                continue

            # we assume that all python sources in that location are
            # suitable plugins
            pfiles = glob.glob(ppath + pglob)

            if not pfiles:
                continue

            for pfile in pfiles:

                # from the full plugin file name, derive a short name for more
                # useful logging, duplication checks etc. -- simply remove the
                # namespace path portion...
                if pfile.startswith(spath):
                    pshort = pfile[len(spath):]
                else:
                    pshort = pfile

                # check for duplication
                if pshort in seen:
                    continue
                else:
                    seen.append(pshort)

                try:
                    modname = '%s.plugins.%s.%s' % (
                                self._namespace,
                                os.path.basename(os.path.dirname(pfile)),
                                os.path.splitext(os.path.basename(pfile))[0])

                    # now load the plugin proper
                    spec   = imp.spec_from_file_location(modname, pfile)
                    plugin = imp.module_from_spec(spec)
                    spec.loader.exec_module(plugin)

                    # get plugin details from description
                    ptype  = plugin.PLUGIN_DESCRIPTION.get('type',        None)
                    pname  = plugin.PLUGIN_DESCRIPTION.get('name',        None)
                    pclass = plugin.PLUGIN_DESCRIPTION.get('class',       None)
                    pvers  = plugin.PLUGIN_DESCRIPTION.get('version',     None)
                    pdescr = plugin.PLUGIN_DESCRIPTION.get('description', None)

                    # make sure details are complete
                    if not ptype:
                        self._log.error('no plugin type in %s' % pshort)
                        continue

                    if not pname:
                        self._log.error('no plugin name in %s' % pshort)
                        continue

                    if not pclass:
                        self._log.error('no plugin class in %s' % pshort)
                        continue

                    if not pvers:
                        self._log.error('no plugin version in %s' % pshort)
                        continue

                    if not pdescr:
                        self._log.error('no plugin description in %s' % pshort)
                        continue

                    # now put the plugin and plugin info into the plugin
                    # registry.  Duh!
                    if ptype not in self._plugins:
                        self._plugins[ptype] = {}

                    if pname in self._plugins[ptype]:
                        self._log.warn('overloading plugin %s' % pshort)

                    self._plugins[ptype][pname] = {
                        'plugin'     : plugin,
                        'class'      : pclass,
                        'type'       : ptype,
                        'name'       : pname,
                        'version'    : pvers,
                        'description': pdescr,
                        'instance'   : None
                    }

                    self._log.debug('loading plugin %s' % pfile)
                    self._log.info ('loading plugin %s' % pshort)

                except Exception:
                    self._log.exception('loading plugin %s failed' % pshort)


    # --------------------------------------------------------------------------
    #
    def list_types(self):
        '''
        return a list of loaded plugin types
        '''
        return list(self._plugins.keys())


    # --------------------------------------------------------------------------
    #
    def list(self, ptype):
        '''
        return a list of loaded plugins for a given plugin type
        '''
        if ptype not in self._plugins:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._plugins.keys())))

        return list(self._plugins[ptype].keys())


    # --------------------------------------------------------------------------
    #
    def describe(self, ptype, pname):
        '''
        return a plugin details for a given plugin type / name pair
        '''
        if ptype not in self._plugins:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._plugins.keys())))

        if pname not in self._plugins[ptype]:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin name %s (type: %s) in %s'
                    % (pname, ptype, list(self._plugins[ptype].keys())))

        return self._plugins[ptype][pname]


    # --------------------------------------------------------------------------
    #
    def load(self, ptype, pname):
        '''
        check if a plugin with given type and name was loaded, if so,
        instantiate its plugin class and return it.
        '''

        if ptype not in self._plugins:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._plugins.keys())))

        if pname not in self._plugins[ptype]:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin name %s (type: %s) in %s'
                    % (pname, ptype, list(self._plugins[ptype].keys())))


        plugin = self._plugins[ptype][pname]['plugin']
        pclass = self._plugins[ptype][pname]['class']
        pinst  = getattr(plugin, pclass)()

        # create new plugin instance
        return pinst


    # --------------------------------------------------------------------------
    #
    def dump(self):

        print('plugins')
        pprint.pprint(self._plugins)


    # --------------------------------------------------------------------------
    #
    def dump_str(self):

        return 'plugins: \n%s' % pprint.pformat(self._plugins)


# ------------------------------------------------------------------------------

