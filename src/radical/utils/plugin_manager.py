
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
from .misc      import as_list


# ------------------------------------------------------------------------------
#
class PluginBase(object):
    '''
    This class serves as base class for any plugin managed by the plugin handler
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, descr: dict) -> None:
        '''
        This constructor MUST be called by any inheriting implementation.
        '''

        self._plugin_descr = descr

    @property
    def plugin_type(self) -> str:
        return self._plugin_descr['type']

    @property
    def plugin_name(self) -> str:
        return self._plugin_descr['name']

    @property
    def plugin_class(self) -> str:
        return self._plugin_descr['class']

    @property
    def plugin_version(self) -> str:
        return self._plugin_descr['version']

    @property
    def plugin_description(self) -> str:
        return self._plugin_descr['description']


# ------------------------------------------------------------------------------
#
class PluginManager(object, metaclass=Singleton):
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
    def __init__(self, namespaces):
        '''
        namespace: name of module (plugins are expected in namespace/plugins/)
        '''

        # import here to avoid circular imports
        self._namespaces = as_list(namespaces)
        self._registry   = dict()
        self._seen       = list()
        self._log        = Logger('radical.utils')

        for namespace in self._namespaces:
            self.load_plugins(namespace, self._log)


    # --------------------------------------------------------------------------
    #
    def seen(self, pfile):

        if pfile in self._seen:
            return True
        else:
            self._seen.append(pfile)
            return False


    # --------------------------------------------------------------------------
    #
    def register(self, ptype, pname, pinfo):

        if ptype not in self._registry:
            self._registry[ptype] = dict()

        if pname not in self._registry[ptype]:
            self._registry[ptype][pname] = pinfo


    # --------------------------------------------------------------------------
    #
    def retrieve(self, ptype, pname):

        return self._registry.get(ptype, {}).get(pname)


    # --------------------------------------------------------------------------
    #
    def load_plugins(self, namespace, log):
        '''
        Load all plugins for the given namespace.  Previously loaded plugins
        are overloaded.
        '''

        self._log.info('loading plugins for namespace %s' % namespace)

        # search for plugins in all system module paths
        for spath in sys.path:

            # we only load plugins installed under the namespace hierarchy
            npath  = namespace.replace('.', '/')
            ppath  = '%s/%s/plugin*/' %  (spath, npath)
            pglob1 = '*/plugin_*.py'
            pglob2 = 'plugin_*.py'

            # we assume that all python sources in that location are
            # suitable plugins
            pfiles = glob.glob(ppath + pglob1) + glob.glob(ppath + pglob2)

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

                if self.seen(pshort):
                    continue

                try:
                    modname = '%s.plugins.%s.%s' % (namespace,
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
                        log.error('no plugin type in %s' % pshort)
                        continue

                    if not pname:
                        log.error('no plugin name in %s' % pshort)
                        continue

                    if not pclass:
                        log.error('no plugin class in %s' % pshort)
                        continue

                    if not pvers:
                        log.error('no plugin version in %s' % pshort)
                        continue

                    if not pdescr:
                        log.error('no plugin description in %s' % pshort)
                        continue

                    # now put the plugin and plugin info into the plugin
                    # registry.  Duh!

                    pinfo = {
                        'plugin'     : plugin,
                        'class'      : pclass,
                        'type'       : ptype,
                        'name'       : pname,
                        'version'    : pvers,
                        'description': pdescr,
                        'instance'   : None
                    }
                    self.register(ptype, pname, pinfo)

                    log.debug('loading plugin %s' % pfile)
                    log.info ('loading plugin %s' % pshort)

                except Exception:
                    log.exception('loading plugin %s failed' % pshort)


    # --------------------------------------------------------------------------
    #
    def list_types(self):
        '''
        return a list of loaded plugin types
        '''
        return list(self._registry.keys())


    # --------------------------------------------------------------------------
    #
    def list(self, ptype):
        '''
        return a list of loaded plugins for a given plugin type
        '''
        if ptype not in self._registry:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._registry.keys())))

        return list(self._registry[ptype].keys())


    # --------------------------------------------------------------------------
    #
    def describe(self, ptype, pname):
        '''
        return a plugin details for a given plugin type / name pair
        '''
        if ptype not in self._registry:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._registry.keys())))

        if pname not in self._registry[ptype]:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin name %s (type: %s) in %s'
                    % (pname, ptype, list(self._registry[ptype].keys())))

        return self._registry[ptype][pname]


    # --------------------------------------------------------------------------
    #
    def load(self, ptype, pname, *args, **kwargs):
        '''
        check if a plugin with given type and name was loaded, if so,
        instantiate its plugin class and return it.
        '''

        if ptype not in self._registry:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin type %s in %s'
                    % (ptype, list(self._registry.keys())))

        if pname not in self._registry[ptype]:
            self._log.debug(self.dump_str())
            raise LookupError('No such plugin name %s (type: %s) in %s'
                    % (pname, ptype, list(self._registry[ptype].keys())))

        try:
            pdescr = self._registry[ptype][pname]
            plugin = pdescr['plugin']
            pclass = pdescr['class']
            pinst  = getattr(plugin, pclass)(pdescr, *args, **kwargs)

            assert(isinstance(pinst, PluginBase)), pinst

        except Exception as e:
            self._log.exception('plugin init failed')
            raise LookupError('Failed to load plugin %s (type: %s)' %
                              (pname, ptype)) from e

        return pinst


    # --------------------------------------------------------------------------
    #
    def dump(self):

        print('plugins')
        pprint.pprint(self._registry)


    # --------------------------------------------------------------------------
    #
    def dump_str(self):

        return 'plugins: \n%s' % pprint.pformat(self._registry)


# ------------------------------------------------------------------------------

