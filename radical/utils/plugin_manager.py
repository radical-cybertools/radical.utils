
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import imp
import sys
import glob


# ------------------------------------------------------------------------------
#
class PluginManager (object) :
    """ 
    The RADICAL plugin management and loading utility.

    The plugin manager allows to manage plugins of a specific types.  For those
    types, the manager can search for installed plugins, list and describe
    plugins found, load plugins, and instantiate the plugin for further use.

    Example::

       plugin_type = 'echo'

       pm = radical.utils.PluginManager ('radical')

       for plugin_name in pm.list (plugin_type) :
           print plugin_name
           print pm.describe (plugin_type, plugin_name)

        default_plugin = pm.load ('echo', 'default')

        default_plugin.init ("world")
        greeting = default_plugin.run ()

        print greeting   # prints "hello default world"

    The plugins are expected to follow a specific naming and coding schema to be
    recognized by the plugin manager.  The naming schema is:

        [mname].plugins.[ptype].plugin_[ptype]_[pname].py

    i.e. for the code example above: `radical.plugins.scheduler.plugin_hello_default.py`

    The plugin code consists of two parts:  a plugin description, and a plugin
    class.  The description is a module level dictionary named
    `PLUGIN_DESCRIPTION`, the plugin class must be named `PLUGIN_CLASS`, and
    must have a class constructor `__init__(*args, **kwargs)` to create plugin
    instances for further use.

    At this point, we leave the definition of the exact plugin signature open,
    but expect that to be more strictly defined per plugin type in the future.
    """


    #---------------------------------------------------------------------------
    # 
    def __init__ (self, mname) :
        """
        mname: name of module (plugins are expected in mname/plugins/)
        """

        self._mname   = mname
        self._plugins = {}

        # load adaptors
        self._load_plugins ()

        print "-------------------------------------------"
        print self._plugins
        print "-------------------------------------------"


    #---------------------------------------------------------------------------
    # 
    def _load_plugins (self) :
        """ 
        Load all plugins for the given namespace.  Previously loaded plugins
        are overloaded.
        """

        # search for plugins in all system module paths
        for path in sys.path :

            # we only load plugins installed under the mname hierarchy
            ppath = "%s/%s/plugins/"  %  (path, self._mname)
            pglob = "*/plugin_*.py"  

            if  not os.path.isdir (ppath) :
                continue

            # we assume that all python sources in that location are
            # suitable plugins
            pfiles = glob.glob (ppath + pglob)

            if  not pfiles :
                continue

            for pfile in pfiles :

                # load and register the plugin
                plugin = imp.load_source (self._mname, pfile)
                pname  = plugin.PLUGIN_DESCRIPTION['name']
                ptype  = plugin.PLUGIN_DESCRIPTION['type']

                if  not ptype in self._plugins :
                    self._plugins[ptype] = {}

                if  pname in self._plugins[ptype] :
                    print "warning: overloading plugin '%s'" % pfile

                self._plugins[ptype][pname] = {
                    'class'       : plugin.PLUGIN_CLASS,
                    'type'        : plugin.PLUGIN_DESCRIPTION['type'],
                    'name'        : plugin.PLUGIN_DESCRIPTION['name'],
                    'version'     : plugin.PLUGIN_DESCRIPTION['version'],
                    'description' : plugin.PLUGIN_DESCRIPTION['description']
                }


    #---------------------------------------------------------------------------
    # 
    def list_types (self) :
        """
        return a list of loaded plugin types
        """
        return self._plugins.keys ()


    #---------------------------------------------------------------------------
    # 
    def list (self, ptype) :
        """
        return a list of loaded plugins for a given plugin type
        """
        if  not ptype in self._plugins :
            raise LookupError ("No such plugin type %s" % ptype)

        return self._plugins[ptype].keys ()


    #---------------------------------------------------------------------------
    # 
    def describe (self, ptype, pname) :
        """
        return a list of loaded plugins for a given plugin type
        """
        if  not ptype in self._plugins :
            raise LookupError ("No such plugin type %s" % ptype)

        if  not pname in self._plugins[ptype] :
            raise LookupError ("No such plugin named %s" % pname)

        return self._plugins[ptype][pname]


    #---------------------------------------------------------------------------
    # 
    def load (self, ptype, pname) :
        """
        check if a plugin with given type and name was loaded, if so, instantiate its
        plugin class, initialize and return in.
        """
        if  not ptype in self._plugins :
            raise LookupError ("No such plugin type %s" % ptype)

        if  not pname in self._plugins[ptype] :
            raise LookupError ("No such plugin named %s" % pname)

        return self._plugins[ptype][pname]['class']()


# ------------------------------------------------------------------------------
#
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

