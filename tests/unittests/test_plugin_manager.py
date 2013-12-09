
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
def test_plugin_manager () :
    """
    Test plugin manager
    """

    pmgr   = ru.PluginManager ('radical.utils')
    plugin = pmgr.load ('unittests', 'default')

    plugin.init ('a', 1)
    
    ret = plugin.run ()

    assert (ret == (1, 'a')), "plugin invocation: '%s' != '%s'" % ([1, 'a'], ret)
    
    # load twice -- plugin is marked as singleton plugin, and will raise if it
    # is created twice
    
    pmgr   = ru.PluginManager ('radical.utils')
    plugin = pmgr.load ('unittests', 'default')
    plugin.init ('a', 1)
    
    ret = plugin.run ()
    assert (ret == (1, 'a')), "plugin invocation: '%s' != '%s'" % ([1, 'a'], ret)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_plugin_manager ()

# ------------------------------------------------------------------------------

