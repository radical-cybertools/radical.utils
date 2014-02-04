
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
def test_plugin_manager () :
    """
    Test plugin manager
    """

    pmgr = ru.PluginManager ('radical.utils')

    plugin_2 = pmgr.load ('unittests_1', 'default_2')
    plugin_2.init ('a', 1)
    ret = plugin_2.run ()
    assert (ret == (1, 'a')), "plugin_2 invocation: '%s' != '%s'" % ([1, 'a'], ret)

    try :
        plugin_1 = pmgr.load ('unittests_1', 'default_1')
        assert False 
    except LookupError :
        pass
    except :
        assert False


    plugin_2 = pmgr.load ('unittests_1', 'default_2')
    plugin_2.init ('a', 1)
    ret = plugin_2.run ()
    assert (ret == (1, 'a')), "plugin_2 invocation: '%s' != '%s'" % ([1, 'a'], ret)
    
    plugin_3 = pmgr.load ('unittests_2', 'default_2')
    plugin_3.init ('a', 1)
    ret = plugin_3.run ()
    assert (ret == (1, 'a')), "plugin_3 invocation: '%s' != '%s'" % ([1, 'a'], ret)
    
    # load twice -- plugin_2 is marked as singleton plugin_2, and will raise if it
    # is created twice
    plugin_3 = pmgr.load ('unittests_2', 'default_2')
    plugin_3.init ('a', 1)
    ret = plugin_3.run ()
    assert (ret == (1, 'a')), "plugin_3 invocation: '%s' != '%s'" % ([1, 'a'], ret)
    

    try :
        pmgr = ru.PluginManager     ('troy')
        plugin_strategy = pmgr.load ('strategy', 'basic_late_binding')
        import troy
        session = troy.Session ()
        plugin_strategy.init_plugin (session)
        plugin_strategy.execute     (None, None, None, None)
    except :
        pass


    import resource
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print "%5d  %d" % (0, mem)

    pmgr = ru.PluginManager ('radical.utils')

    import resource
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print "%5d  %d" % (0, mem)

    for i in range (1000000) :
        plugin_4 = pmgr.load ('unittests_2', 'default_2')
        plugin_4.init ('a', 1)
        ret = plugin_4.run ()
        assert (ret == (1, 'a')), "plugin_4 invocation: '%s' != '%s'" % ([1, 'a'], ret)

        if not i % 100000 :
            mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            print "%5d  %d" % (i, mem)

    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print "%5d  %d" % (i+1, mem)



# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_plugin_manager ()

# ------------------------------------------------------------------------------

