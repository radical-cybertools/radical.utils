
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru

pmgr   = ru.PluginManager ('radical.utils')
plugin = pmgr.load ('unittests', 'default')
plugin.init ('a', 1)

ret = plugin.run ()
assert (ret == (1, 'a')), "unexpected return from plugin invocation (%s)" % ret

# testing twice, should *not* result in a reload message

pmgr   = ru.PluginManager ('radical.utils')
plugin = pmgr.load ('unittests', 'default')
plugin.init ('a', 1)

ret = plugin.run ()
assert (ret == (1, 'a')), "unexpected return from plugin invocation (%s)" % ret


