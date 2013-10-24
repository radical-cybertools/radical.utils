
import radical.utils as ru

pmgr    = ru.PluginManager ('radical.utils')
plugin = pmgr.load ('unittests', 'default')
plugin.init ('a', 1)

ret = plugin.run ()
assert (ret == (1, 'a')), "unexpected return from plugin invocation (%s)" % ret

