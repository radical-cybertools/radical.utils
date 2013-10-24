
import radical.utils as ru

pmgr    = ru.PluginManager ('troy')
default = pmgr.load ('workload_scheduler', 'default')
default.init ('workload', 'overlay')

print default.run ()

