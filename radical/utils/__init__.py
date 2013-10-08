

from plugin_manager import PluginManager

import os
import threading
import subprocess      as sp


# ------------------------------------------------------------------------------
#
_rlock = threading.RLock ()


# ------------------------------------------------------------------------------
#
with _rlock :

    version = "unknown"
    
    try :
        cwd     = os.path.dirname (os.path.abspath (__file__))
        fn      = os.path.join    (cwd, 'VERSION')
        version = open (fn).read ().strip ()
    
        p   = sp.Popen (['git', 'describe', '--tags', '--always'],
                        stdout=sp.PIPE)
        out = p.communicate()[0]
    
        # ignore pylint error on p.returncode -- false positive
        if  out and not p.returncode :
            version += '-' + out.strip()
    
    except Exception :
        pass
    


# ------------------------------------------------------------------------------
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

