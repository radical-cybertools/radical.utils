
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


from   logging import DEBUG, INFO, WARNING, WARN, ERROR, CRITICAL
from   logger  import getLogger

import radical.utils as ru

# ------------------------------------------------------------------------------
def log_version (module, name, version, version_detail=None) :

    _log = getLogger (module)
    if  version_detail :
        _log.info ('%-20s version: %s (%s)', name, version, version_detail)
    else :
        _log.info ('%-20s version: %s', name, version)

import sys
log_version ('radical', 'python.interpreter', ' '.join(sys.version.split()))
log_version ('radical', 'radical.utils',      ru.version, ru.version_detail)


# ------------------------------------------------------------------------------

