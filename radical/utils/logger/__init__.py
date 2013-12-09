
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


from   logging import DEBUG, INFO, WARNING, WARN, ERROR, CRITICAL
from   logger  import getLogger

import radical.utils as ru

# ------------------------------------------------------------------------------
def log_version (module='radical', name='radical.utils', version=ru.version) :

    _log = getLogger (module)
    _log.info ('%-15s version: %s', name, version)

log_version ()


# ------------------------------------------------------------------------------

