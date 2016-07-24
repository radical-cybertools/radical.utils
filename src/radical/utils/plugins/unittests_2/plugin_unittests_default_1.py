
from __future__ import absolute_import
import six
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import radical.utils  as ru
import does_not_exist as nope

# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'        : 'unittests_2', 
    'name'        : 'default_1', 
    'version'     : '0.1',
    'description' : 'this is an empty test which basically does nothing.'
  }

# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS (six.with_metaclass(ru.Singleton, object)) :
    """
    This class implements the (empty) default unittest plugin for radical.utils.
    """
    _created      = False # singleton test


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        if  PLUGIN_CLASS._created :
            assert (False), "singleton plugin should not get created twice"

        PLUGIN_CLASS._created = True


    # --------------------------------------------------------------------------
    #
    def init (self, arg1, arg2) :

      # print 'loading unittest plugin'

        self.arg1 = arg1
        self.arg2 = arg2


    # --------------------------------------------------------------------------
    #
    def run (self) :

        return (self.arg2, self.arg1)


# ------------------------------------------------------------------------------

