
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'

import radical.utils as ru


# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'       : 'unittests_2',
    'name'       : 'default_2',
    'class'      : 'PLUGIN_CLASS',
    'version'    : '0.1',
    'description': 'this is a test which basically does nothing.'
}


# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS(ru.PluginBase):
    '''
    This class implements a unittest plugin for radical.utils.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, descr, *args, **kwargs):

        super(PLUGIN_CLASS, self).__init__(descr)

        self._args = args


    # --------------------------------------------------------------------------
    #
    def run(self):

        return self._args


# ------------------------------------------------------------------------------

