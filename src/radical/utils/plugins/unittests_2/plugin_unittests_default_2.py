
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'        : 'unittests_2',
    'name'        : 'default_2',
    'version'     : '0.1',
    'description' : 'this is a test which basically does nothing.'
}


# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS(object):
    '''
    This class implements a unittest plugin for radical.utils.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._args = None


    # --------------------------------------------------------------------------
    #
    def init(self, *args):

        assert(self._args is None), 'plugin should get created twice'
        self._args = args


    # --------------------------------------------------------------------------
    #
    def run(self):

        return self._args


# ------------------------------------------------------------------------------

