
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'

import radical.utils  as ru
import does_not_exist as nope                       # noqa pylint: disable=F0401


# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'       : 'unittests_1',
    'name'       : 'default_1',
    'version'    : '0.1',
    'description': 'this is an empty test which basically does nothing.'
}


# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS(object, metaclass=ru.Singleton):
    '''
    This class implements the (empty) default unittest plugin for radical.utils.
    '''

    _created = False  # singleton test


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        if PLUGIN_CLASS._created:
            assert(False), 'singleton plugin should not get created twice'

        PLUGIN_CLASS._created = True

        self._args = None


    # --------------------------------------------------------------------------
    #
    def init(self, *args):

        self._args = args


    # --------------------------------------------------------------------------
    #
    def run(self):

        return self._args


# ------------------------------------------------------------------------------

