
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'

import radical.utils as ru


# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'       : 'unittests_1',
    'name'       : 'default_2',
    'class'      : 'PLUGIN_CLASS',
    'version'    : '0.1',
    'description': 'this is an empty test which basically does nothing.'
}


# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS(ru.PluginBase, metaclass=ru.Singleton):
    '''
    This class implements the (empty) default unittest plugin for radical.utils.
    '''

    _created = False  # singleton test


    # --------------------------------------------------------------------------
    #
    def __init__(self, descr, *args, **kwargs):

        super(PLUGIN_CLASS, self).__init__(descr)

        if PLUGIN_CLASS._created:
            assert(False), 'singleton plugin created twice'

        PLUGIN_CLASS._created = True

        self._args   = args
        self._kwargs = kwargs


    # --------------------------------------------------------------------------
    #
    def run(self):

        return self._args


# ------------------------------------------------------------------------------

