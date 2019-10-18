#!/usr/bin/env python

__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


'''
And example on using the radical.utils.config tools.

This example will read config options fomr $HOME/.examples.cfg::

    [config]
    casing   = upper
    excluded = sparks,pftools

    [sp3.cd]
    exe  = /usr/local/bin/sp3
    args = no,idea

That setting can be overwritting via $EXAMPLE_CONFIG_CASING.
'''

import radical.utils.config  as ruc


# ------------------------------------------------------------------------------
#
# a set of pre-defined options
#
_config_options = {'casing'  : '${EXAMPLE_CONFIG_CASING:default}',
                   'excluded': []}

_sp3_options    = {'exe'     : '/usr/bin/sp3',
                   'args'    : []}


# ------------------------------------------------------------------------------
#
class FancyEcho(ruc.Configurable):
    """
    This example will evaluate the given configuration, and
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ruc.Configurable.__init__(self, 'radical.utils')
        self._cfg.merge(_config_options)
        self._cfg.merge(_sp3_options)

        self._mode = self._cfg['casing']
        print("mode: %s" % self._mode)

        self._excl = self._cfg['excluded']
        print("excl: %s" % type(self._excl))
        print("excl: %s" % self._excl)


        if 'sp3' not in self._excl:
            print('running sp3')

        if  'sparks' not in self._excl:
            print('running sparks')

        if  'pftools' not in self._excl:
            print('running pftools')

        # use sp3 configuration
        print(self._cfg['exe'])
        print(self._cfg['args'])


    # --------------------------------------------------------------------------
    #
    def echo(self, source):

        target = ""

        if  self._mode == 'default':
            target = source

        elif self._mode == 'lower':
            target = source.lower()

        elif self._mode == 'upper':
            target = source.upper()

        else:
            target = 'cannot handle mode %s' % self._mode

        return target


# ------------------------------------------------------------------------------
#

fc  = FancyEcho()
src = ''

while 'quit' != src:
    src = input('> ')
    tgt = fc.echo(src)
    print('  ' + tgt)


# ------------------------------------------------------------------------------
#



