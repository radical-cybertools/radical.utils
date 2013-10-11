
""" And example on using the radical.utils.config tools. """

import radical.utils.config  as ruc


# ------------------------------------------------------------------------------
#
# a set of pre-defined options
##
_config_options = [
    { 
    'category'      : 'config',
    'name'          : 'casing', 
    'type'          : str, 
    'default'       : 'default',
    'valid_options' : ['default', 'lower', 'upper'],
    'documentation' : "This option determines the casing of example's output",
    'env_variable'  : 'EXAMPLE_CONFIG_CASING'
    }
]

# ------------------------------------------------------------------------------
#
class FancyEcho (ruc.Configurable): 
    """ 
    This example will evaluate the given configuration, and 
    """

    #-----------------------------------------------------------------
    # 
    def __init__(self):
        
        # set the configuration options for this object
        ruc.Configurable.__init__ (self, 'examples', 'config', _config_options)
        self._cfg = self.get_config ()

        self._mode = self._cfg['casing'].get_value ()
        print "mode: %s" % self._mode

    #-----------------------------------------------------------------
    # 
    def echo (self, source) :

        target = ""

        if  self._mode == 'default' :
            target = source

        elif self._mode == 'lower' :
            target = source.lower()

        elif self._mode == 'upper' :
            target = source.upper()

        else :
            target = 'cannot handle mode %s' % self._mode

        return target


# ------------------------------------------------------------------------------
#

fc  = FancyEcho ()
src = ''

while 'quit' != src :
    src = raw_input ('> ')
    tgt = fc.echo (src)
    print '  ' + tgt


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


