

# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'        : 'unittests', 
    'name'        : 'default', 
    'version'     : '0.1',
    'description' : 'this is an empty test which basically does nothing.'
  }

# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS (object) :
    """
    This class implements the (empty) default unittest plugin for radical.utils.
    """

    # --------------------------------------------------------------------------
    #
    def init (self, arg1, arg2) :

        self.arg1 = arg1
        self.arg2 = arg2


    # --------------------------------------------------------------------------
    #
    def run (self) :

        return (self.arg2, self.arg1)


# ------------------------------------------------------------------------------

