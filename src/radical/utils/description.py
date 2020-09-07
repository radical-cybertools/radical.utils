
# ------------------------------------------------------------------------------
#
# We provide a base class for all kinds of description objects in the RCT
# stack: job descriptions, task descriptions, pilot descriptions, workload
# descriptions etc.  That base class provides:
#
#   - dict like API
#   - schema based type definitions
#   - optional runtime type checking
#
# The Description base class provides a property API, similar to the `ru.Config`
# class.
#

from .munch import Munch


# ------------------------------------------------------------------------------
#
class Description(Munch):
    '''
    This is an abstract base class for RCT description types.  Any inheriting
    class MUST provide a `self._schema` class member (not class instance member)
    which is used to verify the description's data validity.  Validation can be
    performed on request (`d.verify()`), or when setting description properties.
    The default is to verify on explicit calls only.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None, verify_setter=False):

        super(Description, self).__init__(from_dict=from_dict)

        if verify_setter:
            raise NotImplementedError('setter verification not yet implemented')

        # TODO: setter verification should be done by attempting to cast the
        #       value to the target type and raising on failure.  Non-trivial
        #       types (lists, dicts) can use `as_list` and friends, or
        #       `isinstance` if that is not available
        #
        # TODO: setter verification should verify that the property is allowed


# ------------------------------------------------------------------------------

