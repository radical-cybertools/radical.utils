
# ------------------------------------------------------------------------------
#
class SchedulerBase(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, resources):
        """
        Initialize the scheduler with a certain set and topology of
        resources to schedule requests over.
        """
        raise NotImplementedError('base class is virtual')


    # --------------------------------------------------------------------------
    #
    def alloc(self, request):
        """ 
        allocate a slice of the available resources to satisfy the request, and
        return that allocation.
        """
        raise NotImplementedError('base class is virtual')


    # --------------------------------------------------------------------------
    #
    def dealloc(self, allocation):
        """
        deallocate a given allocation to return its resources to the pool of
        resources available for new allocations.
        """
        raise NotImplementedError('base class is virtual')


    # --------------------------------------------------------------------------
    #
    def get_layout(self):
        """
        return row/col info for visual representation
        """
        raise NotImplementedError('base class is virtual')


    # --------------------------------------------------------------------------
    #
    def get_map(self):
        """
        return a map indicating current resource allocation
        """
        raise NotImplementedError('base class is virtual')


# ------------------------------------------------------------------------------

