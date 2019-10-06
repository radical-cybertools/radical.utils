
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2018, RADICAL"
__license__   = "GPL"


"""
Provide an abstract base class for a component of a distributed queue network.
The node can exist either as thread, as a local process, or as a remote process,
spawned via SAGA.  The communication between nodes is always via queues, were
groups of nodes feed into the same queues, and/or are fed from the same queues.
A node can be fed from multiple queues, where queues are prioritized according
to some policy.
"""


# ------------------------------------------------------------------------------
#
class Node(object):
    pass


# ------------------------------------------------------------------------------

