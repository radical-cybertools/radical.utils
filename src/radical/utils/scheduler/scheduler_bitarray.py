
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import math

from .scheduler_base import SchedulerBase


# ------------------------------------------------------------------------------
#
class BitarrayScheduler(SchedulerBase):

    try:
        from bitarray import bitarray as _ba             # pylint: disable=E0401

    except:
        # fake with a do-nothing implementation so that initialization works
        class _ba(object):
            def __init__(self, i):
                pass
            def setall(self, x):
                pass

    _one = _ba(1)
    _one.setall(True)

    # --------------------------------------------------------------------------
    #
    def __init__(self, resources=0):

        if 'cores' not in resources:
            raise ValueError('no cores to schedule over')

        self._pos  = 0
        self._size = resources.get('cores')
        self._ppn  = resources.get('ppn', self._size)

        self._cores = self._ba(self._size)
        self._cores.setall(True)   # True: free

        self._flag_align   = resources.get('align',   True)
        self._flag_scatter = resources.get('scatter', True)


    # --------------------------------------------------------------------------
    #
    def get_layout(self):

        return {'cores' : self._size,
                'rows'  : math.sqrt(self._size),
                'cols'  : math.sqrt(self._size)}


    # --------------------------------------------------------------------------
    #
    def get_map(self):

        return self._cores


    # --------------------------------------------------------------------------
    #
    def _align(self, pat, req, loc):
        """
        For a given continuous set of cores, try to move the set so that it
        resides within a single node.  This obviously only works for sets
        smaller than self.ppn.
        """

        if not self._flag_align:
            # alignment is disabled
            return loc, False

        if req > self._ppn:
            # we can't align allocations spanning nodes
            return loc, False

        # we check if the allocation is on a single node.  If that is not the
        # case, we search again from the point of allocation, and check again.
        # We do that until we cycle around the full core list once, reaching
        # the original location again
        orig_pos   = loc
        pos        = loc
        start_node =  pos            / self._ppn
        end_node   = (pos + req - 1) / self._ppn

        if start_node == end_node:
            # already aligned
            return loc, False

        # we search from pos=loc to the end of cores, then rewind to pos=0
        # and search again from the beginning until again pos=loc.
        rewound = False
        while (pos <= self._size and not rewound) or \
              (pos <  orig_pos   and     rewound)    :

            if pos > self._size:
                pos     = 0
                rewound = True

            # we need to renew the allocation -- we search further from pos
            # until we find something
            loc = self._cores.search(pat, 1, pos + 1)

            if not loc:
                pos     =  0
                rewound = True
                continue

            pos        = loc[0]
            start_node =  pos            / self._ppn
            end_node   = (pos + req - 1) / self._ppn

            if start_node == end_node:
                # found an alignment
                return pos, True

            # found new loc, but no dice wrt. alignment -- try again


        raise RuntimeError('alignment error (%s cores requested)' % req)


    # ------------------------------------------------------------------------------
    #
    def alloc(self, req):

        scattered = False
        aligned   = False
        orig_pos  = self._pos
        loc       = list()
        pat       = self._ba(req)   # keep dict of patterns?
        pat.setall(True)

        # simple search
        loc = self._cores.search(pat, 1, self._pos)


        if not loc:
            # FIXME: assume we first search from pos 100 to 1000, then rewind,
            #        then search from 0 to 1000, 900 cores being searched twice.
            #        We should add an 'end_pos' parameter to bitarray.search.
            #        But then again, rewind should be rare if cores are on
            #        average much larger than requests -- and if that is not the
            #        case, we won't be able to allocate many requests anyway...
            self._pos = 0
            loc = self._cores.search(pat, 1, self._pos)


        if not loc and self._flag_scatter:

            scattered = True

            # search for non-continuous free cores
            loc = self._cores.search(self._one, req, orig_pos)

            if not loc:
                # try again from start
                loc = self._cores.search(self._one, req, 0)


        if not loc:
          # with open('ba_cores.bin', 'w') as f:
          #     self._cores.tofile(f)
          # with open('ba_pat.bin', 'w') as f:
          #     pat.tofile(f)
          # with open('ba_req.bin', 'w') as f:
          #     f.write('%d\n' % req)
            self._pos = 0
            raise RuntimeError('out of cores (%s cores requested)' % req)


        # found a match - update pos
        if scattered:

            # we got a scattered list of cores
            self._pos = loc[-1] + 1
            self._cores.setlist(loc, False)
          # for i in loc:
          #     # FIXME: bulk op?
          #     self._cores[i] = False

        else:
            # we got a continuous block
            loc = loc[0]

            if self._flag_align:
                loc, aligned = self._align(pat, req, loc)

            self._pos = loc + req
            self._cores.setrange(loc, self._pos - 1, False)


        return [req, loc, scattered, aligned]


    # --------------------------------------------------------------------------
    #
    def dealloc(self, res):

        req, loc, scattered, _ = res

        if scattered:
            # we got a scattered list of cores
            self._cores.setlist(loc, True)
          # for i in loc:
          #     # FIXME: bulk op?
          #     self._cores[i] = True
        else:
            # we got a continuous block
            self._cores.setrange(loc, loc + req - 1, True)


    # --------------------------------------------------------------------------
    #
    def get_stats(self):

        # determine frequency and length of stretches of free and busy cores
        free_dict = dict()
        free_cnt  = 0
        busy_dict = dict()
        busy_cnt  = 0
        for c in self._cores:
            if c:
                free_cnt += 1
                if busy_cnt:
                    if busy_cnt not in busy_dict:
                        busy_dict[busy_cnt] = 0
                    busy_dict[busy_cnt] += 1
                    busy_cnt = 0
            else:
                busy_cnt += 1
                if free_cnt:
                    if free_cnt not in free_dict:
                        free_dict[free_cnt] = 0
                    free_dict[free_cnt] += 1
                    free_cnt = 0

        # determine frequency of free core counts per node
        node = 0
        node_free = dict()
        for i in range(self._ppn + 1):
            node_free[i] = 0
        while node * (self._ppn + 1) < self._size:
            free_count = 0
            for i in range(self._ppn):
                if self._cores[node * self._ppn + i]:
                    free_count += 1
            node_free[free_count] += 1
            node += 1


        return {'total'     : self._size,
                'free'      : self._cores.count(),
                'busy'      : self._size - self._cores.count(),
                'free_dist' : free_dict,
                'busy_dist' : busy_dict,
                'node_free' : node_free}


# ------------------------------------------------------------------------------

