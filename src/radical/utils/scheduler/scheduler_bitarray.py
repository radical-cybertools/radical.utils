
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import time
import pprint
import random

from bitarray        import bitarray as ba

from .scheduler_base import SchedulerBase


# ------------------------------------------------------------------------------
#
class BitarrayScheduler(SchedulerBase):

    _one = ba(1)
    _one.setall(True)

    # --------------------------------------------------------------------------
    #
    def __init__(self, resources=0):

        if not 'cores' in resources:
            raise ValueError('no cores to schedule over')

        self._pos  = 0
        self._size = resources.get('cores')
        self._ppn  = resources.get('ppn', self._size)

        self._cores = ba(self._size)
        self._cores.setall(True)   # True: free

        self._flag_align   = resources.get('align',   True)
        self._flag_scatter = resources.get('scatter', True)


    # --------------------------------------------------------------------------
    #
    def get_layout(self):

        return {'cores' : 1024*1024,
                'cols'  : 1024,
                'rows'  : 1024}


    # --------------------------------------------------------------------------
    #
    def get_map(self):

        return self._cores


    # --------------------------------------------------------------------------
    #
    def _align(self, pat, req, res):

        if not self._flag_align:
            return res

        # we only align continuous blocks
        if isinstance(res[0], list):
            return

        # make sure that small requests remain on a single node
        if res and self._flag_align  and req < self._ppn:

            start_node =  res[0]        / self._ppn
            end_node   = (res[0] + req) / self._ppn

            while res and (start_node != end_node):

                # we need to renew the allocation -- we search further from pos
                # until we find something
                res = self._cores.search(pat, 1, res[0]+1)

                if res:
                    start_node =  res[0]        / self._ppn
                    end_node   = (res[0] + req) / self._ppn

            # we either found a suitable place, or we reached end of cores.  In the
            # latter case, we will try once more from the beginning.
            if not res:

                res = self._cores.search(pat, 1, 0)

                if not res:
                    # could not align, restart search from origin
                    res = self._cores.search(pat, 1, 0)

                if not res:
                    raise RuntimeError('out of cheese')

        return res


    # ------------------------------------------------------------------------------
    #
    def alloc(self, req):

        loc = list()
        pat = ba(req)   # keep dict of patterns?
        pat.setall(True)

        # simple search
        loc = self._cores.search(pat, 1, self._pos)

        if not loc:
            # FIXME: assume we first search from pos 100 to 1000, then
            #        rewind, then search from 0 to 1000 -- that is 900
            #        cores searched twice.
            self._pos = 0
            loc = self._cores.search(pat, 1, self._pos)

        if not loc and self._flag_scatter:
            # search for non-continuous free cores
            loc = self._cores.search(self._one, req, self._pos)
            if loc:
                self._pos = loc[-1]

        if not loc:
            self._pos = 0
            raise RuntimeError('out of cheese error')

        if self._flag_align:
            loc = self._align(pat, req, loc)

        if not loc:
            self._pos = 0
            raise RuntimeError('cheese alignment error')

        loc = loc[0]

        if isinstance(loc, list):
            # we got a scattered list of cores
            self._pos = loc[-1] + 1
            for i in loc:
                # FIXME: bulk op?
                self._cores[i] = False
        else:
            # we got a continuous block
            self._pos = loc + 1
            self._cores.setrange(loc, loc+req, False)

        return [req, loc]


    # --------------------------------------------------------------------------
    #
    def dealloc(self, res):

        req, loc = res

        if isinstance(loc, list):
            # we got a scattered list of cores
            for i in loc:
                # FIXME: bulk op?
                self._cores[i] = True
        else:
            # we got a continuous block
            self._cores.setrange(loc, loc+req, True)


# ------------------------------------------------------------------------------

