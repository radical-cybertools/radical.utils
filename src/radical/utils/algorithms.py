
import os
import regex


# ------------------------------------------------------------------------------
#
def collapse_ranges (ranges):
    """
    given be a set of ranges (as a set of pairs of floats [start, end] with
    'start <= end'.  This algorithm will then collapse that set into the
    smallest possible set of ranges which cover the same, but not more nor less,
    of the domain (floats).

    We first sort the ranges by their starting point.  We then start with the
    range with the smallest starting point [start_1, end_1], and compare to
    the next following range [start_2, end_2], where we now know that
    start_1 <= start_2.  We have now two cases:

    a) when start_2 <= end_1, then the ranges overlap, and we collapse them into
    range_1: range_1 = [start_1, max[end_1, end_2]

    b) when start_2 > end_1, then ranges don't overlap.  Importantly, none of
    the other later ranges can ever overlap range_1, because there start points
    are even larger.  So we move range_1 to the set of final ranges, and restart
    the algorithm with range_2 being the smallest one.

    Termination condition is if only one range is left -- it is also moved to
    the set of final ranges then, and that set is returned.
    """

    final = list()

    # return empty list if given an empty list
    if not ranges:
        return final

    # sort ranges into a copy list
    _ranges = sorted (ranges, key=lambda x: x[0])

    START = 0
    END   = 1

    base = _ranges[0]  # smallest range

    for _range in _ranges[1:] :

        if _range[START] <= base[END]:
            # ranges overlap -- extend the base
            base[END] = max(base[END], _range[END])

        else:
            # ranges don't overlap -- move base to final, and current _range
            # becomes the new base
            final.append (base)
            base = _range

    # termination: push last base to final
    final.append (base)

    return final


# ------------------------------------------------------------------------------
#
def partition(space, nparts):
    '''
    create balanced partitions from an iterable space.  This method preserves
    contiguous order.

    kudos:
    http://code.activestate.com/recipes/425397-split-a-list-into-roughly-equal-sized-pieces/
    '''

    n   = len(space)
    b   = 0
    ret = list()

    for k in range(nparts):

        q, r = divmod(n-k, nparts)
        a, b = b, b + q + (r!=0)

        ret.append(space[a:b])

    return ret


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test = [ [ 0, 10],
             [20, 30],
             [40, 50],
             [60, 70],
             [80, 90],
             [ 5, 15],
             [35, 55] ]

    import pprint
    pprint.pprint (test)
    pprint.pprint (collapse_ranges (test))

    space = range(75)
    parts = partition(space, 8)
    for part in parts:
        print "%3d: %s" % (len(part), part)


# ------------------------------------------------------------------------------

