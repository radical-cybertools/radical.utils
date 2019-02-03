

# ------------------------------------------------------------------------------
#
def collapse_ranges (ranges):
    """
    Given be a set of ranges (as a set of pairs of floats [start, end] with
    'start <= end'.  This algorithm will then collapse that set into the
    smallest possible set of ranges which cover the same, but not more nor
    less, of the domain (floats).

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
    the list of final ranges then, and that list is returned.
    """

    # FIXME: does tuple and set conversion really add anything?

    # Ranges must be unique: we do not count timings when they start and end at
    # exactly the same time. By using a set, we do not repeat ranges.
    # we convert to a list before return.
    final = set()

    # return empty list if given an empty list
    if not ranges:
        return []

    START = 0
    END   = 1

    # sort ranges into a copy list, by their start time
    _ranges = sorted(ranges, key=lambda x: x[START])

    # sat 'base' to the earliest range (smallest starting point)
    base = _ranges[0]

    for _range in _ranges[1:]:

        # if range is empty, skip it
        if _range[START] == _range[END]:
            continue

        if _range[START] <= base[END]:
            # ranges overlap -- extend the base
            base[END] = max(base[END], _range[END])

        else:
            # ranges don't overlap -- move base to final, and current _range
            # becomes the new base
            final.add(tuple(base))
            base = _range

    # termination: push last base to final
    final.add(tuple(base))

    # Return final as list of list in case a mutable type is needed.
    return [list(b) for b in final]


# ------------------------------------------------------------------------------
#
def range_concurrency(ranges):
    '''
    given a set of *un*collapsed ranges, return a series which describes the
    range-concurrency at any point.

    Example:
      Ranges: 
        [----]  [---------]             [--------]
          [--------]         [-]      [------------]      [--]

      Concurrency:
        1 2  1  2  1      0  1 0      1 2        1 0      1  0

    Returned is a sorted list of tuples where the first entry defines at what
    range value the concurrency changed, and the second value defines to what
    the concurrency count changed at that point.

    You could consider the ranges to be of type `[time_start, time_end]`, and
    the return would be a list of `[timestamp, concurrency]`, if that helps --
    but the algorithm does not make any assumption on the data type, really,
    only that the values can be sorted.
    '''

    if not ranges:
        return list()

    # we want to sort all range boundaries by value, but also want to remember
    # if they were start or end
    START = 0
    END   = 1
    vals  = list()
    for idx in range(len(ranges)):
        r = ranges[idx]
        vals.append([r[0], START])
        vals.append([r[1], END  ])

    # go through the sorted list of times, and increase concurrency on `START`,
    # decrease it on `END`.  Make sure we add up all entries for the same value
    # at once.

    # first entry
    assert(vals[0][1] == START), 'inconsistent ranges'
    last_val    = vals[0][0]
    ret         = list()
    concurrency = 1
    for val,kind in sorted(vals):
        if val == last_val:
            last_val = val
            if kind == START: concurrency += 1
            else            : concurrency -= 1
        else:
            ret.append([last_val, concurrency])

    assert(concurrency == 0), 'inconsistent range structure?'
    ret.append([last_val, concurrency])

    return ret


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

        q, r = divmod(n - k, nparts)
        a, b = b, b + q + (r != 0)

        ret.append(space[a:b])

    return ret


# ------------------------------------------------------------------------------
#
def in_range(value, ranges):
    """
    checks if a float value `value` is in any of the given `ranges`, which are
    assumed to be a list of tuples (or a single tuple) of start end end points
    (floats).
    Returns `True` or `False`.
    """

    # is there anythin to check?
    if not ranges or not len(ranges):
        return False

    if not isinstance(ranges[0], list):
        ranges = [ranges]

    for r in ranges:
        if value >= r[0] and value <= r[1]:
            return True

    return False


# ------------------------------------------------------------------------------
#
def remove_common_prefix(data, extend=0):
    '''
    For the given list of strings, remove the part which appears to be a common
    prefix to all of them.  If `extend` is given, the last `n` letters of the
    prefix are preserved.

    See also https://stackoverflow.com/questions/46128615/ .
    '''

    if not data:
        return data

    # sort the list, get prefix from comparing first and last element
    s1 = min(data)
    s2 = max(data)
    prefix = s1

    for i, c in enumerate(s1):
        if c != s2[i]:
            prefix = s1[:i]
            break

    if extend > 0 and len(prefix) < len(s1):
        prefix = s1[:min(len(prefix) - extend, len(s1))]

    # remove the found prefix from all elements
    return [elem.split(prefix, 1)[-1] for elem in data]


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test = [[ 0, 10],
            [20, 30],
            [40, 50],
            [60, 70],
            [80, 90],
            [ 5, 15],
            [35, 55]]

    import pprint
    pprint.pprint(test)
    pprint.pprint(collapse_ranges(test))

    test_space = range(75)
    parts = partition(test_space, 8)
    for part in parts:
        print "%3d: %s" % (len(part), part)


# ------------------------------------------------------------------------------

