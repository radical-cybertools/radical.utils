
import math as m

from .logger import Logger


# ------------------------------------------------------------------------------
#
def collapse_ranges (ranges):
    '''
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
    '''

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

    for _range in ranges:
        if _range[START] > _range[END]:
            _range[START], _range[END] = _range[END], _range[START]

    # sort ranges into a copy list, by their start time
    _ranges = sorted(ranges, key=lambda x: x[START])

    # set 'base' to the earliest range (smallest starting point)
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
    return sorted([list(b) for b in final])


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

    START = 0
    END   = 1

    t = {0: 'start',
         1: 'end  '}

    for _range in ranges:
        if _range[START] > _range[END]:
            _range[START], _range[END] = _range[END], _range[START]

    # we want to sort all range boundaries by value, but also want to remember
    # if they were start or end
    times  = list()
    for idx in range(len(ranges)):
        r = ranges[idx]
        times.append([r[0], START])
        times.append([r[1], END  ])

    times.sort()

    # go through the sorted list of times, and increase concurrency on `START`,
    # decrease it on `END`.  Make sure we add up all entries for the same value
    # at once.

    # return a sequence of [timestamp, concurrency] tuples
    ret = list()

    # check initial conditions
    assert(times[0][0] >= 0    ), 'negative time %s'       % times[0]
    assert(times[0][1] == START), 'inconsistent ranges %s' % ranges

    last_time   = times[0][0]
    concurrency = 1

    # set zero marker
    ret.append([last_time, 0])

    # for all range boundaries:
    #     if time changed:
    #         store prervious tuple
    #     if a range STARTed at this time, increase concurrency,
    #     if a range ENDed   at this time, decrease concurrency,
    # store trailing tuple

    for time,mode in times[1:]:
        if time != last_time:
            ret.append([last_time, concurrency])
            last_time = time

        if mode == START: concurrency += 1
        else            : concurrency -= 1

    ret.append([last_time, concurrency])

    assert(concurrency == 0), \
            'inconsistent range structure? %s : %s : %s' % (ranges, ret, times)

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
    '''
    checks if a float value `value` is in any of the given `ranges`, which are
    assumed to be a list of tuples (or a single tuple) of start end end points
    (floats).
    Returns `True` or `False`.
    '''

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
# Assume we want to schedule a set of tasks over a set of resources.  To
# maximize utilization, we want to place the largest tasks first and fill the
# gap with small tasks, thus we sort the list of tasks by size and schedule
# beginning with the largest.
#
# For large number of tasks this can become expensive, specifically once the set
# of resources is near exhaustion: we need to search through the whole task list
# before being able to, possibly, place some small tasks.
#
# The algorithm below attempts to use the knowledge that, once a task of
# a certain size can *not* be scheduled, no larger task can.  It performs as
# follows:
#
#       1  attempt to schedule the largest task (n)
#       2  if success
#       3      schedule the next smaller task (n = n - 1)
#       4      if success
#       5          goto (2)
#       7  bisect distance to smallest task: l = bisect(m, 0)
#       8  schedule task l
#       9  if success   # task l is small enough
#      10      bisect distance toward larger task: l = bisect(m, l)
#      11      goto 9
#      12  else         # task l is still too large
#      13      bisect distance towards smaller tasks: l = bisect(l, 0)
#      14      goto 9
#
# This algorithm stops once (m - l == 1), which is the boundary between 'too
# large' and 'small enough' tasks.  From here, we mark all tasks > m as too
# large, set n = l, and begin from scratch.
#
# The implementation below contains some further optimizations.  Specifically,
# we remember the schedule results of all bisections, so that we don't schedule
# or test those tasks twice.  Further, we assume that scheduling a task may
# alter the result for all further schedule attempts (apart from tasks we know
# are too large).
#
# Example:
#
#   # --------------------------------------------------------------------------
#   #
#   # Tasks larger 65 cannot be scheduled.  Once 65 is scheduled, 64 is too
#   # large and fails.  63, 62, 61 fit, but then space is tight and all tasks up
#   # to 42 fail.  42 to 22 get scheduled ok, but then space is out again for
#   # everything larger than 7 - the last 8 tasks (very small) can be scheduled.
#   #
#   # Out of the 128 tasks, 32 will get schedules, so 96 will not.  Instead of
#   # trying schedule those 96 tasks, the algorithm will only test 15 tasks
#   # unsuccessfully (~15%).  The effect improves with larger numbers of tasks
#   # and less available resources, which is usually the case when the RP
#   # scheduler sees high contention between scheduling and unscheduling
#   # operations.  For example, the same setup for 128k tasks results in 25
#   # failed scheduling attempts (0.01%).
#   #
#   failed = list()
#   def schedule(n):
#       if n in [61, 62, 63, 65]    or \
#          n in list(range(22, 42)) or \
#          n < 8:
#           scheduled.append(n)
#           return True   # could be scheduled
#       else:
#           failed.append(n)
#           return False  # schedule failed
#
#   tasks = list(range(128 * 1024))
#   good, bad = lazy_bisect(tasks, schedule)
#
#   assert(len(failed) == 25))
#   for task in good:
#       assert(schedule(task) is True)
#       task not in bad
#   for task in bad:
#       assert(schedule(task) is False)
#       task not in good
#   # --------------------------------------------------------------------------
#
def lazy_bisect(data, check, on_ok=None, on_nok=None, on_skip=None,
                             ratio=0.5, log=None):
    '''
    Find the next potentially good candidate element in a presumably ordered
    list `data` (from smallest to largest). The given callable `check` should
    accept a list element for good/bad checks, and return `True` for good (small
    enough), `False` otherwise (too large).

    The method will return a list with only bad elements (elements which the
    checker deemed too large).  `data` is not altered by this method.

    Note that this is only a bisect if `ratio` is set at `0.5` - otherwise the
    search will back off through the list faster (1.0 >= ratio > 0.5) or slower
    (0.5 > ratio >= 0.0) than bisect.

    The method returns two lists, a list with elements of data which were
    checked `good`, and a list of elements checked as `bad`.

    `on_ok` will be called on tasks where `check()` returned `good`, `on_nok`
    where `check()` returned `bad`, and `on_skip` where bisect decided that no
    `check()` invocation is needed.  The respective data item is the only
    argument to the calls.
    '''

    if not data:
        return [], []

    if not log:
        log = Logger('radical.utils.alg')

    if ratio > 1.0: ratio = 1.0
    if ratio < 0.0: ratio = 0.0

    last_good  = None    # last known good index
    last_bad   = None    # last known bad index
    check_good = list()  # found good index
    check_bad  = list()  # found bad index

    # --------------------------------------------------------------------------
    def wrapcheck(thing, skip=False):
        ret = None
        if skip:
            if on_skip:
                on_skip(thing)
        else:
            ret = check(thing)
            if ret is True  and on_ok : on_ok(thing)
            if ret is False and on_nok: on_nok(thing)

        return ret
    # --------------------------------------------------------------------------

  # # --------------------------------------------------------------------------
  # def state_hay():
  #     hay = ''
  #     for i,x in enumerate(data):
  #         if not i % 10       : hay += '|'
  #         if check(x) is True : hay += '#'
  #         else                : hay += '.'
  #     if not hay.endswith('|'):
  #         hay += '|'
  #     log.debug('=== %30s %s', '', hay)
  # # --------------------------------------------------------------------------
  #
  # # --------------------------------------------------------------------------
  # def state_needle(msg=''):
  #     needle = ''
  #     for i,x in enumerate(data):
  #         if not i % 10       : needle += '|'
  #         if   x in check_good: needle += '#'
  #         elif x in check_bad : needle += '.'
  #         else                : needle += ' '
  #     if not needle.endswith('|'):
  #         needle += '|'
  #     g = last_good
  #     b = last_bad
  #     if g is None: g = '?'
  #     if b is None: b = '?'
  #     log.debug('=== %3s - %3s %20s %s %s', g, b, ' ', needle, msg)
  # # --------------------------------------------------------------------------

  # state_hay()
    first = True
    while True:

        # if we don't know anything, yet, check the first element
        if  last_good is None and \
            last_bad  is None:

            assert(first)
            first = False

            idx = len(data) - 1
            ret = wrapcheck(data[idx])

            if ret is True: check_good.append(idx)
            else          : check_bad .append(idx)

          # state_needle('start %s %s' % (idx, ret))

            if ret is True: last_good = idx
            else          : last_bad  = idx


        # If we only know a good one, just try the next in the list.  This can
        # happen repeatedly, and we'll traverse the list for long as we can
        elif last_good is not None and \
             last_bad  is     None:

            # make sure we still have something to check
            if last_good == 0:
                break

            # check the next element
            idx = last_good - 1

            if idx in check_good:
                last_good = idx
              # state_needle('known %3d True' % idx)
                continue

            if idx in check_bad:
                last_bad = idx
              # state_needle('known %3d False' % idx)
                continue

            ret = wrapcheck(data[idx])

            if ret is True: check_good.append(idx)
            else          : check_bad .append(idx)

            if ret is True: last_good = idx
            else          : last_bad  = idx  # break out of this branch

          # state_needle('good  %3d %s' % (idx, ret))


        # If we know a bad one, we bisect the remaining list and check the
        # found candidate.  If it is also bad, we assume all intermediate
        # elements as bad.  If it is good, we set `last_good` and try again.
        #
        # If last_good is not known here, we bisect to begin of list (0),
        # otherwise we bisect to last_good.
        elif last_bad is not None:

            # make sure we still have something to check
            if last_bad == 0:
                break

            if last_good is not None:
                if last_good > last_bad:
                    last_good = None

            # bisect for next candidate
            if last_good is not None:

                # bisect the difference
                idx = int(m.ceil((last_bad - last_good + 1) / 2 + last_good))

            else:
                # bisect to begin of data
                idx = int(m.ceil((last_bad + 1) / 2))

          # state_needle('range %3d?' % idx)

            # make sure we make progress: if space is too small for bisect, then
            # increase last_good or decrease last_bad
            if idx == last_good: idx = last_good + 1
            if idx == last_bad : idx = last_bad  - 1

            if idx < 0: idx = 0

            # check this bisected index (if we don't know it yet)
            if   idx in check_good: ret = True
            elif idx in check_bad:  ret = False
            else:
                ret = wrapcheck(data[idx])
                if ret is True: check_good.append(idx)
                else          : check_bad .append(idx)

          # state_needle('range %3d %s' % (idx, ret))

            if ret is True:
                # found a new good one closer to the last bad one - bisect again
                # until we neighbor last_bad.  In that case, reset the last_bad
                # and restart searching (and bisecting) from that meeting point.
                last_good = idx

                if last_bad is not None:
                    if last_bad < last_good:
                        last_good = None
                      # state_needle('range  A')

                    elif last_bad - last_good == 1:
                        last_bad = None
                      # state_needle('range  a')

            else:

                # found a new, smaller bad one - consider all indexes between
                # id and the previous last_bad as bad
                for i in range((last_bad - idx - 1)):
                    this = last_bad - i - 1
                    if this not in check_bad and \
                       this not in check_good:
                        wrapcheck(data[this], skip=True)
                        check_bad.append(this)
              # state_needle('Range')
                last_bad = idx

                if last_good is not None and last_good > last_bad:
                    # this last_good is not interesting anymore  we search
                    # downwards
                    last_good = None

  # state_hay()

    # return list of all bad elements
    assert(len(data) == len(check_good) + len(check_bad))
    return [data[i] for i in check_good], \
           [data[i] for i in check_bad ]


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

    test_space = list(range(75))
    parts = partition(test_space, 8)
    for part in parts:
        print('%3d: %s' % (len(part), part))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    import time
    scheduled = list()
    def schedule(n, check=False):

        if not check:
            time.sleep(0.1)  # scheduling is slow (but not checks)
            print(n)

        if n in [61, 62, 63, 65]    or \
           n in list(range(22, 42)) or \
           n < 8:
            scheduled.append(n)
            if not check: print('ok')
            return True

        else:
            if not check: print('--')
            return False

    tasks = list(range(128 * 1024))
    tasks = lazy_bisect(tasks, schedule)

    for task in tasks:
        assert(schedule(task, check=True) is False)

    for task in scheduled:
        assert(task not in tasks)


# ------------------------------------------------------------------------------

