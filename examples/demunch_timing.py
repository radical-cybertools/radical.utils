#!/usr/bin/env python
# pylint: disable=unnecessary-lambda

import timeit

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def demunch_timing():

    # --------------------------------------------------------------------------
    #
    class Bar_1(ru.Munch):
        _schema = {'bar': str}

    # --------------------------------------------------------------------------
    #
    class Buz_1(ru.Munch):
        _schema = {'buzz': list}

    # --------------------------------------------------------------------------

    print('\nTimings for demunch operation')

    b = Bar_1({'bar': 'test'})
    print('No  list                            : ',
          timeit.timeit(lambda: b.as_dict(), number=1000))

    b = Buz_1({'buzz': ['buz'] * 10})
    print('Has list with 10   elements         : ',
          timeit.timeit(lambda: b.as_dict(), number=1000))

    b = Buz_1({'buzz': ['buz'] * 100})
    print('Has list with 100  elements         : ',
          timeit.timeit(lambda: b.as_dict(), number=1000))

    b = Buz_1({'buzz': ['buz'] * 1000})
    print('Has list with 1000 elements         : ',
          timeit.timeit(lambda: b.as_dict(), number=1000))

    bar_1 = Bar_1({'bar': 'test'})
    buz_1 = Buz_1({'buzz': [bar_1] * 1000})
    print('Has list with 1000 ru.Munch elements: ',
          timeit.timeit(lambda: ru.demunch(buz_1), number=1000))

    b = Buz_1({'buzz': [{k: str(k) for k in range(1000)}]})
    print('Has dict with 1000 elements         : ',
          timeit.timeit(lambda: b.as_dict(), number=1000))


if __name__ == '__main__':
    demunch_timing()
