#!/usr/bin/env python

# ------------------------------------------------------------------------------
#
# This example demonstrates various utilities to inspect, print, trigger stack
# traces and exceptions with RU.
#
# ------------------------------------------------------------------------------

import radical.utils as ru

# ------------------------------------------------------------------------------
# helper method
def raise_something():
    print '%s wants an exception' % ru.get_caller_name()
    raise RuntimeError('oops')

# ------------------------------------------------------------------------------
# print current stack trace
def inner_1(arg_1, arg_2):
    ru.print_stacktrace()

# ------------------------------------------------------------------------------
# get currenet stack trace as list (to store to disk or print or whatever)
def inner_2(arg_1, arg_2):
    st = ru.get_stacktrace()
    import pprint
    pprint.pprint(st)

# ------------------------------------------------------------------------------
# print an exception trace, pointint to the origin of an exception
def inner_3(arg_1, arg_2):
    try:
        raise_something()
    except:
        ru.print_exception_trace()

# ------------------------------------------------------------------------------
# print the name of the calling class and method
def inner_4(arg_1, arg_2):
    print ru.get_caller_name()

# ------------------------------------------------------------------------------
# trigger exception for integration testing etc.
def inner_5(arg_1, arg_2):

    import os
    os.environ['RU_RAISE_ON_TEST'] = '3'

    for i in range(10):
        print i
        ru.raise_on('test')

    print

    import os
    os.environ['RU_RAISE_ON_RAND'] = 'RANDOM_10'

    for i in range(100):
        try:
            ru.raise_on('rand')
        except Exception as e:
            print 'raised on %d' % i


# ------------------------------------------------------------------------------
#
def outer(arg):
    print '################################'
    inner_1(arg, 'bar')
    print '################################'
    inner_2(arg, 'bar')
    print '################################'
    inner_3(arg, 'bar')
    print '################################'
    inner_4(arg, 'bar')
    print '################################'
    inner_5(arg, 'bar')
    print '################################'

# ------------------------------------------------------------------------------
#
outer('foo')

