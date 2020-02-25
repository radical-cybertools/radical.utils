
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2014-2020, RADICAL@Rutgers"
__license__   = "MIT"

# pylint: disable=protected-access

import time
import threading as mt

from   datetime import datetime


# ------------------------------------------------------------------------------
#
# static datetime instance used for the `epoch` helper method
_epoch = datetime(1970, 1, 1)


# ------------------------------------------------------------------------------
#
def epoch(data, pattern):
    '''
    convert a given datetime string into seconds since EPOCH.
    The string is parsed as defined by POSIX's `strptime()`.
    '''

    return dt_epoch(datetime.strptime(data, pattern))


# ------------------------------------------------------------------------------
#
def dt_epoch(dt):
    '''
    convert a given datetime instance into seconds since EPOCH.
    '''

    return(dt - _epoch).total_seconds()


# ------------------------------------------------------------------------------
#
# Provides class decorators to time all public class methods
#
# The class decorator is provided as decorator method `@timed_class`, and as
# decorator class `@TimedClass`.  It thus also serves as documentation on the
# different ways to implement class decorators.
#
# This is called on *decorator class* instantiation
def timed_method(func):
    '''
    This class decorator will decorate all public class methods with a timing
    function.  That will time the call invocation, and store the respective data
    in a private class dict '__timing'.  Additionally, it will add the class
    method '_timing_last()', which will return the tuple `[method name, method
    timer]` for the last timed invocation of a class method.
    '''

    # apply timing decorator to all public methods
    def func_timer(obj, *args, **kwargs):

        try:
            _ = obj.__timing
            assert(_)

        except Exception:

            # never seen this one before -- create private timing dict, and add
            # timing_last method
            obj.__timing = dict()

            def timing_last():
                last_call = obj.__timing.get('__last_call', None)
                timer     = obj.__timing.get(last_call, [None])[0]
                return last_call, timer

            def timing_stats():
                import math
                ret = ""
                for key in sorted(obj.__timing.keys()):

                    if  key == '__last_call':
                        continue

                    tlist = obj.__timing[key]
                    tnum  = len(tlist)
                    tmean = sum(tlist) / tnum
                    tdev  = [x - tmean for x in tlist]
                    tdev2 = [x * x for x in tdev]
                    tstd  = math.sqrt(sum(tdev2) / tnum)

                    ret  += "%-20s: %10.3fs (+/- %5.3fs) [n=%5d]\n" \
                          % (key, tmean, tstd, tnum)

                return ret


            def timing_reset():
                obj.__timing = dict()

            obj._timing_last  = timing_last
            obj._timing_stats = timing_stats
            obj._timing_reset = timing_reset


        # object is set up -- time the call and record timings
        fname = func.__name__
        tdict = obj.__timing

        start = time.time()
        ret   = func(obj, *args, **kwargs)
        stop  = time.time()
        timed = stop - start

        if fname not in tdict:
            tdict[fname] = list()
        tdict[fname].append(timed)
        tdict['__last_call'] = fname

        return ret

    return func_timer


# ------------------------------------------------------------------------------
#
class Time(object):
    '''
    This is a timing class that allows to simulate different types of clocks.

    Parameters:
    tick: This is the resolution of the clock.
    speed: This is the speed of the clock.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, tick=0.01, speed=1.0):
        self._tick    = tick
        self._speed   = speed
        self._seconds = 0.0
        self._term    = mt.Event()
        self._lock    = mt.Lock()
        self._clock   = mt.Thread(target=self._clock_thread)

        self._clock.daemon = True
        self._clock.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''
        Stops the clock
        '''
        self._term.set()
        self._clock.join()


    # --------------------------------------------------------------------------
    #
    def _clock_thread(self):

        last = time.time()

        while not self._term.is_set():

            time.sleep(self._tick)
            now = time.time()

            with self._lock:
                self._seconds += (now - last) * self._speed

            last = now


    # --------------------------------------------------------------------------
    #
    def time(self):
        '''
        Returns the current value of the clock
        '''

        return self._seconds


    # --------------------------------------------------------------------------
    #
    def sleep(self, amount):
        '''
        Sleeps for a specific amount of time units. Actual time spent is equal
        to amount divided by speed.
        '''

        time.sleep(amount / self._speed)


    # --------------------------------------------------------------------------
    #
    def advance(self, amount):
        '''
        Advance the clock with a specific amount of time units
        '''
        with self._lock:
            self._seconds += amount


# ------------------------------------------------------------------------------

