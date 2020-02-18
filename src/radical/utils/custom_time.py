#!/usr/bin/env python3
"""
Author: Andre Merzky, Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import time
import threading as mt

class Time(object):
    '''
    This is a timing class that allows to simulate different types of clocks.

    Parameters:
    tick: This is the resolution of the clock.
    speed: This is the speed of the clock.
    '''

    def __init__(self, tick=0.01, speed=1.0):
        self._tick    = tick
        self._speed   = speed
        self._seconds = 0.0
        self._term    = mt.Event()
        self._lock    = mt.Lock()
        self._clock   = mt.Thread(target=self._clock_thread)
        self._clock.daemon = True
        self._clock.start()

    def stop(self):
        '''
        Stops the clock
        '''
        self._term.set()
        self._clock.join()

    def _clock_thread(self):
        last = time.time()
        while not self._term.is_set():
            time.sleep(self._tick)
            now = time.time()
            with self._lock:
                self._seconds += (now - last) * self._speed
            last = now

    def time(self):
        '''
        Returns the current value of the clock
        '''
        return self._seconds

    def sleep(self, amount):
        '''
        Sleeps for a specific amount of time units. Actual time spent is equal
        to amount divided by speed.
        '''
        
        now = self._seconds

        while self._seconds < now + amount:
            time.sleep(1 / self._speed)

    def advance(self, amount):
        '''
        Advance the clock with a specific amount of time units
        '''
        with self._lock:
            self._seconds += amount