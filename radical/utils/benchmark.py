
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import math
import numpy
import time
import socket
import threading

import threads as rut
import testing as rutest


# --------------------------------------------------------------------
#
class Benchmark (object) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, config, name, func_pre, func_core, func_post) :
    
        self.pre    = func_pre
        self.core   = func_core
        self.post   = func_post
        self.name   = name
        self.lock   = rut.RLock ()
        self.events = dict()
    
        if  isinstance (config, basestring) :
            cfg = rutest.TestConfig (config)
        elif isinstance (config, dict) :
            cfg = config
        else :
            print 'warning: no valid benchmarl config (neither filename nor dict) -- ignore'
            cfg = dict()

        if  'benchmarks' in cfg :
            cfg = cfg['benchmarks']

    
        # RADICAL_BENCHMARK_ environments will overwrite config settings
        if  'RADICAL_BENCHMARK_CONCURRENCY' in os.environ :
            cfg['concurrency'] = os.environ['RADICAL_BENCHMARK_CONCURRENCY']
    
        if  'RADICAL_BENCHMARK_ITERATIONS' in os.environ :
            cfg['iterations'] = os.environ['RADICAL_BENCHMARK_ITERATIONS']
    
        if  'RADICAL_BENCHMARK_ARGUMENTS' in os.environ :
            cfg['arguments'] = eval(os.environ['RADICAL_BENCHMARK_ARGUMENTS'])
    
        
        # check benchmark settings for completeness, set some defaults
        if  not 'concurrency' in cfg : 
            raise ValueError ('no concurrency configured')
    
        if  not 'iterations'  in cfg : 
            raise ValueError ('no iterations configured')
    
        if  not 'arguments' in cfg : 
            raise ValueError ('no arguments configured')
    
        if  not 'tags' in cfg : 
            cfg['tags'] = ""
    
        self.cfg = cfg
    

    # --------------------------------------------------------------------------
    #
    def _thread (self, tid) :
    
        try :
            pre_ret  = self.pre (tid, self.cfg)

            sys.stdout.write ('-')
            sys.stdout.flush ()
    
            self.events[tid]['event_1'].set  ()  # signal we are done        
            self.events[tid]['event_2'].wait ()  # wait 'til others are done 
    
            iterations = int(self.cfg['iterations']) / int(self.cfg['concurrency'])
    
            # poor-mans ceil()
            if (iterations * int(self.cfg['concurrency'])) < int(self.cfg['iterations']) :
                iterations += 1
    
            for i in range (0, iterations+1) :
                core_ret = self.core (tid, i, pre_ret)
                self._tic (tid)
    
    
            self.events[tid]['event_3'].set ()   # signal we are done        
            self.events[tid]['event_4'].wait ()  # wait 'til others are done 
    
    
            post_ret = self.post (tid, core_ret)
            sys.stdout.write ('=')
            sys.stdout.flush ()
    
            self.events[tid]['event_5'].set ()   # signal we are done        

    
        except Exception as e :
    
            sys.stdout.write ("exception in benchmark thread: %s\n\n" % str(e))
            sys.stdout.flush ()
    
            # Oops, we are screwed.  Tell main thread that we are done for, and
            # bye-bye...
            self.events[tid]['event_1'].set  ()
            self.events[tid]['event_3'].set  ()
            self.events[tid]['event_5'].set  ()

            raise (e)
    
            sys.exit (-1)
    
        # finish thread
        sys.exit (0)
    
    
    # --------------------------------------------------------------------------
    #
    def run (self) :
        """
        - create 'concurrency' number of threads
        - per thread call pre()
        - sync threads, start timer
        - per thread call core() 'iteration' number of times', tic()
        - stop timer
        - per thread, call post, close threads
        - eval once
        """
    
        threads     = []
        concurrency = int(self.cfg['concurrency'])
    
        self._start ()
    
        for tid in range (0, concurrency) :
    
            self.events[tid] = {}
            self.events[tid]['event_1'] = rut.Event ()
            self.events[tid]['event_2'] = rut.Event ()
            self.events[tid]['event_3'] = rut.Event ()
            self.events[tid]['event_4'] = rut.Event ()
            self.events[tid]['event_5'] = rut.Event ()
            self.start [tid] = time.time ()
            self.times [tid] = list()
    
            t = rut.Thread (self._thread, tid)
            threads.append (t)
    
    
        for t in threads :
            t.start ()
    
        
        # wait for all threads to start up and initialize
        self.t_init = time.time ()
        rut.lout ("\n> " + "="*concurrency)
        rut.lout ("\n> ")
        for tid in range (0, concurrency) :
            self.events[tid]['event_1'].wait ()
    
        # start workload in all threads
        self.t_start = time.time ()
        for tid in range (0, concurrency) :
            self.events[tid]['event_2'].set ()
    
        # wait for all threads to finish core test
        for tid in range (0, concurrency) :
            self.events[tid]['event_3'].wait ()
        self.t_stop = time.time ()
    
        # start shut down
        rut.lout ("\n< " + "-"*concurrency)
        rut.lout ("\n< ")
        for tid in range (0, concurrency) :
            self.events[tid]['event_4'].set ()
    
        # wait for all threads to finish shut down
        for tid in range (0, concurrency) :
            self.events[tid]['event_5'].wait ()
    
    
    
    # ----------------------------------------------------------------
    #
    def _start (self) :
    
        self.start = dict()
        self.times = dict()
        self.idx   = 0
    
        rut.lout ("\n")
        rut.lout ("benchmark   : %s (%s)\n" % (self.name, self.cfg['tag']))
        rut.lout ("concurrency : %s\n"      %  self.cfg['concurrency'])
        rut.lout ("iterations  : %s\n"      %  self.cfg['iterations'])
        rut.lout ("arguments   : %s\n"      %  self.cfg['arguments'])
    
        sys.stdout.flush ()
    
    
    # ----------------------------------------------------------------
    #
    def _tic (self, tid='master_tid') :
    
        with self.lock :

            now   = time.time ()
            timer = now - self.start[tid]

            self.times[tid].append (timer)
            self.start[tid] = now

            if len(self.times[tid][1:]) :
                numpy_times = numpy.array (self.times[tid][1:])
                vmean = numpy_times.mean ()
            else :
                vmean = timer
    
            if   timer  <  0.75 * vmean : marker = '='
            if   timer  <  0.90 * vmean : marker = '~'
            elif timer  <  0.95 * vmean : marker = '_'
            elif timer  <  1.05 * vmean : marker = '.'
            elif timer  <  1.10 * vmean : marker = '-'
            elif timer  <  1.25 * vmean : marker = '+'
            else                        : marker = '*'
    
    
            if       not ( (self.idx)        ) : sys.stdout.write ('\n* ')
            else :
                if   not ( (self.idx) % 1000 ) : sys.stdout.write (" %7d\n\n# " % self.idx)
                elif not ( (self.idx) %  100 ) : sys.stdout.write (" %7d\n| "   % self.idx)
                elif not ( (self.idx) %   10 ) : sys.stdout.write (' ')
            if  True                                    : sys.stdout.write (marker)
            
            sys.stdout.flush ()
    
            self.idx += 1
    
    # ----------------------------------------------------------------
    #
    def eval (self, error=None) :
    
        times = list()
    
        for tid in self.times :
            times += self.times[tid][1:]

      # import pprint
      # pprint.pprint (times)
    
        if  len(times) < 1 :
            raise ValueError ("min 1 timing value required for benchmark evaluation (%d)" % len(times))
    
        concurrency = int(self.cfg['concurrency'])
        args        = str(self.cfg['arguments'])
        tags        = str(self.cfg['tags'])
    
        out = "\n"
        top = ""
        tab = ""
        num = ""
    
        out += "Results :\n"

        numpy_times = numpy.array (times)
    
        vtot  = self.t_stop  - self.t_start
        vini  = self.t_start - self.t_init
        vn    = len (times)
        vsum  = sum (times)
        vmin  = min (times)
        vmax  = max (times)

        if len (times) :
            vmean = numpy_times.mean ()
            vsdev = numpy_times.std  ()
        else :
            vmean = 0.0
            vsdev = 0.0
        vrate = vn / vtot
    
        bdat  = "benchmark.%s.dat" % (self.name)
    
        out += "  name    : %s\n"                               % (self.name)
        out += "  tags    : %s\n"                               % (tags)
        out += "  args    : %s\n"                               % (args)
        out += "  threads : %8d        iters   : %8d\n"        % (concurrency, vn)
        out += "  init    : %8.2fs       min     : %8.2fs\n"    % (vini,        vmin )
        out += "  total   : %8.2fs       max     : %8.2fs\n"    % (vtot,        vmax )
        out += "  rate    : %8.2fs       mean    : %8.2fs\n"    % (vrate,       vmean)
        out += "                            sdev    : %8.2fs\n" % (             vsdev)
    
        num = "# %7s  %7s  %7s  %7s  %7s  %7s  %8s  %8s  %9s  %10s  %10s" \
            % (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        top = "# %7s  %7s  %7s  %7s  %7s  %7s  %8s  %8s  %9s  %10s  %10s" \
            % ('n', 'threads', 'init', 'tot', 'min',  'max', 'mean', \
               'std-dev', 'rate', 'name', 'args')
    
        tab = "  "      \
              "%7d  "   \
              "%7d  "   \
              "%7.2f  " \
              "%7.2f  " \
              "%7.2f  " \
              "%7.2f  " \
              "%8.3f  " \
              "%8.3f  " \
              "%9.3f  " \
              "%10s  "   \
              "%10s  "   \
            % (vn, 
               concurrency, 
               vini,
               vtot,   
               vmin,  
               vmax, 
               vmean, 
               vsdev, 
               vrate, 
               self.name,
               str(args)) 
    
        rut.lout ("\n%s" % out)
    
        create_top = True
        try :
            statinfo = os.stat (bdat)
            if  statinfo.st_size > 0 :
                create_top = False
        except :
            pass
    
        f = open (bdat, "a+")
    
        if  create_top :
            f.write ("%s\n" % num)
            f.write ("%s\n" % top)
        f.write ("%s\n" % tab)
    
    
# --------------------------------------------------------------------

