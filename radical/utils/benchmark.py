
import os
import sys
import math
import time
import socket

import radical.utils.threads     as rut
import radical.utils.test_config as rutc


# --------------------------------------------------------------------
#
def benchmark_init (name, func_pre, func_core, func_post) :

    _benchmark = {}

    rut.lout ('session was set up\n')

    # check if a config file was specified via '-c' command line option, and
    # read it, return the dict

    config_name = None

    for i, arg in enumerate (sys.argv[1:]) :
        if  arg == '-c' and len (sys.argv) > i+2 :
            config_name = sys.argv[i+2]


    if  not config_name :
        benchmark_eval (_benchmark, 'no configuration specified (-c <conf>')

    tc   = rutc.TestConfig ()
    tc.read_config (config_name)

    test_cfg  = tc.get_test_config ()
    bench_cfg = tc.get_benchmark_config ()
    session   = tc.session

    # SAGA_BENCHMARK_ environments will overwrite config settings
    if  'SAGA_BENCHMARK_CONCURRENCY' in os.environ :
        bench_cfg['concurrency'] = os.environ['SAGA_BENCHMARK_CONCURRENCY']

    if  'SAGA_BENCHMARK_ITERATIONS' in os.environ :
        bench_cfg['iterations'] = os.environ['SAGA_BENCHMARK_ITERATIONS']

    if  'SAGA_BENCHMARK_LOAD' in os.environ :
        bench_cfg['load'] = os.environ['SAGA_BENCHMARK_LOAD']

    
    # check benchmark settings for completeness, set some defaults
    if  not 'concurrency' in bench_cfg : 
        benchmark_eval (_benchmark, 'no concurrency configured')

    if  not 'iterations'  in bench_cfg : 
        benchmark_eval (_benchmark, 'no iterations configured')

    _benchmark['session']   = session
    _benchmark['test_cfg']  = test_cfg
    _benchmark['bench_cfg'] = bench_cfg

    _benchmark['bench_cfg']['pre']   = func_pre
    _benchmark['bench_cfg']['core']  = func_core
    _benchmark['bench_cfg']['post']  = func_post
    _benchmark['bench_cfg']['name']  = name
    _benchmark['bench_cfg']['cname'] = config_name

    benchmark_run  (_benchmark)
    benchmark_eval (_benchmark)

# ------------------------------------------------------------------------------
#
def benchmark_thread (tid, _benchmark) :

    t_cfg    = _benchmark['test_cfg']
    b_cfg    = _benchmark['bench_cfg']
    session  = _benchmark['session']
    lock     = _benchmark['lock']

    pre      = b_cfg['pre']
    core     = b_cfg['core']
    post     = b_cfg['post']

    try :
        pre_ret  = pre (tid, t_cfg, b_cfg, session)
        sys.stdout.write ('-')
        sys.stdout.flush ()


        _benchmark['events'][tid]['event_1'].set  ()  # signal we are done        
        _benchmark['events'][tid]['event_2'].wait ()  # wait 'til others are done 

        iterations = int(b_cfg['iterations']) / int(b_cfg['concurrency'])

        # poor-mans ceil()
        if (iterations * int(b_cfg['concurrency'])) < int(b_cfg['iterations']) :
            iterations += 1

        for i in range (0, iterations+1) :
            core_ret = core (tid, i, pre_ret)
            benchmark_tic   (_benchmark, tid)


        _benchmark['events'][tid]['event_3'].set ()   # signal we are done        
        _benchmark['events'][tid]['event_4'].wait ()  # wait 'til others are done 


        post_ret = post (tid, core_ret)
        sys.stdout.write ('=')
        sys.stdout.flush ()

        _benchmark['events'][tid]['event_5'].set ()   # signal we are done        

    except Exception as e :

        sys.stdout.write ("exception in benchmark thread: %s\n\n" % e)
        sys.stdout.flush ()

        # Oops, we are screwed.  Tell main thread that we are done for, and
        # bye-bye...
        _benchmark['events'][tid]['event_1'].set  ()  # signal we are done        
        _benchmark['events'][tid]['event_3'].set  ()  # signal we are done        
        _benchmark['events'][tid]['event_5'].set  ()  # signal we are done        

        sys.exit (-1)

    sys.exit (0)


# ------------------------------------------------------------------------------
#
def benchmark_run (_benchmark) :
    """
    - create 'concurrency' number of threads
    - per thread call pre()
    - sync threads, start timer
    - per thread call core() 'iteration' number of times', tic()
    - stop timer
    - per thread, call post, close threads
    - eval once
    """

    cfg         = _benchmark['bench_cfg']
    threads     = []
    concurrency = int(_benchmark['bench_cfg']['concurrency'])

    benchmark_start (_benchmark)

    _benchmark['events'] = {}

    for tid in range (0, concurrency) :

        _benchmark['events'][tid] = {}
        _benchmark['events'][tid]['event_1'] = rut.Event ()
        _benchmark['events'][tid]['event_2'] = rut.Event ()
        _benchmark['events'][tid]['event_3'] = rut.Event ()
        _benchmark['events'][tid]['event_4'] = rut.Event ()
        _benchmark['events'][tid]['event_5'] = rut.Event ()

        _benchmark['start'][tid] = time.time ()
        _benchmark['times'][tid] = []

        t = rut.SagaThread (benchmark_thread, tid, _benchmark)
        threads.append (t)


    for t in threads :
        t.start ()

    
    # wait for all threads to start up and initialize
    _benchmark['t_init'] = time.time ()
    rut.lout ("\n> " + "="*concurrency)
    rut.lout ("\n> ")
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_1'].wait ()

    # start workload in all threads
    _benchmark['t_start'] = time.time ()
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_2'].set ()

    # wait for all threads to finish core test
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_3'].wait ()
    _benchmark['t_stop'] = time.time ()

    # start shut down
    rut.lout ("\n< " + "-"*concurrency)
    rut.lout ("\n< ")
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_4'].set ()

    # wait for all threads to finish shut down
    for tid in range (0, concurrency) :
        _benchmark['events'][tid]['event_5'].wait ()



# --------------------------------------------------------------------
#
def benchmark_start (_benchmark) :

    cfg = _benchmark['bench_cfg']

    _benchmark['lock']  = rut.RLock ()
    _benchmark['start'] = {}
    _benchmark['times'] = {}
    _benchmark['idx']   = 0

    rut.lout ("\n")
    rut.lout ("Benchmark   : %s\n" % (cfg['name']))
    rut.lout ("concurrency : %s\n" %  cfg['concurrency'])
    rut.lout ("iterations  : %s\n" %  cfg['iterations'])
    rut.lout ("load        : %s\n" %  cfg['load'])

    sys.stdout.flush ()



# --------------------------------------------------------------------
#
def benchmark_tic (_benchmark, tid='master_tid') :

    with _benchmark['lock'] :

        now   = time.time ()
        timer = now - _benchmark['start'][tid]

        _benchmark['times'][tid].append (timer)
        _benchmark['start'][tid] = now

        if len(_benchmark['times'][tid][1:]) :
            vmean = sum (_benchmark['times'][tid][1:]) / len(_benchmark['times'][tid][1:])
        else :
            vmean = timer

        if   timer  <  0.75 * vmean : marker = '='
        if   timer  <  0.90 * vmean : marker = '~'
        elif timer  <  0.95 * vmean : marker = '_'
        elif timer  <  1.05 * vmean : marker = '.'
        elif timer  <  1.10 * vmean : marker = '-'
        elif timer  <  1.25 * vmean : marker = '+'
        else                        : marker = '*'


        if       not ( (_benchmark['idx'])        ) : sys.stdout.write ('\n* ')
        else :
            if   not ( (_benchmark['idx']) % 1000 ) : sys.stdout.write (" %7d\n\n# " % _benchmark['idx'])
            elif not ( (_benchmark['idx']) %  100 ) : sys.stdout.write (" %7d\n| " % _benchmark['idx'])
            elif not ( (_benchmark['idx']) %   10 ) : sys.stdout.write (' ')
        if  True                                    : sys.stdout.write (marker)
        
        sys.stdout.flush ()

        _benchmark['idx'] += 1

# --------------------------------------------------------------------
#
def benchmark_eval (_benchmark, error=None) :

    if  error :
        rut.lout ("\nBenchmark error: %s\n" % error)
        sys.exit (-1)

    times = []

    for tid in _benchmark['times'] :
        times += _benchmark['times'][tid][1:]

    if  len(times) < 1 :
        raise ValueError ("min 1 timing value required for benchmark evaluation (%d)" % len(times))

    concurrency = int(_benchmark['bench_cfg']['concurrency'])
    load        = int(_benchmark['bench_cfg']['load'])

    out = "\n"
    top = ""
    tab = ""
    num = ""

    out += "Results :\n"

    vtot  = _benchmark['t_stop']  - _benchmark['t_start']
    vini  = _benchmark['t_start'] - _benchmark['t_init']
    vn    = len (times)
    vsum  = sum (times)
    vmin  = min (times)
    vmax  = max (times)
    vmean = sum (times) / vn
    vsdev = math.sqrt (sum ((x - vmean) ** 2 for x in times) / vn)
    vrate = vn / vtot

    bname = _benchmark['bench_cfg']['name']
    bid   = "%s.%s" % (burl.scheme, burl.host)
    bdat  = "benchmark.%s.%s.dat" % (bname, bid)

    out += "  ping    : %8.5fs\n"                            % (_benchmark['ping'])
    out += "  threads : %9d          load    : %9d\n"        % (concurrency, load )
    out += "  iterats.: %9d          min     : %8.2fs\n"     % (vn,          vmin )
    out += "  init    : %8.2fs          max     : %8.2fs\n"  % (vini,        vmax )
    out += "  total   : %8.2fs          mean    : %8.2fs\n"  % (vtot,        vmean)
    out += "  rate    : %8.2fs          sdev    : %8.2fs\n"  % (vrate,       vsdev)

    num = "# %5s  %7s  %7s  %7s  %7s  %7s  %7s  %7s  %8s  %8s  %9s   %-18s" \
        % (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    top = "# %5s  %7s  %7s  %7s  %7s  %7s  %7s  %7s  %8s  %8s  %9s   %-18s" \
        % ('ping', 'n', 'threads', 'load', 'init', 'tot', 'min',  'max', 'mean', \
           'std-dev', 'rate', 'name')

    tab = "%7.5f  " \
          "%7d  "   \
          "%7d  "   \
          "%7d  "   \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%7.2f  " \
          "%8.3f  " \
          "%8.3f  " \
          "%9.3f  " \
          "%-20s  " \
        % (_benchmark['ping'], 
           vn, 
           concurrency, 
           load, 
           vini,
           vtot,   
           vmin,  
           vmax, 
           vmean, 
           vsdev, 
           vrate, 
           bname)

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
#
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

