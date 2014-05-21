
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru
import sys
import time


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, config) :

    if  not 'arguments' in config : 
        raise KeyError ('no benchmark arguments configured')

    return config


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, config={}) :

    time.sleep (float(config['arguments'][0]))

    return config


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, config={}) :

    pass


# ------------------------------------------------------------------------------
#
cfg = sys.argv[1]
b = ru.Benchmark (cfg, 'job_run', benchmark_pre, benchmark_core, benchmark_post)
b.run  ()
b.eval ()


# ------------------------------------------------------------------------------


