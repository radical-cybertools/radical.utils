
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils.benchmark as rb
import time


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, config) :

    if  not 'load' in config : 
        raise KeyError ('no benchmark load configured')

    return config


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, config={}) :

    time.sleep (config['load'])

    return config


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, config={}) :

    pass


# ------------------------------------------------------------------------------
#
try:
    rb.benchmark_init ('job_run', benchmark_pre, benchmark_core, benchmark_post)

except Exception as ex:
    print "An exception occured: %s " % ex


# ------------------------------------------------------------------------------


