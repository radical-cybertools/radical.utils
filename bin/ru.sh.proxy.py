#!/usr/bin/env python

import os
import sys
import optparse

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def usage (error=None, noexit=False) :

    if  error :
        print "\n\terror: %s\n" % error

    print '''
    synopsis: Create a proxy tunnel using the given proxy URL.
              A tunnel is given a `name` to identify.  
              Tunnel states are stored in `$HOME/.radical/utils/proxies/`.

    usage   : %(cmd)s name -m <start|translate|stop> [-u url] [-h]

    examples: %(cmd)s name -m start     -u  ssh://10.0.0.2/
              %(cmd)s name -m translate -u http://10.0.0.2:631/
              %(cmd)s name -m stop

    ''' % {'cmd': sys.argv[0]}

    if  error:
        sys.exit (1)

    if  not noexit :
        sys.exit (0)


# ------------------------------------------------------------------------------
#
def start(name, url):
    pass


# ------------------------------------------------------------------------------
#
def translate(name, url):
    pass


# ------------------------------------------------------------------------------
#
def stop(name):
    pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__' :

    parser = optparse.OptionParser (add_help_option=False)

    parser.add_option('-m', '--mode',    dest='mode')
    parser.add_option('-u', '--url',     dest='url')
    parser.add_option('-h', '--help', dest='help', action="store_true")

    options, args = parser.parse_args ()

    if  options.help : usage ()

    if not args      : usage ("missing proxy name") 
    if len(args) > 1 : usage ("Too many arguments (%s)" % args) 

    name = args[0]
    mode = options.mode 
    url  = options.url

    if mode in ['start', 'translate']:
        if not url : url  = os.environ.get('RADICAL_PROXY_URL')
        if not url : usage('no proxy url and $RADICAL_PROXY_URL is unset')

    print "name: %s" % name
    print "mode: %s" % mode
    if url:
        print "url : %s" % url

    if   mode == 'start'    : start     (name, url) 
    elif mode == 'translate': translate (name, url) 
    elif mode == 'stop'     : stop      (name) 
    else                    : usage ("unknown mode '%s'" % mode)


# ------------------------------------------------------------------------------

