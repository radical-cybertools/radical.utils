#!/usr/bin/env python

import sys
import time
import radical.utils as ru


def test():

    proxy_url = 'ssh://127.0.0.1/'
    proxy     = ru.Proxy(url=proxy_url)
    test_url  = 'ssh://127.0.0.1/'
    ret_url   = proxy.url(test_url, socks=False)

    assert(ret_url.host == '127.0.0.1')
    print ret_url

    proxy.close()

    sys.exit()

test()


if len(sys.argv) <= 1:
    raise ValueError('missing mode argument (send | recv)')

mode = sys.argv[1]
assert(mode in ['send', 'recv'])

if mode == 'send':

    ve        = '/home/merzky/radical.pilot.sandbox/ve.local.localhost.0.47'
    proxy_url = 'ssh://127.0.0.1/'
    proxy     = ru.Proxy(url=proxy_url)
    out, err, ret = proxy.run('source %s/bin/activate' % ve)
    out, err, ret = proxy.run('ru.sh.py recv 2>&1 > /tmp/ru.sh.log &')
    out, err, ret = proxy.run('cat /tmp/ru.sh.log')
    test_url  = out.strip()
    ret_url   = proxy.url(test_url, socks=False)

    print out
    print ret_url

    assert(ret_url.host == '127.0.0.1')

    proxy.close()

else:

    if len(sys.argv) > 2:
        uid = sys.argv[2]
        sh = ru.SH(uid)
    else:
        sh = ru.SH()
    
    print '\nshell %s' % sh.uid
    
    cmd = sh.run('sleep 10')
    cmd = sh.run('date')
    
    for cid in sh.list():
        cmd = sh.get(cid)
        print ' -- %s: [%5d] %20s [%s]' \
            % (cmd.uid, cmd.pid, cmd.cmd, cmd.state)

# ------------------------------------------------------------------------------

