
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import subprocess    as sp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_socks():

    proxy_url = 'ssh://two.radical-project.org/'
    proxy     = ru.Proxy(url=proxy_url)
    test_url  = 'https://raw.githubusercontent.com/radical-cybertools/' \
                'radical.utils/devel/src/radical/utils/__init__.py'
    ret_url   = proxy.url(test_url, socks=True)

    cmd = 'curl --socks5 %s:%s %s' % (ret_url.host, ret_url.port, test_url)
    out = sp.check_output(cmd, shell=True, stderr=sp.STDOUT)
    print 'curl   : %s' % test_url
    print '         %s' % ret_url
    print '         %d characters' % len(out)

    assert(ret_url.host == '127.0.0.1')
    assert('version_detail' in out)

    proxy.close()


# ------------------------------------------------------------------------------
#
def test_tunnel():

    proxy_url = 'ssh://two.radical-project.org/'
    proxy     = ru.Proxy(url=proxy_url)
    test_url  = 'mongodb://am:am@ds015720.mlab.com:15720/am_rp'
    ret_url   = proxy.url(test_url, socks=False)

    _, db, _, _, _ = ru.mongodb_connect(ret_url)
    cols = list(db.collection_names())
    print 'mongodb: %s' % test_url
    print '         %s' % ret_url
    print '         %d sessions' % len(cols)

    assert(ret_url.host == '127.0.0.1')
    assert(len(cols))

    proxy.close()


# ------------------------------------------------------------------------------
#
def test_command_channel():

    proxy_url = 'ssh://two.radical-project.org/'
    proxy     = ru.Proxy(url=proxy_url, cmd='date >> /tmp/t')
    test_url  = 'ssh://two.radical-project.org/'
    ret_url   = proxy.url(test_url, socks=False)

    assert(ret_url.host == '127.0.0.1')
    assert(len(cols))

    proxy.close()


# ------------------------------------------------------------------------------
#
def test_integration():

    p1_url   =  'ssh://two.radical-project.org/'
    p2_url   =  'ssh://one.radical-project.org/'
    t1_url   =  'ssh://titan-ext1.ccs.ornl.gov/'
    rush_url = 'rush://titan-ext1.ccs.ornl.gov/'


    # create a proxy
    p1 = ru.Proxy(url=p1_url)
    p1.is_alive()
    p1.kill()
    p1.restart()
    print p1.url

    # chain proxies
    p2 = p1.chain(p2_url)
    assert(p1 == p2.proxy)

    # create a tunnel over a proxy
    t1 = p2.tunnel(t1_url, socks5=False)
    t1.is_alive()
    t1.kill()
    t1.restart()
    t1.url
    assert(p2 == t1.proxy)

    # create a command endpoint (zmq, persistent, async)
    rush = ru.SH(p2.tunnel(rush_url))
    print rush.ps()


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_integration()
    test_socks()
    test_tunnel()


# ------------------------------------------------------------------------------

