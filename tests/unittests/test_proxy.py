
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import subprocess    as sp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_socks():

    proxy_url = 'ssh://144.76.72.175/'
    proxy     = ru.Proxy(url=proxy_url)
    test_url  = 'https://raw.githubusercontent.com/radical-cybertools/' \
                'radical.utils/devel/src/radical/utils/__init__.py'
    ret_url   = proxy.url(test_url, socks=True)

    cmd = 'curl --socks5 %s:%s %s' % (ret_url.host, ret_url.port, test_url)
    out = sp.check_output(cmd, shell=True, stderr=sp.STDOUT)

    assert(ret_url.host == '127.0.0.1')
    assert('version_detail' in out)

    proxy.close()


# ------------------------------------------------------------------------------
#
def test_tunnel():

    proxy_url = 'ssh://144.76.72.175/'
    proxy     = ru.Proxy(url=proxy_url)
    test_url  = 'mongodb://am:am@ds015720.mlab.com:15720/am_rp'
    ret_url   = proxy.url(test_url, socks=False)

    assert(ret_url.host == '127.0.0.1')
    _, db, _, _, _ = ru.mongodb_connect(ret_url)
    cols = list(db.collection_names())
    assert(len(cols))
    proxy.close()



# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_socks()
    test_tunnel()

# ------------------------------------------------------------------------------

