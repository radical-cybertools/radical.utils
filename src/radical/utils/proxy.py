
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import fcntl
import psutil
import signal
import socket

import subprocess as sp

from .url   import Url
from .which import which


# ------------------------------------------------------------------------------
#
class Proxy(object):
    '''
    We frequently face the problem that a network connection cannot be directly
    established due to firewall policies.  At the same time, establishing ssh
    tunnels can be cumbersome, as they need activity by every user, need
    coordination of used port numbers, out-of-band communication of tunnel
    settings, etc.

    This class eleviates that problem for te skope of the RADICAL stack.  It
    expects two user level settings:

        export RADICAL_PROXY_URL=ssh://host/

    to specify a suitable tunnel host.  When being used by some layer in the
    radical stack, this class will:

        - search a free local port
        - establish a SOCKS5 tunnel to the given host, binding it to the given
          port
        - additionally establish any required (non-SOCKS) application tunnel
          over the SOCKS tunnel

    The existence of the SOCKS tunnel is recorded in the file

        `$HOME/.radical/utils/proxy.<host>[.<name>]`
        
    which contains information about the used local port.  That file will
    disappear when the proxy disappears.  The `[.<name>]` part is used for
    direct application tunnels established over the original socks proxy tunnel.
    `name` is expected to be a unique identifyer.  The proxy is expected to live
    until is explicitly closed.

    
    Requirements:
    -------------

      * passwordless ssh login needs to be configured from localhost to the
        proxy host
      * the proxy host must have these settings in `/etc/ssh/sshd_config`:

            GatewayPorts       yes
            AllowTcpForwarding yes


    Usage:
    ------

        # connect to `http://www.google.com/` (port 80) with SOCKS enabled client
        proxy = ru.Proxy(timeout=300)
        client.connect(proxy.url(socks=True, 'http://www.google.com/'))
        print proxy.url(socks=True, 'http://www.google.com/')
          --> http://localhost:10000/

        # connect to `mongodb://www.mlab.com:12017/rp` with client which is
        # *not* able to use SOCKS
        proxy = ru.Proxy()
        print proxy.url(socks=False, `mongodb://www.mlab.com:12017/rp`)
          --> mongodb://localhost:10001/rp

        proxy.close() / gc


    We  will also provide a command line tool which supports similar operations
    in the shell:

        # connect to `http://www.google.com/` (port 80) with SOCKS enabled client
        wget `radical-proxy --socks=True 'http://www.google.com/'`

        # connect to `mongodb://foobar.mlab.com:12017/rp` with client which is
        # *not* able to use SOCKS
        mongo `radical-proxy --socks=False 'foobar.mlab.com:12017'`
    '''

    
        # nc -X 5 -x 127.0.0.1:10000 %h %p
        # ncat --proxy-type socks5 --proxy 127.0.0.1:10000 %h %p
        # netcat -X 5 -x 127.0.0.1:10000 %h %p
        # socat - socks:127.0.0.1:%h:%p,socksport=10000
    _tunnel_proxies = {
        'ncat'   : '%(tunnel_exe)s --proxy-type socks5 '\
                                  '--proxy 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'nc'     : '%(tunnel_exe)s -X 5 -x 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'netcat' : '%(tunnel_exe)s -X 5 -x 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'socat'  : '%(tunnel_exe)s - socks:127.0.0.1:%%%%h:%%%%p,'\
                                    'socksport=%(proxy_port)d'
        }

    # --------------------------------------------------------------------------
    #
    def __init__(self, url=None, timeout=None):
        '''
        Create a new Proxy instance.  This might open to a new proxy, or attach
        to an existing one.  We find existing proxies by inspecting the file
        `$HOME/.radical/utils/proxy.<proxy-host>` which contains pid and port of
        the proxy process.
        '''

        # keep track of created tunnels
        self._tunnels        = list()
        self._tunnel_command = None

        # TODO: perform some kind of time limited or activity dependent garbage
        #       collection.  Posslubly use a lifetime parameter?

        if url:
            proxy_url = Url(url)
        else:
            proxy_url = Url(os.environ.get('RADICAL_PROXY_URL'))

        if proxy_url:
            url_host  = proxy_url.host
            url_port  = proxy_url.port
            if not url_port:
                try:
                    url_port = socket.getservbyname(proxy_url.schema)
                except socket.error as e:
                    raise ValueError('cannot handle "%s" urls' % proxy_url.schema)
        else:
            url_host  = None
            url_port  = None

        self._proxy_host = None
        self._proxy_port = None
        self._proxy_pid  = None

        if not proxy_url:
            # we can't really create a proxy, so continue as is.
            return

        proxy_path = '%s/.radical/utils' % os.environ['HOME']

        try:
            os.makedirs(proxy_path)
        except:
            pass

        self._proxy_file = '%s/proxy_%s_%s' % (proxy_path, url_host, url_port)

        fd = None 
        try:
            # open the file or create it, then lock it, then read from
            # beginning.  We expect two integers, pid and port of an existing
            # proxy.  If those are found, we close the file (which unlocks it).
            # If they are not found, we create the proxy, write the information
            # to the file, and close it (which also unlocks it).
            #
            # NOTE: that there is a race condition between checking for an
            #       existing proxy and using it: the proxy might disappear 
            #       meanwhile. Since we have no control over that time, we make
            #       no guarantees wrt. proxy health whatsoever.  It is the
            #       aplication's responsibility to rensure sufficient proxy
            #       lifetime.
            #
            fd = os.open(self._proxy_file, os.O_RDWR | os.O_CREAT)
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.lseek(fd, 0, os.SEEK_SET )

            try:
                # assume proxy exists
                data = os.read(fd, 512) # '%06d %06d\n'
                str_1, int_1, int_2, = data.split()
                self._proxy_host = str(str_1.strip())
                self._proxy_port = int(int_1.strip())
                self._proxy_pid  = int(int_2.strip())

                # make sure we see that pid alive
                os.kill(self._proxy_pid, 0)
                # pid can be signalled, so is assumed alive
                
            except Exception as e:
                # either data were not present or invalid, or pid cannot be
                # signalled, either way, we can't use the proxy.
                self._proxy_host = None
                self._proxy_port = None
                self._proxy_pid  = None

            # check if a new proxy is needed
            if not self._proxy_host or \
               not self._proxy_port or \
               not self._proxy_pid     :

                port_min = None
                while True:

                    # we want a new proxy, only accessible from localhost
                    self._proxy_host = url_host
                    self._proxy_port = self._find_port(interface='127.0.0.1',
                                                       port_min=port_min)

                    if not self._proxy_port:
                        raise RuntimeError('Could not find a free port to use')
                    
                    cmd = 'ssh -o ExitOnForwardFailure=yes ' \
                        +     '-o StrictHostKeyChecking=no ' \
                        +     '-fND %d -p %s %s' \
                        % (self._proxy_port, url_port, url_host)
                    try: 
                        sp.check_call(cmd, shell=True)

                        # find the pid
                        for c in psutil.net_connections(kind='tcp'):
                            if c.laddr[0] == '127.0.0.1' and \
                               c.laddr[1] == self._proxy_port:
                                    self._proxy_pid = c.pid
                                    break

                        # we are done, break while(True)
                        break

                    except sp.CalledProcessError as e:

                        if 'bind: Address already in use' in e.output:
                            # try again with a higher port number
                            port_min = self._proxy_port + 1
                            continue

                        else:
                            raise

                assert(self._proxy_host)
                assert(self._proxy_port)
                assert(self._proxy_pid)

                # store new proxy parameters
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, "%s %d %d\n" % (self._proxy_host,
                                             self._proxy_port,
                                             self._proxy_pid))

        finally:
            if fd:
                # this releases the lock
                os.close(fd)


        # Now that we have a proxy, we can configure the tunnel command for
        # later use on `url(socks=False)`
        #
        # Check for the availablity of various utilities which help to tunnel ssh
        # over a SOCKS proxy.  See documentation and code comments in `self.url()`
        # for details.
        tunnel_proxy = None
        for name in self._tunnel_proxies:
            exe = which(name)
            if exe:
                tunnel_proxy = self._tunnel_proxies[name] \
                             % {'tunnel_exe' : exe, 
                                'proxy_port' : self._proxy_port}
                break

        # NOTE: user/pass info are not supported for tunneled connections
        self._tunnel_cmd = 'ssh -o ExitOnForwardFailure=yes ' \
                         +    ' -o StrictHostKeyChecking=no ' \
                         +    ' -fNL %(loc_port)d:%(url_host)s:%(url_port)d %(proxy_host)s'

        # if we have tunnel proxy support, it becomes part of the tunnel proxy
        # command
        if tunnel_proxy: 
            self._tunnel_cmd += " -o ProxyCommand='%s'" % tunnel_proxy


    # --------------------------------------------------------------------------
    #
    def _find_port(self, interface=None, port_min=None, port_max=None):
        '''
        Inspect the OS for about tcp connection usage, and pick a port in the
        private, ephemeral port range between 49152 and 65535.
        from those which are not used.  By default we check all interfaces, but
        an interface can also be specified as optional argument, as IP number.

        Note that this mechanism does not *reserve* a port, so there is a race
        between finding a free port and using it.
        '''

        used_adr     = [c.laddr for c in psutil.net_connections(kind='tcp')]
        if interface:
            used_adr = [x       for x in used_adr if x[0] == interface]
        used_ports   = [x[1]    for x in used_adr]

        if not port_min: port_min = 49152
        if not port_max: port_max = 65535
        for port in range(port_min, port_max):
            if port not in used_ports:
                return port

        return None


    # --------------------------------------------------------------------------
    #
    def _find_port_alt(self, interface=None):
        '''
        Let the OS choose an open port for us and return it.  By default, we
        listen on all interfaces, but an interface can also be specified as
        optional argument, as IP number.

        Note that this mechanism does not *reserve* a port, so there is a race
        between finding a free port and using it.

        We don't bother catching any system errors: if we can't find a port this
        way, its unlikely that it can be found any other way
        '''

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("",0))
        port = sock.getsockname()[1]
        sock.close()
        return port


    # --------------------------------------------------------------------------
    #
    def url(self, url, socks=True):
        '''
        This method accepts an URL to which the callee wants to connect, using
        the proxy we own (or at least interface to).  The call with translate
        the given URL into a suitable URL which points to the proxy.

        If the callee intents to use the URL with a SOCKS5 enables application,
        we only replace host and port to point to our socks proxy, and return
        the resulting URL.

        If the application is *not* SOCKS5 enabled, then such a socks URL would
        be unusable.  Instead, we create a separate dedicated ssh tunnel *over
        the socks tunnel*, and return that specific URL.  That will now bind to
        a specific port.  One condition is to be able to derive the target port
        from the given URL (in case it is not explicitly specified in the URL).
        For that we perform a service port lookup via `socket.getservbyname()`.

        Note that in the `socks=False` case, we create a new tunnel for each
        request , even if a tunnel for the same target endpoint was already
        created before, as we do not know if multiple applications are able to
        share the same tunnel.  They usually are though, and future versions of
        this method may add a `reuse` flag (defaulting to `True`).

        All tunnels will be closed on `proxy.close()`.
        '''

        # Here are the different options to create an ssh tunnel over the socks
        # proxy.  Ironically, ssh can natively create, but not use a socks
        # proxy.  We thus need to specify an external proxy command.  We can
        # currently handle: `nc`, `ncat`, `netcat`, `socat`, which are all
        # different variations of the original BSD `netcat` I believe:
        #
        #   ssh -vNL 10001:ds015720.mlab.com:15720 144.76.72.175 \
        #       -o ProxyCommand='nc -X 5 -x 127.0.0.1:10000 %h %p'
        #   ssh -vNL 10001:ds015720.mlab.com:15720 144.76.72.175 \
        #       -o ProxyCommand='netcat -X 5 -x 127.0.0.1:10000 %h %p'
        #   ssh -vNL 10001:ds015720.mlab.com:15720 144.76.72.175 \
        #       -o ProxyCommand='ncat --proxy-type socks5 --proxy 127.0.0.1:10000 %h %p'
        #   ssh -vNL 10001:ds015720.mlab.com:15720 144.76.72.175 \
        #       -o ProxyCommand='socat - socks:127.0.0.1:%h:%p,socksport=10000'
        #
        # If we don't find any of those tools, we will create a separate ssh
        # tunnel connction, which then does *not* use the socks proxy.  This
        # will keep another ssh connection open though:
        #
        #   ssh -vNL 10001:ds015720.mlab.com:15720 144.76.72.175 \
        #
        # 'tsocks' would be another option, but it is less widely deployed, 
        # relies on `LD_PRLOAD`, which won't work on all our machines and is
        # somewhat hacky (little more so than the others that is).
        #
        # The above examples assume self._proxy_port to be 10000, and 10001 to
        # be the next open port found.

        # make sure we have an URL
        url      = Url(url)
        url_port = url.port
        url_host = url.host

        # if we have no proxy, we return the url as-is
        if not self._proxy_pid or not self._proxy_port:
            # nothing to do
            return Url(url)

        # if the application has socks support, we simply exchange host and
        # port in the url
        if socks:
            url.host = '127.0.0.1'
            url.port = self._proxy_port
            return url

        # in all other cases we need to be more clever, and create a dedicated
        # ssh tunnel for the callee
        if not url_port:
            try:
                url_port = socket.getservbyname(url.schema)
            except socket.error as e:
                raise ValueError('cannot handle "%s" urls' % url.schema)

        assert(url_host)
        assert(url_port)

        # we have a target host and port, we know the socks5 host and port, so
        # we can setup the ssh tunnel and use the localhost and the local port
        # as tunnel endpoint

        pid      = None
        port     = None
        port_min = None
        while True:

            # find a local port to use for the tunnel endpoint
            port = self._find_port(interface='127.0.0.1',
                                   port_min=port_min)

            cmd = self._tunnel_cmd % {'loc_port'   : port, 
                                      'url_host'   : url_host, 
                                      'url_port'   : url_port,
                                      'proxy_host' : self._proxy_host,
                                      'proxy_port' : self._proxy_port}
            try: 
                sp.check_call(cmd, shell=True)

                # find the pid
                for c in psutil.net_connections(kind='tcp'):
                    if c.laddr[0] == '127.0.0.1' and \
                       c.laddr[1] == port:
                            pid = c.pid
                            break

                # we got a tunnel...
                break

            except sp.CalledProcessError as e:

                if e.output and 'bind: Address already in use' in e.output:
                    # try again with a higher port number
                    port_min = port + 1
                    continue

                else:
                    raise

            # make sure we see that pid alive
            os.kill(pid, 0)

        # FIXME: we need a way to request killing of the new tunnel, or better
        #        some way to garbage collect it.  A lifetime parameter might be
        #        useful?

        ret = Url(url)
        ret.host = '127.0.0.1'  # we only tunnel via the local interface
        ret.port = port

        # remember that we own that tunnel
        self._tunnels.append({ 'url' : url, 
                               'ret' : ret, 
                               'pid' : pid, 
                               'port': port})
        return ret


    # --------------------------------------------------------------------------
    #
    def close(self):
        '''
        Close the underlying SOCKS5 proxy and all associated ssh tunnels.
        '''

        for t in self._tunnels:
            try:
                os.kill(t['pid'], signal.SIGTERM)
            except:
                # tunnel might be gone already
                pass

        if self._proxy_pid:
            try:
                os.kill(self._proxy_pid, signal.SIGTERM)
            except:
                # proxy might be gone already
                pass

        os.unlink(self._proxy_file)


# ------------------------------------------------------------------------------

