
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
# Usage example:
#
#   * setup:
#     - client : laptop
#     - proxy 1: two.radical-cynertools.org (ssh, public key)
#     - proxy 2: one.radical-cynertools.org (ssh, user/pass)
#     - tunnel : titan-ext1.ccs.ornl.gov    (ssh, keyfob)
#     - rush   : job submission (qsub)
#
#   * sequence
#     - create above chain (with user/pass and keyfob prompts)
#     - start a process (async, I/O redirection to buffered pipe)
#     - kill delete proxy_2 connection out-of-band
#     - kill client
#     - start new client instance
#     - reconnect chain (with user/pass prompt for proxy_2 hop)
#     - reconnect rush
#     - check state of job (reconnect to async process, retrieve missed I/O)
#
# ------------------------------------------------------------------------------
#
# Not covered:
#
#   - any protocol tunneled must itself be reconnectable - no provision is made
#     to support that
#   - no credentials are kept for reconnect
#
# ------------------------------------------------------------------------------
#
# API 'spec'
#
#     * create a proxy
#       p1 = ru.Proxy(proxy_url=None)
#       p1.is_alive()
#       p1.kill()
#       p1.restart()
#       p1.is_alive()
#       p1.url
#
#     * chain proxies
#       p2 = p1.chain(proxy_url)
#       p1 = p2.proxy
#
#     * create a tunnel over a (direct or chained) proxy
#       t1 = p2.tunnel(service_url, socks5=False)
#       t1.is_alive()
#       t1.kill()
#       t1.restart()
#       t1.is_alive()
#       t1.socks5
#       t1.url
#       p2 = t1.proxy
#
#     * create a command endpoint over a tunnel creates thus
#       (zmq protocol, persistent service EP, async)
#       rush = ru.Sh(p.tunnel('rush://targethost/'))
#       print rush.ps()
#
# ------------------------------------------------------------------------------
#
# Definitions:
#
# - Proxy:  a authenticated and authorized tcp connection to a remote host
#           (proxy host).  That connection can be used to tunnel one or more
#           protocol channels.
#           Proxies can be chained, in that connections to some remote hosts
#           may consist of multiple hops over intermediary hosts.  A chained
#           proxy transparently acts as a connection to the last target host,
#           and all communication is transparently forwarded over the proxy
#           chain.
#
# - Tunnel: a channel with a specific protocol which is tunneled over a proxy
#           connection.  Establishing a tunnel will open an ephermeal port on
#           the *local* machine which is transparently forwarded over a proxy
#           or proxy chain to a specified port on the target host.
#           A Tunnel instance can only be created from a Proxy, buy calling
#           Proxy.tunnel().  Multiple tunnels can be created over the same
#           proxy connection.
#
# ------------------------------------------------------------------------------


class Proxy(object):
    '''
    We frequently face the problem that a network connection cannot be directly
    established due to firewall policies.  At the same time, establishing ssh
    tunnels manually can be cumbersome, as that needs activity by every user,
    coordination of used port numbers, out-of-band communication of tunnel
    settings, etc.

    This class eleviates that problem for the scope of the RADICAL stack.  It
    expects two user level settings:

        export RADICAL_PROXY_URL=ssh://host/

    to specify a suitable tunnel host.  When being used by some layer in the
    radical stack, this class will:

        - search a free local port;
        - establish a SOCKS5 tunnel to the given host, binding it to the given
          port;
        - additionally establish, on demand, required (non-SOCKS) application
          tunnels over that SOCKS tunnel.

    The existence of the SOCKS tunnel is recorded in the file

        `$HOME/.radical/utils/proxy.<host>[.<name>]`

    which contains information about the used local port.  That file will
    disappear when the proxy disappears.  The `[.<name>]` part is used for
    direct application tunnels established over the original socks proxy tunnel.
    `name` is expected to be a unique identifyer.  The proxy is expected to live
    until it is explicitly closed.

    If not proxy is given, the methods in this class are NOOPs - it can thus
    transparently be used for proxied and direct connections.


    Requirements:
    -------------

      * passwordless ssh login needs to be configured from localhost to the
        proxy host (but see TODOs below)
      * the proxy host must have these settings in `/etc/ssh/sshd_config`:

            GatewayPorts       yes
            AllowTcpForwarding yes

    TODO:
    -----

      * gsissh support
      * support different auth mechanisms (user/key, public key, encrypted key,
        ssh agent, myproxy, etc - see SAGA security contexts)


    Usage:
    ------

        # connect to `http://www.google.com/` (port 80), SOCKS enabled client
        proxy = ru.Proxy(timeout=300)
        client.connect(proxy.url(socks=True, 'http://www.google.com/'))
        print proxy.url(socks=True, 'http://www.google.com/')
          --> http://localhost:10000/

        # connect to `mongodb://www.mlab.com:12017/rp`, client is *not* able to
        # use SOCKS
        proxy = ru.Proxy()
        print proxy.url(socks=False, `mongodb://www.mlab.com:12017/rp`)
          --> mongodb://localhost:10001/rp

        proxy.close() / gc


    We  will also provide a command line tool which supports similar operations
    in the shell:

        # connect to `http://www.google.com/` (port 80), SOCKS enabled client
        wget `radical-proxy --socks=True 'http://www.google.com/'`

        # connect to `mongodb://foobar.mlab.com:12017/rp`. client is *not* able
        # to use SOCKS
        mongo `radical-proxy --socks=False 'foobar.mlab.com:12017' rp`
    '''


    # we use netcat and its derivates to establish the proxy connection.  The
    # dict below abstracts the syntax differences for the different flavors.
    #
    #     nc     -X 5 -x 127.0.0.1:10000 %h %p
    #     netcat -X 5 -x 127.0.0.1:10000 %h %p
    #     ncat   --proxy-type socks5 --proxy 127.0.0.1:10000 %h %p
    #     socat  - socks:127.0.0.1:%h:%p,socksport=10000
    #
    _tunnel_proxies = {
        'ncat'   : 'ncat    --proxy-type socks5 '
                           '--proxy 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'nc'     : 'nc      -X 5 -x 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'netcat' : 'netcat  -X 5 -x 127.0.0.1:%(proxy_port)d %%%%h %%%%p',
        'socat'  : 'socat   - socks:127.0.0.1:%%%%h:%%%%p,'
                             'socksport=%(proxy_port)d'
    }

    # information about established proxy endpoints are stored on disk, so that
    # proxies can be picked up and reconnected to in case of failures
    #
    # TODO: lock proxies so that only one client instance can use them
    #       at any time, or make sure that multi-tenant use works
    #
    _proxy_base = '%s/.radical/utils/proxies/' % os.environ['HOME']
    if not os.path.isdir(_proxy_base):
        os.makedirs(_proxy_base)

    # `_ptty_cmd` is the shell command to be run on the proxy host upon
    # establishing the proxy connection
  # _ptty_cmd = '''
  # echo 1
  # echo 2
  # '''

    _ptty_cmd = ''

    # --------------------------------------------------------------------------
    #
    def __init__(self, url=None, timeout=None):
        '''
        Create a new Proxy instance.  This might establsh to a new proxy
        connection, or attach to an existing one.  We find existing proxies by
        inspecting the file `$HOME/.radical/utils/proxy.<proxy-host>` which
        contains pid and port of the proxy process.
        '''

        # keep track of tunnels created on this proxy instance
        # TODO: store tunnel info on disk, and recreate them on reconnect?
        self._tunnels        = list()
        self._tunnel_command = None

        # TODO: perform some kind of time limited or activity dependent garbage
        #       collection.  Possibly add a lifetime parameter?

        if url: proxy_url = Url(url)
        else  : proxy_url = Url(os.environ.get('RADICAL_PROXY_URL'))

        if not proxy_url:
            raise ValueError("missing proxy URL ('RADICAL_PROXY_URL']")

        url_host = proxy_url.host
        url_port = proxy_url.port
        if not url_port:
            try:
                url_port = socket.getservbyname(proxy_url.schema)
            except socket.error as e:
                raise ValueError('cannot handle "%s" urls' %
                                  proxy_url.schema)

        self._proxy_host = None
        self._proxy_port = None
        self._proxy_pid  = None

        self._proxy_id   = 'proxy.%s.%s' % (url_host, url_port)

        proxy_in         = '%s/%s.in'  % (self._proxy_base, self._proxy_id)
        proxy_out        = '%s/%s.out' % (self._proxy_base, self._proxy_id)
        proxy_err        = '%s/%s.err' % (self._proxy_base, self._proxy_id)

        self._proxy_stat = '%s/%s.stat' % (self._proxy_base, self._proxy_id)

        fd = None 
        try:

            # open the file or create it, then lock it, then read from begin of
            # file.  We expect a string and two integers(host, port, and pid of
            # an existing proxy).  If those are found, we close the file (which
            # unlocks it).  If they are not found, we create the proxy, write
            # the information to the file, and close it (which also unlocks it).
            #
            # NOTE: that there is a race condition between checking for an
            #       existing proxy and using it: the proxy might disappear 
            #       meanwhile. Since we have no control over that time, we make
            #       no guarantees wrt. proxy health whatsoever.  It is the
            #       application's responsibility to ensure sufficient proxy
            #       lifetime, and to fail or recover on dead proxies.
            #
            fd = os.open(self._proxy_stat, os.O_RDWR | os.O_CREAT)
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.lseek(fd, 0, os.SEEK_SET)

            try:
                # assume proxy exists
                data  = os.read(fd, 512)  # POSIX HOST_NAME_MAX: 256
                elems = data.split()
                self._proxy_host = str(elems[0].strip())
                self._proxy_port = int(elems[1].strip())
                self._proxy_pid  = int(elems[2].strip())

                # if pid can be signalled, then the process is alive
                # NOTE: this races with PID reuse.
                #       Collisions should be rare (TM).
                os.kill(self._proxy_pid, 0)

            except Exception as e:
                # either data were not present or invalid, or pid cannot be
                # signalled -- either way, we can't use the proxy.
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

                    # FIXME: support gsissh
                    # FIXME: support interactive passwd / passkey
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
                            # what just happened??
                            raise

                assert(self._proxy_host)
                assert(self._proxy_port)
                assert(self._proxy_pid)

                # store new proxy parameters (first rewind from previous read)
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, "%s %d %d\n" % (self._proxy_host,
                                             self._proxy_port,
                                             self._proxy_pid))

        finally:

            # release the lock
            if fd:
                os.close(fd)


        # Now that we have a proxy, we can configure the tunnel command for
        # later use on `url(socks=False)`
        #
        # Check for the availablity of various utilities which help to tunnel
        # ssh over a SOCKS proxy.  See documentation and code comments in
        # `self.url()` for details.
        #
        # FIXME: parts of this should be done only once, on module load
        tunnel_proxy = None
        for name in self._tunnel_proxies:
            exe = which(name)
            if exe:
                tunnel_proxy = self._tunnel_proxies[name] \
                             % {'proxy_port': self._proxy_port}
                break

        # FIXME: support gsissh
        # FIXME: support interactive passwd / passkey
        self._tunnel_cmd = 'ssh -o ExitOnForwardFailure=yes' \
                         +    ' -o StrictHostKeyChecking=no' \
                         +    ' -NfL %(loc_port)d:%(url_host)s:%(url_port)d' \
                         +    ' %(proxy_host)s'

        # if we have tunnel proxy support, it becomes part of the tunnel command
        if tunnel_proxy: 
            self._tunnel_cmd += " -o ProxyCommand='%s'" % tunnel_proxy

        # finally, we add the command for the ptty shell at the other tunnel end
        # FIXME: remove '-f' from tunnel_cmd for this to make any sense
        # FIXME: not every tunnel should be a RUSH tunnel
      # self._tunnel_cmd += ' %(_ptty_cmd)s'


    # --------------------------------------------------------------------------
    #
    def _find_port(self, interface=None, port_min=None, port_max=None):
        '''
        Inspect the OS for tcp connection usage, and pick an unused port in the
        private, ephemeral port range between 49152 and 65535.  By default we
        check all interfaces, but an interface can also be specified as optional
        argument, identified by its IP number.

        NOTE: this mechanism does not *reserve* a port, so there is a race
        between finding a free port and using it.
        '''

        if not port_min: port_min = 49152
        if not port_max: port_max = 65535

        used_adr     = [c.laddr for c in psutil.net_connections(kind='tcp')]
        if interface:
            used_adr = [x       for x in used_adr if x[0] == interface]
        used_ports   = [x[1]    for x in used_adr]

        for port in range(port_min, port_max):
            if port not in used_ports:
                return port

        return None


    # --------------------------------------------------------------------------
    #
    def _find_port_alt(self, interface=''):
        '''
        Let the OS choose an open port for us and return it.  By default, we
        listen on all interfaces, but an interface can also be specified as
        optional argument, as IP number.

        Note that this mechanism does not *reserve* a port, so there is a race
        between finding a free port and using it.

        We don't bother catching any system errors: if we can't find a port this
        way, its unlikely that it can be found any other way.
        '''

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((interface, 0))
        port = sock.getsockname()[1]
        sock.close()
        return port


    # --------------------------------------------------------------------------
    #
    def url(self, url, socks=True):
        '''
        This method accepts an URL to which the callee wants to connect, using
        the proxy we own / interface to.  The call with translate the given URL
        into a suitable URL which points to same endpoint over the proxy
        connection.

        If the callee intents to use the URL with a SOCKS5 enables application,
        we only replace host and port to point to our socks proxy, and return
        the resulting URL.

        If the application is *not* SOCKS5 enabled, then such a socks URL would
        be unusable.  Instead, we create a separate dedicated ssh tunnel *over
        the socks tunnel*, and return that specific URL.  That will now bind to
        a specific port.  One condition is to be able to derive the target port
        from the given URL (in case it is not explicitly specified in the URL).
        For that we perform a service port lookup via `socket.getservbyname()`.

        All tunnels will be closed on `proxy.close()`.
        '''

        # FIXME: Note that in the `socks=False` case, we create a new tunnel for
        #        each request, even if a tunnel for the same target endpoint was
        #        already created before, as we do not know if multiple
        #        applications are able to share the same tunnel.  They usually
        #        are though, and future versions of this method may add
        #        a `reuse` flag (defaulting to `True`).


        # Here are the different options to create an ssh tunnel over the socks
        # proxy.  Ironically, ssh can natively create, but not use a socks
        # proxy.  We thus need to specify an external proxy command.  We can
        # currently handle: `nc`, `ncat`, `netcat` and `socat`, which are all
        # different variations of the original BSD `netcat` (I believe):
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
        # somewhat hacky (even more so than the others).
        #
        # The examples above assume `self._proxy_port` to be set to `10000`,
        # and `10001` to be the next open port found.

        # make sure we have an URL
        url      = Url(url)
        url_port = url.port
        url_host = url.host

        # if we have no proxy, we return the url as-is
        # FIXME: should we raise an error instead?
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
        # ssh tunnel for the callee.  First though we make sure we have a usable
        # port number
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
                                      'proxy_port' : self._proxy_port,
                                      'ptty_cmd'   : self._ptty_cmd
                                      }
            try:

              # # set up stdin, stdout, stderr named pipes (FIFOs), and create
              # # a lock. 
              # #
              # # We need to put the FIFOs in asyn I/O mode, as otherwise we may
              # # hang on a dead tunnel.
              # # 
              # # We need the lock as only one thing can talk to that process at
              # # any point in time, and right now this is us.  
              # # 
              # # Why do we use named pipes in the first place?  So that the
              # # tunnel survives this process.  But since there is
              # # a back-and-forth on the fifos, it might well happen that the
              # # this process dies and leaves the protocol (if one can call
              # # that) in an unclean state.  Any reconnecting process is well
              # # advised to rest the channel content to a well known state.
              # os.mkfifo (self._proxy_in)
              # os.mkfifo (self._proxy_out)
              # os.mkfifo (self._proxy_err)
              #
              # except OSError, e:
              #     print "Failed to create FIFO: %s" % e
              # else:
              #     fifo = open(filename, 'w')
              #     # write stuff to fifo
              #     print >> fifo, "hello"
              #     fifo.close()
              #     os.remove(filename)
              #     os.rmdir(tmpdir)

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
        self._tunnels.append({'url' : url, 
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

        os.unlink(self._proxy_stat)


# ------------------------------------------------------------------------------

