
import os
import zmq
import errno

from ..url  import Url
from ..host import get_hostip
from ..misc import as_list, as_string, find_port, ru_open


# NOTE: this is ignoring `RADICAL_LOG_LVL` on purpose
LOG_ENABLED = os.environ.get('RADICAL_ZMQ_LOG', '0').lower() in ['1', 'true']


# --------------------------------------------------------------------------
#
# zmq will (rightly) barf at interrupted system calls.  We are able to rerun
# those calls.
#
# This is presumably rare, and repeated interrupts increasingly unlikely.
# More than, say, 3 point to races or I/O thrashing
#
# FIXME: how does that behave wrt. timeouts?  We probably should include
#        an explicit timeout parameter.
#
# kudos: https://gist.github.com/minrk/5258909
#
def no_intr(f, *args, **kwargs):

    _max = 3
    cnt  = 0
    while True:
        try:
            return f(*args, **kwargs)

        except zmq.ContextTerminated:
            return None    # connect closed or otherwise became unusable

        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                if cnt > _max:
                    raise  # interrupted too often - forward exception
                cnt += 1
                continue   # interrupted, try again
            raise          # some other error condition, raise it


# ------------------------------------------------------------------------------
#
def get_uids(msgs):

    msgs = as_list(msgs)
    try   : return [str(m.get('uid')) for m in msgs]
    except: return []


# ------------------------------------------------------------------------------
#
def prof_bulk(prof, event, msgs, msg=None):

    for uid in get_uids(msgs):
        if uid:
            prof.prof(event, uid=uid)
        else:
            prof.prof(event, msg=msgs[:32])


# ------------------------------------------------------------------------------
#
def get_channel_url(ep_type, channel=None, url=None):
    '''
    For the given endpoint type, ensure that both channel name and endpoint URL
    are known.  If they are not, raise a ValueError exception.

    For a given URL, the channel is derived as path element of that URL
    (leading `/` is stripped).

    For a given channel channel name, the URL is searched in the process
    environment (under uppercase version of `<CHANNEL>_<EPTYPE>_URL`).  If not
    found, the method will look if a config file with the name `<channel>.cfg`
    exists, and if it has a top level entry named `<ep_type>` (lower case).

    Before returning the given or derived channel and url, the method will check
    if both data match (i.e. if the channel name is reflected in the URL)
    '''

    if not channel and not url:
        raise ValueError('need either channel name or URL')

    if not channel:
        # get channel from path element of URL
        # example:
        #   channel `foo`
        #   url     `pubsub://localhost:1234/foo`
        channel = os.path.basename(str(Url(url.path)))

    elif not url:
        # get url from environment (`FOO_PUB_URL`) or config file (`foo.cfg`)

        env_name = '%s_%s_URL' % (channel.upper(), ep_type.upper())
        cfg_name = './%s.cfg'  %  channel.lower()

        if env_name in os.environ:
            url = os.environ[env_name]

        elif os.path.exists(cfg_name):
            with ru_open(cfg_name, 'r') as fin:
                for line in fin.readlines():
                    _ep_type, _url = line.split(':')
                    if _ep_type.strip().upper() == ep_type.upper():
                        url = _url
                        break

    # sanity checks
    if not url:
        raise ValueError('no URL for %s channel %s' % (channel, ep_type))

    if not channel:
        raise ValueError('no %s channel for URL %s' % (ep_type, url))

    if channel.lower() != Url(url).path.lstrip('/').lower():
        raise ValueError('%s channel (%s) / url (%s) mismatch'
                        % (ep_type, channel, url))

    return channel, url


# ------------------------------------------------------------------------------
#
def log_bulk(log, token, msgs):

    if log.num_level > 1:
        # log level `debug_9` disabled
        return

    if not msgs:
        return

    if isinstance(msgs[0], dict) and 'uid' in msgs[0]:
        for msg in msgs:
            log.debug_9("%s: %s [%s]", token, msg['uid'], msg.get('state'))

    else:
        for msg in msgs:
            log.debug_9("%s: %s", token, str(msg)[0:32])


# ------------------------------------------------------------------------------
#
def sock_connect(sock, url, hop=None):

    if not hop:
        hop = os.environ.get('RADICAL_SSH_HOP')

    if hop:
        from zmq import ssh
        ssh.tunnel_connection(sock, url, hop)

    else:
        sock.connect(url)


# ------------------------------------------------------------------------------
#
def zmq_bind(sock):

    while True:
        port = find_port()
        try:
            sock.bind('tcp://*:%s' % port)
            addr      = Url(as_string(sock.getsockopt(zmq.LAST_ENDPOINT)))
            addr.host = get_hostip()
            return addr
        except:
            pass

    raise RuntimeError('could not bind to any port')


# ------------------------------------------------------------------------------

