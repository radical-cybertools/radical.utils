
__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import collections
import math
import netifaces
import socket

from functools import reduce

from .misc import as_list, ru_open


# ------------------------------------------------------------------------------
#
_hostname = None


def get_hostname():
    """Look up the hostname."""

    global _hostname                                     # pylint: disable=W0603
    if not _hostname:

        _hostname = socket.getfqdn()

    return _hostname


# ------------------------------------------------------------------------------
#
_hostip = None


def get_hostip(req=None, log=None):
    """Look up the IP address for a given requested interface name.
    If interface is not given, do some magic."""

    global _hostip                                       # pylint: disable=W0603
    if _hostip:
        return _hostip

    AF_INET = netifaces.AF_INET

    # We create an ordered preference list, consisting of:
    #   - given arglist
    #   - white list (hardcoded preferred interfaces)
    #   - black_list (hardcoded unfavorable interfaces)
    #   - all others (whatever is not in the above)
    # Then this list is traversed, we check if the interface exists and has an
    # IP address.  The first match is used.

    req = as_list(req)

    white_list = [
        'ipogif0',  # Cray's
        'br0',      # SuperMIC
        'eth0',     # desktops etc.
        'wlan0'     # laptops etc.
    ]

    black_list = [
        'lo',       # takes the 'inter' out of the 'net'
        'sit0'      # ?
    ]

    ifaces = netifaces.interfaces()
    rest   = [iface for iface in ifaces
                     if iface not in req        and
                        iface not in white_list and
                        iface not in black_list]

    preflist = req + white_list + rest

    for iface in preflist:

        if iface not in ifaces:
            if log:
                log.debug('check iface %s: does not exist', iface)
            continue

        info = netifaces.ifaddresses(iface)
        if AF_INET not in info:
            if log:
                log.debug('check iface %s: no information', iface)
            continue

        if not len(info[AF_INET]):
            if log:
                log.debug('check iface %s: insufficient information', iface)
            continue

        if not info[AF_INET][0].get('addr'):
            if log:
                log.debug('check iface %s: disconnected', iface)
            continue

        ip = info[AF_INET][0].get('addr')
        if log:
            log.debug('check iface %s: ip is %s', iface, ip)

        if ip:
            _hostip = ip
            return ip

    return '127.0.0.1'


# ------------------------------------------------------------------------------
#
def create_hostfile(sandbox, name, hostlist, sep=' ', impaired=False):

    hostlist = as_list(hostlist)
    filename = '%s/%s.hosts' % (sandbox or '.', name)
    with ru_open(filename, 'w', encoding='utf8') as fout:

        if not impaired:
            # create dict: {'host1': x, 'host2': y}
            counter = collections.Counter(hostlist)
            # convert it into an ordered dict,
            count_dict = collections.OrderedDict(sorted(counter.items(),
                                                        key=lambda h: h[0]))

            hosts = ['%s%s%d' % (h, sep, c) for h, c in count_dict.items()]
            fout.write('\n'.join(hosts) + '\n')

        else:
            # write entry "hostN\nhostM\n"
            fout.write('\n'.join(hostlist) + '\n')

    # return the filename, caller is responsible for cleaning up
    return filename


# ------------------------------------------------------------------------------
#
def compress_hostlist(hostlist):

    # create dict: {'host1': x, 'host2': y}
    count_dict = dict(collections.Counter(hostlist))
    # find the gcd of the host counts (gcd of a list of numbers)
    host_gcd = reduce(math.gcd, set(count_dict.values()))

    # divide host counts by the gcd
    for host in count_dict:
        count_dict[host] /= host_gcd

    # recreate a list of hosts based on the normalized dict
    hosts = []
    for host, count in count_dict.items():
        hosts.extend([host] * int(count))

    # sort the list for readability
    hosts.sort()

    return hosts


# ------------------------------------------------------------------------------
#
def get_hostlist_by_range(hoststring, prefix='', width=0):
    """Convert string with host IDs into list of hosts.

    Example: Cobalt RM would have host template as 'nid%05d'
                get_hostlist_by_range('1-3,5', prefix='nid', width=5) =>
                ['nid00001', 'nid00002', 'nid00003', 'nid00005']
    """

    if not hoststring.replace('-', '').replace(',', '').isnumeric():
        raise ValueError('non numeric set of ranges (%s)' % hoststring)

    host_ids = []
    id_width = 0

    for num in hoststring.split(','):
        num_range = num.split('-')

        if len(num_range) > 1:
            num_lo, num_hi = num_range
            if not num_lo or not num_hi:
                raise ValueError('incorrect range format (%s)' % num)
            host_ids.extend(list(range(int(num_lo), int(num_hi) + 1)))

        else:
            host_ids.append(int(num_range[0]))

        id_width = max(id_width, *[len(n) for n in num_range])

    width = width or id_width
    return ['%s%0*d' % (prefix, width, hid) for hid in host_ids]


# ------------------------------------------------------------------------------
#
def get_hostlist(hoststring):
    """Convert string with hosts (IDs within brackets) into list of hosts.

    Example: 'node-b1-[1-3,5],node-c1-4,node-d3-3,node-k[10-12,15]' =>
             ['node-b1-1', 'node-b1-2', 'node-b1-3', 'node-b1-5',
              'node-c1-4', 'node-d3-3',
              'node-k10', 'node-k11', 'node-k12', 'node-k15']
    """

    output = []

    hoststring += ','
    host_group  = []

    idx, idx_stop = 0, len(hoststring)
    while idx != idx_stop:

        comma_idx   = hoststring.find(',', idx)
        bracket_idx = hoststring.find('[', idx)

        if comma_idx >= 0 and (bracket_idx == -1 or comma_idx < bracket_idx):

            if host_group:
                prefix = hoststring[idx:comma_idx]
                if prefix:
                    for h_idx in range(len(host_group)):
                        host_group[h_idx] += prefix
                output.extend(host_group)
                del host_group[:]

            else:
                output.append(hoststring[idx:comma_idx])

            idx = comma_idx + 1

        elif bracket_idx >= 0 and (comma_idx == -1 or bracket_idx < comma_idx):

            prefix = hoststring[idx:bracket_idx]
            if not host_group:
                host_group.append(prefix)
            else:
                for h_idx in range(len(host_group)):
                    host_group[h_idx] += prefix

            closed_bracket_idx = hoststring.find(']', bracket_idx)
            range_set = hoststring[(bracket_idx + 1):closed_bracket_idx]

            host_group_ = []
            for prefix in host_group:
                host_group_.extend(get_hostlist_by_range(range_set, prefix))
            host_group = host_group_

            idx = closed_bracket_idx + 1

    return output


# --------------------------------------------------------------------------
#
def is_localhost(host: str) -> bool:
    '''
    Returns `True` if given hostname is localhost, `False` otherwise.
    '''

    if not host:
        return True

    elif host == 'localhost':
        return True

    else:
        sockhost = socket.gethostname()
        while sockhost:
            if host == sockhost:
                return True
            sockhost = '.'.join(sockhost.split('.')[1:])

    return False


# ------------------------------------------------------------------------------

