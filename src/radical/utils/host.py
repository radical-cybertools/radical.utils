
# pylint: disable=E1136

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import collections
import netifaces
import socket

from .misc   import as_list, ru_open
from .config import DefaultConfig


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
def get_hostip(req=None, log=None):
    """Look up the IP address for a given requested interface name.
    If interface is not given, do some magic."""

    AF_INET = netifaces.AF_INET

    # We create an ordered preference list, consisting of:
    #   - given arglist
    #   - white list (hardcoded preferred interfaces)
    #   - black_list (hardcoded unfavorable interfaces)
    #   - all others (whatever is not in the above)
    # Then this list is traversed, we check if the interface exists and has an
    # IP address.  The first match is used.

    if not req:
        req = DefaultConfig().get('iface')

    req = as_list(req)

    white_list = [
        'ens1f1',   # amarel
        'ib0',      # infiniband   # NOTE: unusable on Amarel, use `req` there!
        'hsn0',     # Frontier (HPE Cray EX)
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
def compress_hostlist(nodes):
    '''
    Assume that a batch allocation has these nodes:

       nodes = ['node01', 'node02', 'node03', 'node04',
                'node15', 'node17', 'node18']

    then the nodelist is compressed to:

       nodelist = 'node[01-04,15,17-18]'

    NOTE: not support yet for further packing like:

       nodelist = 'node0[1-4],node1[5,7-8]'
    '''

    if not nodes:
        return ''

    nodes  = sorted(nodes)
    prefix = ''
    for char in nodes[0]:
        if char.isdigit():
            break
        prefix += char

    plen = len(prefix)

    for node in nodes:

        if not node.startswith(prefix):
            raise ValueError('nodes do not have the same prefix: %s' % nodes)

        if not node[len(prefix):].isdigit():
            raise ValueError('nodes do not have numeric suffix: %s' % nodes)

    if len(nodes) == 1:
        return nodes[0]

    ranges = list()
    start  = None
    end    = None

    for node in nodes:

        node_idx  = int(node[plen:])
        start_idx = int(start[plen:]) if start else None
        end_idx   = int(end[plen:])   if end   else None

        if not start:
            start = node
            continue

        if not end:
            # if we have no end yet, check if we have a consecutive node and
            # mark the new end
            if node_idx == start_idx + 1:
                end = node
                continue

            else:
                # store the previous node and start a new range
                ranges.append('%s' % start[plen:])
                start = node

        else:
            # if we have an end, check if we have a consecutive node and move
            # the end
            if node_idx == end_idx + 1:
                end = node
                continue

            else:
                # otherwise, store the previous range and start a new one
                ranges.append('%s-%s' % (start[plen:], end[plen:]))
                start = node
                end   = None

    # if we have a start but no end, we need to store the last node
    # if we have an end, we need to store the last range
    if start:
        if end:
            ranges.append('%s-%s' % (start[plen:], end[plen:]))
        else:
            ranges.append('%s' % start[plen:])

    new = '%s[%s]' % (prefix, ','.join(ranges))

    return new


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

