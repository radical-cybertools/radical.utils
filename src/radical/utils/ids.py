
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import time
import uuid
import fcntl
import socket
import datetime
import threading

from .singleton import Singleton
from .misc      import dockerized, get_radical_base

TEMPLATE_SIMPLE  = "%(prefix)s.%(counter)04d"
TEMPLATE_UNIQUE  = "%(prefix)s.%(date)s.%(time)s.%(pid)06d.%(counter)04d"
TEMPLATE_PRIVATE = "%(prefix)s.%(host)s.%(user)s.%(days)06d.%(day_counter)04d"
TEMPLATE_UUID    = "%(prefix)s.%(uuid)s"


_cache = {'dir'        : list(),
          'user'       : None,
          'pid'        : os.getpid(),
          'dockerized' : dockerized(),
          }


# ------------------------------------------------------------------------------
#
class _IDRegistry(object, metaclass=Singleton):
    """
    This helper class (which is not exposed to any user of radical.utils)
    generates a sequence of continous numbers for each known ID prefix.  It is
    a singleton, and thread safe (assuming that the Singleton metaclass supports
    thread safe construction).
    """


    # --------------------------------------------------------------------------
    def __init__(self):
        """
        Initialized the registry dict and the threading lock
        """

        self._rlock    = threading.RLock()
        self._registry = dict()


    # --------------------------------------------------------------------------
    def get_counter(self, prefix):
        """
        Obtain the next number in the sequence for the given prefix.
        If the prefix is not known, a new registry counter is created.
        """

        with self._rlock:

            if prefix not in self._registry:
                self._registry[prefix] = 0

            ret = self._registry[prefix]

            self._registry[prefix] += 1

            return ret


    # --------------------------------------------------------------------------
    def reset_counter(self, prefix, reset_all_others=False):
        """
        Reset the given counter to zero.
        """

        with self._rlock:

            if reset_all_others:
                # reset all counters *but* the one given
                for p in self._registry:
                    if p != prefix:
                        self._registry[p] = 0
            else:
                self._registry[prefix] = 0


# ------------------------------------------------------------------------------
#
# we create on private singleton instance for the ID registry.
#
_id_registry = _IDRegistry()
_BASE        = get_radical_base('utils')


# ------------------------------------------------------------------------------
#
ID_SIMPLE  = 'simple'
ID_UNIQUE  = 'unique'
ID_PRIVATE = 'private'
ID_CUSTOM  = 'custom'
ID_UUID    = 'uiud'


# ------------------------------------------------------------------------------
#
def generate_id(prefix, mode=ID_SIMPLE, ns=None):
    """
    Generate a human readable, sequential ID for the given prefix.

    The ID is by default very simple and thus very readable, but cannot be
    assumed to be globally unique -- simple ID uniqueness is only guaranteed
    within the scope of one python instance.

    If `mode` is set to the non-default type `ID_UNIQUE`, an attempt is made to
    generate readable but globally unique IDs -- although the level of
    confidence for uniqueness is significantly smaller than for, say UUIDs.

    The ID format per mode is:
    ID_SIMPLE  = "%(prefix)s.%(counter)04d"
    ID_UNIQUE  = "%(prefix)s.%(date)s.%(time)s.%(pid)06d.%(counter)04d"
    ID_PRIVATE = "%(prefix)s.%(host)s.%(user)s.%(days)06d.%(day_counter)04d"
    ID_UUID    = "%(prefix)s.%(uuid)s"

    Examples::

        print(radical.utils.generate_id('item.'))
        print(radical.utils.generate_id('item.'))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_SIMPLE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_SIMPLE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_UNIQUE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_UNIQUE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_PRIVATE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_PRIVATE))
        print(radical.utils.generate_id('item.', mode=radical.utils.ID_UUID))

    The above will generate the IDs:

        item.0001
        item.0002
        item.0003
        item.0004
        item.2014.07.30.13.13.44.0001
        item.2014.07.30.13.13.44.0002
        item.cameo.merzky.021342.0001
        item.cameo.merzky.021342.0002
        item.23cacb7e-0b08-11e5-9f0f-08002716eaa9

    where 'cameo' is the (short) hostname, 'merzky' is the username, and '02134'
    is 'days since epoch'.  The last element, the counter is unique for each id
    type and item type, and restarts for each session (application process).  In
    the last case though (`ID_PRIVATE`), the counter is reset for every new day,
    and can thus span multiple applications.

    'ns' argument can be specified to a value such that unique IDs are created
    local to that namespace. For example, you can create a session and use the
    session ID as a namespace for all the IDs of the objects of that execution.

    Example::

        sid  = generate_id('re.session', ID_PRIVATE)
        uid1 = generate_id('task.%(item_counter)04d', ID_CUSTOM, ns=sid)
        uid2 = generate_id('task.%(item_counter)04d', ID_CUSTOM, ns=sid)
        ...


    This will generate the following ids::

        re.session.rivendell.vivek.017548.0001
        task.0000
        task.0001

    where the `task.*` IDs are unique for the used sid namespace.

    The namespaces are stored under ```$RADICAL_BASE_DIR/.radical/utils/```.
    If `RADICAL_BASE_DIR` is not set, then `$HOME` is used.

    Note that for docker containers, we try to avoid hostname / username clashes
    and will, for `ID_PRIVATE`, revert to `ID_UUID`.
    """

    if not prefix or \
        not isinstance(prefix, str):
        raise TypeError("ID generation expect prefix in basestring type")

    template = ""

    if _cache['dockerized'] and mode == ID_PRIVATE:
        mode = ID_UUID

    if   mode == ID_CUSTOM : template = prefix
    elif mode == ID_UUID   : template = TEMPLATE_UUID
    elif mode == ID_SIMPLE : template = TEMPLATE_SIMPLE
    elif mode == ID_UNIQUE : template = TEMPLATE_UNIQUE
    elif mode == ID_PRIVATE: template = TEMPLATE_PRIVATE
    else: raise ValueError("unsupported mode '%s'", mode)

    return _generate_id(template, prefix, ns)


# ------------------------------------------------------------------------------
#
def _generate_id(template, prefix, ns=None):

    # FIXME: several of the vars below are constants, and many of them are
    # rarely used in IDs.  They should be created only once per module instance,
    # and/or only if needed.

    global _cache

    state_dir = _BASE
    if ns:
        state_dir += '/%s' % ns

    if state_dir not in _cache['dir']:
        try   : os.makedirs(state_dir)
        except: pass
        _cache['dir'].append(state_dir)

    # seconds since epoch(float), and timestamp
    seconds = time.time()
    now     = datetime.datetime.fromtimestamp(seconds)
    days    = int(seconds / (60 * 60 * 24))

    if not _cache['user']:
        try:
            import getpass
            _cache['user'] = getpass.getuser()
        except:
            _cache['user'] = 'nobody'

    user = _cache['user']

    info = dict()

    info['day_counter' ] = 0
    info['item_counter'] = 0
    info['counter'     ] = 0
    info['prefix'      ] = prefix
    info['seconds'     ] = int(seconds)     # full seconds since epoch
    info['days'        ] = days             # full days since epoch
    info['user'        ] = user             # local username
    info['now'         ] = now
    info['date'        ] = "%04d.%02d.%02d" % (now.year, now.month,  now.day)
    info['time'        ] = "%02d.%02d.%02d" % (now.hour, now.minute, now.second)
    info['pid'         ] = _cache['pid']

    # the following ones are time consuming, and only done when needed
    if '%(host)' in template: info['host'] = socket.gethostname()  # localhost
    if '%(uuid)' in template: info['uuid'] = uuid.uuid1()          # plain uuid

    if '%(day_counter)' in template:
        fd = os.open("%s/ru_%s_%s.cnt" % (state_dir, user, days),
                                          os.O_RDWR | os.O_CREAT)
        fcntl.flock(fd, fcntl.LOCK_EX)
        os.lseek(fd, 0, os.SEEK_SET )
        data = os.read(fd, 256)
        if not data: data = 0
        info['day_counter'] = int(data)
        os.lseek(fd, 0, os.SEEK_SET )
        line = "%d\n" % (info['day_counter'] + 1)
        line = str.encode(line)
        os.write(fd, line)
        os.close(fd)

    if '%(item_counter)' in template:
        fd = os.open("%s/ru_%s_%s.cnt" % (state_dir, user, prefix),
                                          os.O_RDWR | os.O_CREAT)
        fcntl.flock(fd, fcntl.LOCK_EX)
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, 256)
        if not data: data = 0
        info['item_counter'] = int(data)
        os.lseek(fd, 0, os.SEEK_SET)
        line = "%d\n" % (info['item_counter'] + 1)
        line = str.encode(line)
        os.write(fd, line)
        os.close(fd)

    if '%(counter)' in template:
        info['counter'] = _id_registry.get_counter(prefix.replace('%', ''))

    ret = template % info

    if '%(' in ret:
        # import pprint
        # pprint.pprint(info)
        # print(template)
        # print(ret)
        raise ValueError('unknown pattern in template (%s)' % template)

    return ret


# ------------------------------------------------------------------------------
#
def reset_id_counters(prefix=None, reset_all_others=False):

    if not isinstance(prefix, list):
        prefix = [prefix]

    for p in prefix:
        _id_registry.reset_counter(p.replace('%', ''), reset_all_others)


# ------------------------------------------------------------------------------

