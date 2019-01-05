
import os
import sys
import time
import fcntl
import regex
import shlex
import socket
import select
import pkgutil
import netifaces
import queue

import subprocess as sp
import threading  as mt


from . import url       as ruu
from . import constants as ruc


# ------------------------------------------------------------------------------
#
def split_dburl(dburl, default_dburl=None):
    '''
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    '''

    # if the given URL does not contain schema nor host, the default URL is used
    # as base, and the given URL string is appended to the path element.

    url = ruu.Url(dburl)

    if not url.schema and not url.host:
        url      = ruu.Url(default_dburl)
        url.path = dburl

    # NOTE: add other data base schemes here...
    if 'mongodb' not in url.schema.split('+'):
        raise ValueError("expected 'mongodb[+ssl]://' url, not '%s'" % dburl)

    host = url.host
    port = url.port
    path = url.path
    user = url.username
    pwd  = url.password
    ssl  = False

    if 'ssl' in url.schema.split('+'):
        ssl = True
        url.schema = 'mongodb'

    if not host:
        host = 'localhost'

    if  path.startswith('/'):
        path = path[1:]
    path_elems = path.split('/')

    dbname = None
    cname  = None
    pname  = None

    if  len(path_elems)  >  0:
        dbname = path_elems[0]

    if  len(path_elems)  >  1:
        dbname = path_elems[0]
        cname  = path_elems[1]

    if  len(path_elems)  >  2:
        dbname = path_elems[0]
        cname  = path_elems[1]
        pname  = '.'.join(path_elems[2:])

    if  dbname == '.':
        dbname = None

    return [host, port, dbname, cname, pname, user, pwd, ssl]


# ------------------------------------------------------------------------------
#
def mongodb_connect(dburl, default_dburl=None):
    '''
    connect to the given mongodb, perform auth for the database (if a database
    was given).
    '''

    try:
        import pymongo
    except ImportError:
        msg  = " \n\npymongo is not available -- install radical.utils with: \n\n"
        msg += "  (1) pip install --upgrade -e '.[pymongo]'\n"
        msg += "  (2) pip install --upgrade    'radical.utils[pymongo]'\n\n"
        msg += "to resolve that dependency (or install pymongo manually).\n"
        msg += "The first version will work for local installation, \n"
        msg += "the second one for installation from pypi.\n\n"
        raise ImportError(msg)

    [host, port, dbname, cname, pname,
           user, pwd,    ssl] = split_dburl(dburl, default_dburl)

    mongo = pymongo.MongoClient(host=host, port=port, ssl=ssl)
    db    = None

    if  dbname:
        db = mongo[dbname]

        if  user and pwd:
            db.authenticate(user, pwd)

    else:

        # if no DB is given, we try to auth against all databases.
        for dbname in mongo.database_names():
            try:
                mongo[dbname].authenticate(user, pwd)
            except Exception:
                pass


    return mongo, db, dbname, cname, pname


# ------------------------------------------------------------------------------
#
def parse_file_staging_directives(directives):
    '''
    staging directives

       [local_path] [operator] [remote_path]

    local path:
        * interpreted as relative to the application's working directory
        * must point to local storage (localhost)

    remote path
        * interpreted as relative to the job's working directory

    operator :
        * >  : stage to remote target, overwrite if exists
        * >> : stage to remote target, append    if exists
        * <  : stage to local  target, overwrite if exists
        * << : stage to local  target, append    if exists

    This method returns a tuple [src, tgt, op] for each given directive.  This
    parsing is backward compatible with the simple staging directives used
    previously -- any strings which do not contain staging operators will be
    interpreted as simple paths (identical for src and tgt), operation is set to
    '=', which must be interpreted in the caller context.
    '''

    bulk = True
    if  not isinstance(directives, list):
        bulk       = False
        directives = [directives]

    ret = list()

    for directive in directives:

        if  not isinstance(directive, basestring):
            raise TypeError("file staging directives muct by of type string, "
                            "not %s" % type(directive))

        rs = regex.ReString(directive)

        if  rs // '^(?P<one>.+?)\s*(?P<op><|<<|>|>>)\s*(?P<two>.+)$':
            res = rs.get()
            ret.append([res['one'], res['two'], res['op']])

        else:
            ret.append([directive, directive, '='])

    if  bulk: return ret
    else    : return ret[0]


# ------------------------------------------------------------------------------
#
def time_stamp(spec):

    if  isinstance(spec, int) or \
        isinstance(spec, float)  :

        import datetime
        return datetime.datetime.utcfromtimestamp(spec)

    return spec


# ------------------------------------------------------------------------------
#
def time_diff(dt_abs, dt_stamp):
    '''
    return the time difference bewteen  two datetime
    objects in seconds (incl. fractions).  Exceptions (like on improper data
    types) fall through.
    '''

    delta = dt_stamp - dt_abs

    # make it easy to use seconds since epoch instead of datetime objects
    if  isinstance(delta, int) or \
        isinstance(delta, float)  :
        return delta

    import datetime
    if  not isinstance(delta, datetime.timedelta):
        raise TypeError("difference between '%s' and '%s' is not a .timedelta"
                     % (type(dt_abs), type(dt_stamp)))

    # get seconds as float
    seconds = delta.seconds + delta.microseconds / 1E6
    return seconds


# ------------------------------------------------------------------------------
#
def all_pairs(iterable, n):
    '''
    [ABCD] -> [AB], [AC], [AD], [BC], [BD], [CD]
    '''

    import itertools
    return list(itertools.combinations(iterable, n))


# ------------------------------------------------------------------------------
#
def cluster_list(iterable, n):
    '''
    s -> [ s0,  s1,    s2,    ... sn-1  ], 
         [ sn,  sn+1,  sn+2,  ... s2n-1 ], 
         [ s2n, s2n+1, s2n+2, ... s3n-1 ], 
         ...
    '''

    from itertools import izip
    return izip(*[iter(iterable)] * n)


# ------------------------------------------------------------------------------
# From https://docs.python.org/release/2.3.5/lib/itertools-example.html
#
def window(seq, n=2):
    '''
    Returns a sliding window (of width n) over data from the iterable"
    s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...
    '''

    from itertools import islice

    it = iter(seq)
    result = tuple(islice(it, n))

    if len(result) == n:
        yield result

    for elem in it:
        result = result[1:] + (elem,)
        yield result


# ------------------------------------------------------------------------------
#
def round_to_base(value, base=1):
    '''
    This method expects an integer or float value, and will round it to any
    given integer base.  For example:

      1.5, 2 -> 2
      3.5, 2 -> 4
      4.5, 2 -> 4

      11.5, 20 -> 20
      23.5, 20 -> 20
      34.5, 20 -> 40

    The default base is '1'.
    '''

    return int(base * round(float(value) / base))


# ------------------------------------------------------------------------------
#
def round_upper_bound(value):
    '''
    This method expects an integer or float value, and will return an integer upper
    bound suitable for example to define plot ranges.  The upper bound is the
    smallest value larger than the input value which is a multiple of 1, 2 or
    5 times the order of magnitude (10**x) of the value.
    '''

    bound = 0
    order = 0
    check = [1, 2, 5]

    while True:

        for c in check:

            bound = c * (10**order)

            if value < bound:
                return bound

        order += 1


# ------------------------------------------------------------------------------
#
def islist(thing):
    '''
    return True if a thing is a list thing, False otherwise
    '''

    return isinstance(thing, list)


# ------------------------------------------------------------------------------
#
def tolist(thing):
    '''
    return a non-list thing into a list thing
    '''

    if islist(thing):
        return thing
    return [thing]


# ------------------------------------------------------------------------------
#
# to keep RU 2.6 compatible, we provide import_module which works around some
# quirks of __import__ when being used with dotted names. This is what the
# python docs recommend to use.  This basically steps down the module path and
# loads the respective submodule until arriving at the target.
#
# FIXME: should we cache this?
#
def import_module(name):

    mod = __import__(name)
    for s in name.split('.')[1:]:
        mod = getattr(mod, s)
    return mod


# ------------------------------------------------------------------------------
#
# as import_module, but without the import part :-P
#
# FIXME: should we cache this?
#
def find_module(name):

    package = pkgutil.get_loader(name)

    if not package:
        return None

    return package.filename


# ------------------------------------------------------------------------------
#
_hostname = None
def get_hostname():
    '''
    Look up the hostname
    '''

    if not _hostname:

        global _hostname
        if socket.gethostname().find('.') >= 0:
            _hostname = socket.gethostname()
        else:
            _hostname = socket.gethostbyaddr(socket.gethostname())[0]

    return _hostname


# ------------------------------------------------------------------------------
#
_hostip = None
def get_hostip(req=None, logger=None):
    '''
    Look up the ip number for a given requested interface name.
    If interface is not given, do some magic.
    '''

    global _hostip
    if _hostip:
        return _hostip

    AF_INET = netifaces.AF_INET

    # We create a ordered preference list, consisting of:
    #   - given arglist
    #   - white list (hardcoded preferred interfaces)
    #   - black_list (hardcoded unfavorable interfaces)
    #   - all others (whatever is not in the above)
    # Then this list is traversed, we check if the interface exists and has an
    # IP address.  The first match is used.

    if req: 
        if not isinstance(req, list):
            req = [req]
    else:
        req = []

    white_list = [
                  'ipogif0',  # Cray's
                  'br0',      # SuperMIC
                  'eth0',     # desktops etc.
                  'wlan0'     # laptops etc.
                 ]

    black_list = [
                  'lo',      # takes the 'inter' out of the 'net'
                  'sit0'     # ?
                 ]

    all  = netifaces.interfaces()
    rest = [iface for iface in all
                   if iface not in req and
                      iface not in white_list and
                      iface not in black_list]

    preflist = req + white_list + black_list + rest

    for iface in preflist:

        if iface not in all:
            if logger:
                logger.debug('check iface %s: does not exist', iface)
            continue

        info = netifaces.ifaddresses(iface)
        if AF_INET not in info:
            if logger:
                logger.debug('check iface %s: no information', iface)
            continue

        if not len(info[AF_INET]):
            if logger:
                logger.debug('check iface %s: insufficient information', iface)
            continue

        if not info[AF_INET][0].get('addr'):
            if logger:
                logger.debug('check iface %s: disconnected', iface)
            continue

        ip = info[AF_INET][0].get('addr')
        if logger:
            logger.debug('check iface %s: ip is %s', iface, ip)

        if ip:
            _hostip = ip
            return ip

    raise RuntimeError('could not determine ip on %s' % preflist)


# ------------------------------------------------------------------------------
#
def watch_condition(cond, target=None, timeout=None, interval=0.1):
    '''
    Watch a given condition (a callable) until it returns the target value, and
    return that value.  Stop watching on timeout, in that case return None.  The
    condition is tested approximately every 'interval' seconds.
    '''

    start = time.time()
    while True:
        ret = cond()
        if ret == target:
            return ret
        if timeout and time.time() > start + timeout:
            return None
        time.sleep(interval)


# ------------------------------------------------------------------------------
#
def name2env(name):
    '''
    convert a name of the for 'radical.pilot' to an env vare base named
    'RADICAL_PILOT'.
    '''

    return name.replace('.', '_').upper()


# ------------------------------------------------------------------------------
#
def get_env_ns(key, ns, default=None):
    '''
    get an environment setting within a namespace.  For example. 

        get_env_ns('verbose', 'radical.pilot.umgr'), 

    will return the value of the first found env variable from the following
    sequence:

        RADICAL_PILOT_UMGR_VERBOSE
        RADICAL_PILOT_VERBOSE
        RADICAL_VERBOSE

    or 'None' if none of the above is set.  The given `name` and `key` are
    converted to upper case, dots are replaced by underscores.

    Note that an environment variable set with

        export RADICAL_VERBOSE=

    (ie. without an explicit, non-empty value) will be returned as an empty
    string.
    '''

    ns     = name2env(ns)
    key    = name2env(key)
    base   = ''
    checks = list()
    for elem in ns.split('_'):
        base += elem + '_'
        check = base + key
        checks.append(check)

    for check in reversed(checks):
        if check in os.environ:
            return os.environ[check]

    return default


# ------------------------------------------------------------------------------
#
def stack():

    ret = {'sys'     : {'python'     : sys.version.split()[0],
                        'pythonpath' : os.environ.get('PYTHONPATH',  ''),
                        'virtualenv' : os.environ.get('VIRTUAL_ENV', '') or
                                       os.environ.get('CONDA_DEFAULT_ENV','')}, 
           'radical' : dict()
          }


    modules = ['radical.utils', 
               'radical.saga', 
               'saga', 
               'radical.pilot', 
               'radical.entk',
               'radical.analytics']

    for mod in modules:
        try:
            tmp = import_module(mod)
            ret['radical'][mod] = tmp.version_detail
        except:
            pass

    return ret


# ------------------------------------------------------------------------------
#
def get_size(obj, seen=None, strict=False):

    size   = sys.getsizeof(obj)
    obj_id = id(obj)

    if strict:
        # perform recursion checks
        if seen is None:
            seen = set()
        if obj_id in seen:
            return 0
        seen.add(obj_id)

    if isinstance(obj, dict):
        size += sum([get_size(v, seen, strict) for v in obj.values()])
        size += sum([get_size(k, seen, strict) for k in obj.keys()])

    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen, strict)

    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen, strict) for i in obj])

    return size


# ------------------------------------------------------------------------------
#
def dockerized():

    if os.path.exists('/.dockerenv'):
        return True
    return False


# ------------------------------------------------------------------------------
#
def sh_callout(cmd, stdout=False, stderr=False, shell=False):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''

    # convert string into arg list if needed
    if not shell and isinstance(cmd, basestring):
        cmd = shlex.split(cmd)

    if stdout: stdout = sp.PIPE
    else     : stdout = None

    if stderr: stderr = sp.PIPE
    else     : stderr = None

    p = sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell)

    if not stdout and not stderr:
        ret = p.wait()
    else:
        stdout, stderr = p.communicate()
        ret            = p.returncode
    return stdout, stderr, ret


# ------------------------------------------------------------------------------
#
def sh_callout_bg(cmd, shell=False):
    '''
    call a shell command, don't capture anything, forget handle
    '''

    # convert string into arg list if needed
    if not shell and isinstance(cmd, basestring):
        cmd = shlex.split(cmd)

    sp.Popen(cmd, stdout=None, stderr=None, shell=shell)

    return 


# ------------------------------------------------------------------------------
#
def is_str(s):
    return isinstance(s, basestring)


# ------------------------------------------------------------------------------
#
def sh_callout_async(cmd, stdout=True, stderr=False, shell=False):
    '''

    Run a command, and capture stdout/stderr if so flagged.  The call will
    return an PROC object instance on which the captured output can be retrieved
    line by line (I/O is line buffered).  When the process is done, a `None`
    will be returned on the I/O queues.

    Line breaks are stripped.

    stdout/stderr: True [default], False, string
      - False : discard I/O
      - True  : capture I/O as queue [default]
      - string: capture I/O as queue, also write to named file

    shell: True, False [default]
      - pass to popen

    PROC:
      - PROC.stdout         : `queue.Queue` instance delivering stdout lines
      - PROC.stderr         : `queue.Queue` instance delivering stderr lines
      - PROC.state          : ru.RUNNING, ru.DONE, ru.FAILED
      - PROC.rc             : returncode (None while ru.RUNNING)
      - PROC.stdout_filename: name of stdout file (when available)
      - PROC.stderr_filename: name of stderr file (when available)
    '''
    # NOTE: Fucking python screws up stdio buffering when threads are used,
    #       *even if the treads do not perform stdio*.  Its possible that the
    #       logging module interfers, too.  Either way, I am fed up debugging
    #       this shit, and give up.  This method does not work for threaded
    #       python applications.
    assert(False), 'this is broken for python apps'


    # --------------------------------------------------------------------------
    #
    class _PROC(object):

        # ----------------------------------------------------------------------
        def __init__(self, cmd, stdout, stderr, shell):

            cmd = cmd.strip()

            self._out_c = bool(stdout)               # flag stdout capture
            self._err_c = bool(stderr)               # flag stderr capture

            self._out_r, self._out_w = os.pipe()     # get stdout from child
            self._err_r, self._err_w = os.pipe()     # get stderr from child

            self._out_o = os.fdopen(self._out_r)     # file object for out ep
            self._err_o = os.fdopen(self._err_r)     # file object for err ep

            self._out_q = queue.Queue()              # put stdout to parent
            self._err_q = queue.Queue()              # put stderr to parent

            if is_str(stdout): self._out_f = open(stdout, 'w')
            else             : self._out_f = None

            if is_str(stderr): self._err_f = open(stderr, 'w')
            else             : self._err_f = None

            self.state = ruc.RUNNING
            self._proc = sp.Popen(cmd, stdout=self._out_w, stderr=self._err_w,
                                  shell=shell, bufsize=1)

            t = mt.Thread(target=self._watch) 
            t.daemon = True
            t.start()

            self.rc = None  # return code



        @property
        def stdout(self):
            if not self._out_c:
                raise RuntimeError('stdout not captured')
            return self._out_q

        @property
        def stderr(self):
            if not self._err_c:
                raise RuntimeError('stderr not captured')
            return self._err_q


        @property
        def stdout_filename(self):
            if not self._out_f:
                raise RuntimeError('stdout not recorded')
            return self._out_f.name

        @property
        def stderr_filename(self):
            if not self._err_f:
                raise RuntimeError('stderr not recorded')
            return self._err_f.name


        # ----------------------------------------------------------------------
        def _watch(self):

            poller = select.poll()
            poller.register(self._out_r, select.POLLIN | select.POLLHUP)
            poller.register(self._err_r, select.POLLIN | select.POLLHUP)

            # try forever to read stdout and stderr, stop only when either
            # signals that process died
            while True:

                active = False
                fds    = poller.poll(100)  # timeout configurable (ms)

                for fd,mode in fds:

                    if mode & select.POLLHUP:
                        # fd died - #grab data from other fds
                        continue

                    if fd    == self._out_r:
                        o_in  = self._out_o
                        q_out = self._out_q
                        f_out = self._out_f

                    elif fd  == self._err_r:
                        o_in  = self._err_o
                        q_out = self._err_q
                        f_out = self._err_f

                    line = o_in.readline()  # `bufsize=1` in `popen`

                    if line:
                        # found valid data (active)
                        active = True
                        if q_out: q_out.put(line.rstrip('\n'))
                        if f_out: f_out.write(line)

                # no data received - check process health
                if not active and self._proc.poll() is not None:

                    # process is dead
                    self.rc = self._proc.returncode

                    if self.rc == 0: self.state = ruc.DONE
                    else           : self.state = ruc.FAILED

                    if self._out_q: self._out_q.put(None)  # signal EOF
                    if self._err_q: self._err_q.put(None)  # signal EOF

                    if self._out_q: self._out_q.join()     # ensure reads
                    if self._err_q: self._err_q.join()     # ensure reads

                    return  # finishes thread

    # --------------------------------------------------------------------------

    return _PROC(cmd=cmd, stdout=stdout, stderr=stderr, shell=shell)


# ------------------------------------------------------------------------------
#
def get_radical_base(module=None):
    '''
    Several parts of the RCT stack store state on the file system.  This should
    usually be under `$HOME/.radical` - but that location is not always
    available or desireable.  We interpret the env variable `RADICAL_BASE_DIR`,
    and fall back to `pwd` if neither that nor `$HOME` exists.

    The optional `module` parameter will result in the respective subdir name to
    be appended.  The resulting dir is created (if it does not exist), and the
    name is returned.
    '''


    base = os.environ.get("RADICAL_BASE_DIR")

    if not base or not os.path.isdir(base):
        base  = os.environ.get("HOME")

    if not base or not os.path.isdir(base):
        base  = os.environ.get("PWD")

    if not base or not os.path.isdir(base):
        base  = os.getcwd()

    if module: base += '/.radical/%s/' % module
    else     : base += '/.radical/'

    if not os.path.isdir(base):
        os.makedirs(base)

    return base


# ------------------------------------------------------------------------------

