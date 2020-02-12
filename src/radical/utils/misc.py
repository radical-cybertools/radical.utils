
import os
import sys
import glob
import time
import errno
import socket
import tarfile
import datetime
import tempfile
import itertools
import netifaces

from .         import url       as ruu
from .modules  import import_module
from .ru_regex import ReString


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
    except ImportError as e:
        msg  = " \n\npymongo is not available -- install RU with: \n\n"
        msg += "  (1) pip install --upgrade -e '.[pymongo]'\n"
        msg += "  (2) pip install --upgrade    'radical.utils[pymongo]'\n\n"
        msg += "to resolve that dependency (or install pymongo manually).\n"
        msg += "The first version will work for local installation, \n"
        msg += "the second one for installation from pypi.\n\n"
        raise ImportError(msg) from e

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
            except:
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

        if  not is_string(directive):
            raise TypeError("file staging directives muct by of type string, "
                            "not %s" % type(directive))

        rs = ReString(directive)

        if  rs // r'^(?P<one>.+?)\s*(?P<op><|<<|>|>>)\s*(?P<two>.+)$':
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

    return zip(*[iter(iterable)] * n)


# ------------------------------------------------------------------------------
# From https://docs.python.org/release/2.3.5/lib/itertools-example.html
#
def window(seq, n=2):
    '''
    Returns a sliding window (of width n) over data from the iterable"
    s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...
    '''

    it = iter(seq)
    result = tuple(itertools.islice(it, n))

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
    This method expects an integer or float value, and will return an integer
    upper bound suitable for example to define plot ranges.  The upper bound is
    the smallest value larger than the input value which is a multiple of 1,
    2 or 5 times the order of magnitude (10**x) of the value.
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
def is_tuple(data):
    '''
    return True if given data are a `tuple`, `False` otherwise
    '''

    return isinstance(data, tuple)


# ------------------------------------------------------------------------------
#
def as_tuple(data):
    '''
    return non-tuple data into a tuple.
    '''

    if   data is None  : return tuple()
    elif is_tuple(data): return data
    else               : return (data, )


# ------------------------------------------------------------------------------
#
def is_list(data):
    '''
    return True if given data are a `list`, `False` otherwise
    '''

    return isinstance(data, list)


# ------------------------------------------------------------------------------
#
def as_list(data):
    '''
    return non-list data into a list.
    '''

    if   data is None : return []
    elif is_list(data): return data
    else              : return [data]


# ------------------------------------------------------------------------------
#
def to_type(data):

    if not isinstance(data, str):
        return data

    try   : return(int(data))
    except: pass

    try   : return(float(data))
    except: pass

    return data


# ------------------------------------------------------------------------------
#
def is_seq(data):
    '''
    tests if the given data is a sequence (but not a string)
    '''
    return hasattr(data, '__iter__') and not is_string(data)


# ------------------------------------------------------------------------------
#
def is_string(data):
    '''
    tests if the given data are a `string` type
    '''
    return isinstance(data, str)


# ------------------------------------------------------------------------------
#
def as_string(data):
    '''
    Make a best-effort attempt to convert bytes to strings.  Iterate through
    lists and dicts, but leave all other datatypes alone.
    '''

    if isinstance(data, dict):
        return {as_string(k): as_string(v) for k,v in data.items()}

    elif isinstance(data, list):
        return [as_string(e) for e in data]

    elif isinstance(data, bytes):
        return bytes.decode(data, 'utf-8')

    else:
        return data


# ------------------------------------------------------------------------------
#
def is_str(s):
    return isinstance(s, str)


# ------------------------------------------------------------------------------
#
def is_bytes(data):
    '''
    checks if the given data are of types `bytes` or `bytearray`
    '''
    return isinstance(data, (bytes, bytearray))


# ------------------------------------------------------------------------------
# thanks to
# http://stackoverflow.com/questions/956867/#13105359
def as_bytes(data):
    '''
    Make a best-effort attempt to convert strings to bytes.  Iterate through
    lists and dicts, but leave all other datatypes alone.
    '''

    if isinstance(data, dict):
        return {as_bytes(k): as_bytes(v) for k,v in data.items()}

    elif isinstance(data, list):
        return [as_bytes(e) for e in data]

    elif isinstance(data, str):
        return str.encode(data, 'utf-8')

    else:
        return data


# ------------------------------------------------------------------------------
#
_hostname = None


def get_hostname():
    '''
    Look up the hostname
    '''

    global _hostname                                     # pylint: disable=W0603
    if not _hostname:

        if socket.gethostname().find('.') >= 0:
            _hostname = socket.gethostname()
        else:
            _hostname = socket.gethostbyaddr(socket.gethostname())[0]

    return _hostname


# ------------------------------------------------------------------------------
#
_hostip = None


def get_hostip(req=None, log=None):
    '''
    Look up the ip number for a given requested interface name.
    If interface is not given, do some magic.
    '''

    global _hostip                                       # pylint: disable=W0603
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

        RADICAL_PILOT_UMGR_LOG_LVL
        RADICAL_PILOT_LOG_LVL
        RADICAL_LOG_LVL

    or 'None' if none of the above is set.  The given `name` and `key` are
    converted to upper case, dots are replaced by underscores.

    Note that an environment variable set with

        export RADICAL_LOG_LVL=

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
            val = os.environ[check]
            return val

    return default


# ------------------------------------------------------------------------------
#
def expand_env(data, env=None, ignore_missing=True):
    '''
    Expand the given data with environment variables from `os.environ`.
    If `env` is provided, use that dictionary for expansion instead.

    `data` can be one of three types:

      - dictionary: `expand_env` is applied to all *values* of the dictionary
      - sequence  : `expand_env` is applied to all elements of the sequence
      - string    : `expand_env` is applied to the string itself


    The method will alter dictionaries and iterables in place, but will return
    a copy of scalar strings, as it seems to be custom in Python.  Other data
    types are silently ignored and not altered.

    The replacement in strings is performed for the following variable specs:

        assume  `export BAR=bar BIZ=biz`:

            ${BAR}     : foo_${BAR}_baz      -> foo_bar_baz
            ${BAR:buz} : foo_${BAR:buz}_baz  -> foo_bar_baz
            ${BAR:$BUZ}: foo_${BAR:$BIZ}_baz -> foo_biz_baz

        assume `unset BAR; export BIZ=biz`, `ignore_missing=True`

            ${BAR}     : foo_${BAR}_baz      -> foo__baz
            ${BAR:buz} : foo_${BAR:buz}_baz  -> foo_buz_baz
            ${BAR:$BUZ}: foo_${BAR:$BIZ}_baz -> foo_biz_baz

        assume `unset BAR; export BIZ=biz`, `ignore_missing=False`

            ${BAR}     : foo_${BAR}_baz      -> ValueError('cannot expand $BAR')
            ${BAR:buz} : foo_${BAR:buz}_baz  -> foo_buz_baz
            ${BAR:$BIZ}: foo_${BAR:$BIZ}_baz -> foo_biz_baz

    The method will also opportunistically convert strings to integers or
    floats if they are formatted that way and contain no other characters.
    '''

    # no data: None, empty dict / sequence / string
    if not data:
        return data

    # dict type
    elif isinstance(data, dict):

        for k,v in data.items():
            data[k] = expand_env(v, env, ignore_missing)
        return data

    # sequence types: list, set, tuple - but not string
    elif is_seq(data):

        for idx, elem in enumerate(data):
            data[idx] = expand_env(elem, env, ignore_missing)
        return data

    # all other non-string types are left alone
    elif not is_string(data):
        return data

    if '$' not in data:
        # nothing to expand
        return data

    # fall back to process env if no other expansion dict is specified
    if not env:
        env = os.environ

    # strings are not expanded in place - create a new one to fill.
    # iterate over the orginial string as long as there is something to expand
    ret = ''
    while data:

        data = ReString(data)

        # idea     :   pre     ${  Vari_ABLE            : val     } post
        # captures :  (   )(?    (                  )(?  (     ))  )(  )
        # indexes  :  1          2                       3          4
        with data // r'(.*?)(?:\${([a-zA-Z][a-zA-Z0-9_-]+)(?::([^}]+))?})(.*)' \
            as res:

            if not res:
                ret += data
                break

            pre  = res[0]
            key  = res[1]
            val  = res[2]
            post = res[3]

            if not ignore_missing and key not in env:
                raise ValueError('cannot expand $%s' % key)

            if pre  is None: pre  = ''
            if val  is None: val  = ''
            if post is None: post = ''

            # support env expansion of val, as in
            #   LOGDIR : "${RCT_LOGDIR:$PWD}"
            if val.startswith('$'):
                val = env.get(val[1:], '')

            val = env.get(key, val)

            if key and not pre and not post and not val:
                # we had something to expand, and that expansion is all there is
                # in the key, and the expand failed - then the result it not an
                # empty string but None
                return None

            ret += pre
            ret += val

            data = ReString(post)

    # attempt string-to-type conversion (int and float detection only)
    return to_type(ret)


# ------------------------------------------------------------------------------
#
def stack():
    '''
    returns a dict with information about the currently active python
    interpreter and all radical modules (incl. version details)
    '''

    ret = {'sys'     : {'python'     : sys.version.split()[0],
                        'pythonpath' : os.environ.get('PYTHONPATH',  ''),
                        'virtualenv' : os.environ.get('VIRTUAL_ENV', '') or
                                       os.environ.get('CONDA_DEFAULT_ENV','')},
           'radical' : dict()
          }

    import radical
    path = radical.__path__
    if isinstance(path, list):
        path = path[0]

    if isinstance(path, str):
        rpath = path
    else:
        rpath = path._path                               # pylint: disable=W0212

    if isinstance(rpath, list):
        rpath = rpath[0]

    for mpath in glob.glob('%s/*' % rpath):

        if os.path.isdir(mpath):

            mbase = os.path.basename(mpath)
            mname = 'radical.%s' % mbase

            if mbase.startswith('_'):
                continue

            try:
                ret['radical'][mname] = import_module(mname).version_detail
            except Exception as e:
                if 'RADICAL_DEBUG' in os.environ:
                    ret['radical'][mname] = str(e)
                else:
                    ret['radical'][mname] = '?'

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

    elif hasattr(obj, '__iter__') and \
        not isinstance(obj, (str, bytes, bytearray)):
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
def get_radical_base(module=None):
    '''
    Several parts of the RCT stack store state on the file system.  This should
    usually be under `$HOME/.radical` - but that location is not always
    available or desireable.  We interpret the env variable `RADICAL_BASE_DIR`,
    and fall back to `pwd` if neither that nor `$HOME` exists.

    The optional `module` parameter will result in the respective subdir name to
    be appended.  The resulting dir is created (if it does not exist), and the
    name is returned.  Any `.` (dot) characters in `module` are replaced by
    slashes.  Leading `radical/` element is removed.
    '''

    if module:
        module = module.replace('.', '/')
        if module.startswith('radical/'):
            module = module[8:]

    base = os.environ.get("RADICAL_BASE_DIR")

    if not base or not os.path.isdir(base):
        base  = os.environ.get("HOME")

    if not base or not os.path.isdir(base):
        base  = os.environ.get("PWD")

    if not base or not os.path.isdir(base):
        base  = os.getcwd()

    if module: base += '/.radical/%s/' % module
    else     : base += '/.radical/'

    rec_makedir(base)

    return base


# ------------------------------------------------------------------------------
#
def rec_makedir(target):
    '''
    recursive makedir which ignores errors if dir already exists
    '''

    try:
        os.makedirs(target)

    except OSError as e:
        # ignore failure on existing directory
        if e.errno == errno.EEXIST and os.path.isdir(os.path.dirname(target)):
            pass
        else:
            raise


# ------------------------------------------------------------------------------
#
def mktar(tarname, fnames=None, data=None):
    '''
    Create a tarfile at the given `tarname`, and pack all files given in
    `fnames` into it, and also pack any `data` blobs.

    `fnames` is expected to be list, where each element is either a string
    pointing to a file to be added under that name, or a tuple where the first
    element points again to the file to be packed, and the second element
    specifies the name under which the file should be packed into the archive.
    And OSError will be raised if the file does not exist.

    `data` is expected to be a list of tuples, where the first element is a set
    of bytes comprising the data to be written into the archive, and the second
    element again specifies the name of the tarred file.

    Note that this method always create bzip'ed tarfiles, but will never change
    the `tarname` to reflect that.
    '''

    tar = tarfile.open(tarname, "w:bz2")
    if fnames:
        for element in fnames:
            if isinstance(str, element):
                tar.add(element)
            else:
                src, tgt = element
                tar.add(src, arcname=tgt)

    if data:
        for fname, fdata in data:
            tmp_name, tmp_fd = tempfile.mkstemp()
            tmp_fd.write(fdata)
            tmp_fd.close()
            tar.add(tmp_name, fname)
            os.unlink(tmp_name)

    tar.close()


# ------------------------------------------------------------------------------

