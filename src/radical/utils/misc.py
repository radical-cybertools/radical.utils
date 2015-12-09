import os
import sys
import time
import regex
import signal
import threading
import traceback
import url as ruu

# ------------------------------------------------------------------------------
#
def split_dburl(dburl, default_dburl=None) :
    """
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    """

    # if the given URL does not contain schema nor host, the default URL is used
    # as base, and the given URL string is appended to the path element.
    
    url = ruu.Url (dburl)

    if not url.schema and not url.host :
        url      = ruu.Url (default_dburl)
        url.path = dburl

    # NOTE: add other data base schemes here...
    if 'mongodb' not in url.schema.split('+'):
        raise ValueError ("url must be a 'mongodb://' or 'mongodb+ssl://' url, not '%s'" % dburl)

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

    if  path.startswith ('/') :
        path = path[1:]
    path_elems = path.split ('/')

    dbname = None
    cname  = None
    pname  = None

    if  len(path_elems)  >  0 :
        dbname = path_elems[0]

    if  len(path_elems)  >  1 :
        dbname = path_elems[0]
        cname  = path_elems[1]

    if  len(path_elems)  >  2 :
        dbname = path_elems[0]
        cname  = path_elems[1]
        pname  = '.'.join (path_elems[2:])

    if  dbname == '.' : 
        dbname = None

    return [host, port, dbname, cname, pname, user, pwd, ssl]


# ------------------------------------------------------------------------------
#
def mongodb_connect (dburl, default_dburl=None) :
    """
    connect to the given mongodb, perform auth for the database (if a database
    was given).
    """

    try :
        import pymongo
    except ImportError :
        msg  = " \n\npymongo is not available -- install radical.utils with: \n\n"
        msg += "  (1) pip install --upgrade -e '.[pymongo]'\n"
        msg += "  (2) pip install --upgrade    'radical.utils[pymongo]'\n\n"
        msg += "to resolve that dependency (or install pymongo manually).\n"
        msg += "The first version will work for local installation, \n"
        msg += "the second one for installation from pypi.\n\n"
        raise ImportError (msg)

    [host, port, dbname, cname, pname, user, pwd, ssl] = split_dburl(dburl, default_dburl)

    mongo = pymongo.MongoClient (host=host, port=port, ssl=ssl)
    db    = None

    if  dbname :
        db = mongo[dbname]

        if  user and pwd :
            db.authenticate (user, pwd)


    else :

        # if no DB is given, we try to auth against all databases.
        for dbname in mongo.database_names () :
            try :
                mongo[dbname].authenticate (user, pwd)
            except Exception as e :
                pass 


    return mongo, db, dbname, cname, pname


# ------------------------------------------------------------------------------
#
def parse_file_staging_directives (directives) :
    """
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
    """

    bulk = True
    if  not isinstance (directives, list) :
        bulk       = False
        directives = [directives]

    ret = list()

    for directive in directives :

        if  not isinstance (directive, basestring) :
            raise TypeError ("file staging directives muct by of type string, "
                             "not %s" % type(directive))

        rs = regex.ReString (directive)

        if  rs // '^(?P<one>.+?)\s*(?P<op><|<<|>|>>)\s*(?P<two>.+)$' :
            res = rs.get ()
            ret.append ([res['one'], res['two'], res['op']])

        else :
            ret.append ([directive, directive, '='])

    if  bulk : return ret
    else     : return ret[0]


# ------------------------------------------------------------------------------
#
def time_stamp (spec) :

    if  isinstance (spec, int)   or \
        isinstance (spec, long)  or \
        isinstance (spec, float) :

        import datetime
        return datetime.datetime.utcfromtimestamp (spec)

    return spec


# ------------------------------------------------------------------------------
#
def time_diff (dt_abs, dt_stamp) :
    """
    return the time difference bewteen  two datetime 
    objects in seconds (incl. fractions).  Exceptions (like on improper data
    types) fall through.
    """

    delta = dt_stamp - dt_abs

    # make it easy to use seconds since epoch instead of datetime objects
    if  isinstance (delta, int)   or \
        isinstance (delta, long)  or \
        isinstance (delta, float) :
        return delta

    import datetime
    if  not isinstance  (delta, datetime.timedelta) :
        raise TypeError ("difference between '%s' and '%s' is not a .timedelta" \
                      % (type(dt_abs), type(dt_stamp)))

    # get seconds as float 
    seconds = delta.seconds + delta.microseconds/1E6
    return seconds


# --------------------------------------------------------------------
#
def get_trace():

    trace = sys.exc_info ()[2]

    if  trace :
        stack           = traceback.extract_tb  (trace)
        traceback_list  = traceback.format_list (stack)
        return "".join (traceback_list)

    else :
        stack           = traceback.extract_stack ()
        traceback_list  = traceback.format_list (stack)
        return "".join (traceback_list[:-1])


# ------------------------------------------------------------------------------
#
class DebugHelper (object) :
    """
    When instantiated, and when "RADICAL_DEBUG" is set in the environment, this
    class will install a signal handler for SIGUSR1.  When that signal is
    received, a stacktrace for all threads is printed to stdout.
    Additionally, we check if SIGINFO is available, which is generally bound to CTRL-T.
    """

    def __init__ (self) :

        if 'MainThread' not in threading.current_thread().name:
            # python only supports signals in main threads :/
            return

        if 'RADICAL_DEBUG' in os.environ :
            signal.signal(signal.SIGUSR1, print_stacktraces) # signum 30
            signal.signal(signal.SIGQUIT, print_stacktraces) # signum  3

            try:
                assert signal.SIGINFO
                signal.signal(signal.SIGINFO, print_stacktraces) # signum 29
            except AttributeError as e:
                pass


# ------------------------------------------------------------------------------
#
def print_stacktraces (signum=None, sigframe=None) :
    """
    signum, sigframe exist to satisfy signal handler signature requirements
    """

    this_tid = threading.currentThread().ident

    # if multiple processes (ie. a process group) get the signal, then all
    # traces are mixed together.  Thus we waid 'pid%100' milliseconds, in
    # the hope that this will stagger the prints.
    pid = int(os.getpid())
    time.sleep((pid%100)/1000)

    out  = "===============================================================\n"
    out += "RADICAL Utils -- Debug Helper -- Stacktraces\n"
    try :
        info = get_stacktraces ()
    except Exception as e:
        out += 'skipping frame'
        info = None

    if info:
        for tid, tname in info :

            if tid == this_tid : marker = '[active]'
            else               : marker = ''
            out += "---------------------------------------------------------------\n"
            out += "Thread: %s %s\n" % (tname, marker)
            out += "  PID : %s \n"   % os.getpid()
            out += "  TID : %s \n"   % tid
            for fname, lineno, method, code in info[tid,tname] :

                if code: code = code.strip()
                else   : code = '<no code>'

              # # [:-1]: .py vs. .pyc :/
              # if not (__file__[:-1] in fname and \
              #         method in ['get_stacktraces', 'print_stacktraces']):
                if method not in ['get_stacktraces', 'print_stacktraces']:
                    out += "  File: %s, line %d, in %s\n" % (fname, lineno, method)
                    out += "        %s\n" % code

    out += "==============================================================="

    sys.stdout.write("%s\n" % out)

    if 'RADICAL_DEBUG' in os.environ:
        with open('/tmp/ru.stacktrace.%s.log' % pid, 'w') as f:
            f.write ("%s\n" % out)

    return True


# --------------------------------------------------------------------------
#
def get_stacktraces () :

    id2name = {}
    for th in threading.enumerate():
        id2name[th.ident] = th.name

    ret = dict()
    for tid, stack in sys._current_frames().items():
        ret[tid,id2name[tid]] = traceback.extract_stack(stack)

    return ret


# ------------------------------------------------------------------------------
#
def all_pairs (iterable, n) :
    """
    [ABCD] -> [AB], [AC], [AD], [BC], [BD], [CD]
    """

    import itertools 
    return list(itertools.combinations (iterable, n))


# ------------------------------------------------------------------------------
#
def cluster_list (iterable, n) :
    """
    s -> (s0,s1,s2,...sn-1), (sn,sn+1,sn+2,...s2n-1), (s2n,s2n+1,s2n+2,...s3n-1), ...
    """

    from itertools import izip
    return izip(*[iter(iterable)]*n)


# ------------------------------------------------------------------------------
# From https://docs.python.org/release/2.3.5/lib/itertools-example.html
#
def window (seq, n=2) :
    """
    Returns a sliding window (of width n) over data from the iterable"
    s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ... 
    """

    from itertools import islice

    it = iter(seq)
    result = tuple(islice(it, n))

    if len(result) == n :
        yield result

    for elem in it :
        result = result[1:] + (elem,)
        yield result

# ------------------------------------------------------------------------------
# 
def round_to_base (value, base=1):
    """
    This method expects an integer or float value, and will round it to any
    given integer base.  For example:

      1.5, 2 -> 2
      3.5, 2 -> 4
      4.5, 2 -> 4

      11.5, 20 -> 20
      23.5, 20 -> 20
      34.5, 20 -> 40

    The default base is '1'.
    """

    return int(base * round(float(value) / base))


# ------------------------------------------------------------------------------
# 
def round_upper_bound (value):
    """
    This method expects an integer or float value, and will return an integer upper
    bound suitable for example to define plot ranges.  The upper bound is the
    smallest value larger than the input value which is a multiple of 1, 2 or
    5 times the order of magnitude (10**x) of the value.
    """

    bound = 0
    order = 0
    check = [1, 2, 5]

    while True:

        for c in check :

            bound = c*(10**order)

            if value < bound: 
                return bound

        order += 1


# ------------------------------------------------------------------------------
#
def islist(thing):
    """
    return True if a thing is a list thing, False otherwise
    """

    return isinstance(thing, list)


# ------------------------------------------------------------------------------
#
def tolist(thing):
    """
    return a non-list thing into a list thing
    """

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
def import_module(name):

    mod = __import__(name)
    for s in name.split('.')[1:]:
        mod = getattr(mod, s)
    return mod


# ------------------------------------------------------------------------------

