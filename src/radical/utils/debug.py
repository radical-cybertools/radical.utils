
import os
import sys
import time
import pprint
import signal
import random
import inspect
import pkgutil
import traceback

import importlib.util

import threading as mt

from .ids     import generate_id
from .threads import get_thread_name


# ------------------------------------------------------------------------------
#
_ps_cmd = 'ps -efw'
if sys.platform != 'darwin':
    _ps_cmd += ' --forest'


# ------------------------------------------------------------------------------
#
def get_trace():

    trace = sys.exc_info()[2]

    if trace:
        stack          = traceback.extract_tb(trace)
        traceback_list = traceback.format_list(stack)
        return ''.join(traceback_list)

    else:
        stack          = traceback.extract_stack()
        traceback_list = traceback.format_list(stack)
        return ''.join(traceback_list[:-1])


# ------------------------------------------------------------------------------
#
# pylint: disable=unused-argument
def print_stacktraces(signum=None, sigframe=None):
    '''
    signum, sigframe exist to satisfy signal handler signature requirements
    '''

    this_tid = mt.currentThread().ident

    # if multiple processes (ie. a process group) get the signal, then all
    # traces are mixed together.  Thus we waid 'pid%100' milliseconds, in
    # the hope that this will stagger the prints.
    pid = int(os.getpid())
    time.sleep((pid % 100) / 1000)

    out  = '=========================================================\n'
    out += 'RADICAL Utils -- Debug Helper -- Stacktraces\n'
    out += os.popen("%s | grep ' %s ' | grep -v grep" % (_ps_cmd, pid)).read()

    if _debug_helper:
        out += '---------------------------------------------------------\n'
        if _debug_helper.locks:
            out += 'Locks:\n'
        for name, lock in _debug_helper.locks.items():
            owner = lock.owner
            waits = lock.waits
            if not owner: owner = '-'
            out += '  %-60s: %s %s\n' % (name, owner, waits)

        if _debug_helper.rlocks:
            out += 'RLocks:\n'
        for name, rlock in _debug_helper.rlocks.items():
            owner = rlock.owner
            waits = rlock.waits
            if not owner: owner = '-'
            out += '  %-60s: %s %s\n' % (name, owner, waits)
        out += '---------------------------------------------------------\n'

    try:
        info = get_stacktraces()

    except Exception as e:
        out += 'skipping frame (%s)' % e
        info = None

    if info:

        for tid,tname in info:

            if tid == this_tid: marker = '[active]'
            else              : marker = ''

            out += '---------------------------------------------------------\n'
            out += 'Thread: %s %s\n' % (tname, marker)
            out += '  PID : %s \n'   % os.getpid()
            out += '  TID : %s \n'   % tid

            for fname,line,func,code in info[tid,tname]:

                if code: code = code.strip()
                else   : code = '<no code>'

              # # [:-1]: .py vs. .pyc:/
              # if not (__file__[:-1] in fname and \
              #         func in ['get_stacktraces', 'print_stacktraces']):
                if func not in ['get_stacktraces', 'print_stacktraces']:
                    out += '  File: %s, line %d, in %s\n' % (fname, line, func)
                    out += '        %s\n' % code

    out += '========================================================='

    sys.stdout.write('%s\n' % out)

    if 'RADICAL_DEBUG' in os.environ:
        with open('/tmp/ru.stacktrace.%s.log' % pid, 'w') as f:
            f.write('%s\n' % out)


# ------------------------------------------------------------------------------
#
def get_stacktraces():

    id2name = dict()
    for th in mt.enumerate():
        id2name[th.ident] = th.name

    ret = dict()
    stacklist = list(sys._current_frames().items())      # pylint: disable=W0212
    for tid, stack in stacklist:

        name = id2name.get(tid, 'noname')
        ret[tid, name] = traceback.extract_stack(stack)

    return ret


# ------------------------------------------------------------------------------
#
def print_stacktrace(msg=None, _stack=None):

    if not msg:
        msg = ''

    tname = mt.currentThread().name
    pid   = os.getpid()

    out   = '--------------\n'
    out  += 'RADICAL Utils -- Stacktrace [%s] [%s]\n' % (pid, tname)
    out  += '%s\n' % msg
    out  += os.popen("%s | grep ' %s ' | grep -v grep" % (_ps_cmd, pid)).read()

    if not _stack:
        _stack = get_stacktrace()

    for line in _stack:
        out += line.strip()
        out += '\n'

    out += '--------------\n'

    sys.stdout.write(out)


# ------------------------------------------------------------------------------
#
def print_exception_trace(msg=None):

    print_stacktrace(msg=msg, _stack=traceback.format_exc().split('\n'))


# ------------------------------------------------------------------------------
#
def get_stacktrace():

    return traceback.format_stack()[:-1]


# ------------------------------------------------------------------------------
#
def get_caller_name(skip=2):
    '''
    Get a name of a caller in the format module.class.method

    `skip` specifies how many levels of stack to skip while getting caller
    name. skip=1 means 'who calls me', skip=2 'who calls my caller' etc.

    An empty string is returned if skipped levels exceed stack height

    Kudos: http://stackoverflow.com/questions/2654113/ \
            python-how-to-get-the-callers-method-name-in-the-called-method
    '''

    stack = inspect.stack()
    start = 0 + skip

    if len(stack) < start + 1:
        return ''

    pframe = stack[start][0]
    name   = list()
    module = inspect.getmodule(pframe)

    # `modname` can be None when frame is executed directly in console
    # TODO(techtonik): consider using __main__
    if module:
        name.append(module.__name__)

    # detect classname
    if 'self' in pframe.f_locals:
        name.append(pframe.f_locals['self'].__class__.__name__)

    codename = pframe.f_code.co_name

    if codename != '<module>':  # top level usually
        name.append(codename)   # function or a method

    del pframe

    return '.'.join(name)


# ------------------------------------------------------------------------------
#
_verb  = False
if 'RADICAL_DEBUG' in os.environ:
    _verb = True

_raise_on_state = dict()
_raise_on_lock  = mt.Lock()


# ------------------------------------------------------------------------------
#
def raise_on(tag, log=None, msg=None):
    '''
    The purpose of this method is to artificially trigger error conditions for
    testing purposes, for example when handling the n'th unit, getting the n'th
    heartbeat signal, etc.

    The tag parameter is interpreted as follows: on the `n`'th invocation of
    this method with any given `tag`, an exception is raised, and the counter
    for that tag is reset.

    The limit `n` is set via an environment variable `RU_RAISE_ON_<tag>`, with
    `tag` in upper casing.  The environment will only be inspected during the
    first invocation of the method with any given tag.  The tag counter is
    process-local, but is shared amongst threads of that process.
    '''

    global _raise_on_state                               # pylint: disable=W0603
    global _raise_on_lock                                # pylint: disable=W0603

    with _raise_on_lock:

        if tag not in _raise_on_state:

            env = os.environ.get('RU_RAISE_ON_%s' % tag.upper())

            if env and env.startswith('RANDOM_'):
                # env is rnd spec
                rate  = int(env[7:])
                limit = 1

            elif env:
                # env is int
                rate  = 1
                limit = int(env)

            else:
                # no env set
                rate  = 1
                limit = 0

            _raise_on_state[tag] = {'count': 0,
                                    'rate' : rate,
                                    'limit': limit}

        _raise_on_state[tag]['count'] += 1

        count = _raise_on_state[tag]['count']
        limit = _raise_on_state[tag]['limit']
        rate  = _raise_on_state[tag]['rate']

        if msg    : info = '%s [%2d / %2d] [%s]' % (tag, count, limit, msg)
        elif _verb: info = '%s [%2d / %2d]'      % (tag, count, limit     )

        if log    : log.debug('raise_on checked   %s' , info)
        elif _verb: print('raise_on checked   %s' % info)

        if limit and count == limit:

            _raise_on_state[tag]['count'] = 0

            if rate == 1:
                val = limit

            else:
                val = random.randint(0, 100)

                if val > rate:
                    if log: log.warning('raise_on ignored   %s [%2d / %2d]',
                                          tag, val, rate)
                    elif _verb:   print('raise_on ignored   %s [%2d / %2d]'
                                       % (tag, val, rate))
                    return

            if log: log.warning('raise_on triggered %s [%2d / %2d]',
                                  tag, val, rate)
            elif _verb:   print('raise_on triggered %s [%2d / %2d]'
                               % (tag, val, rate))

            # reset counter and raise exception
            raise RuntimeError('raise_on for %s [%s]' % (tag, val))


# ------------------------------------------------------------------------------
#
def attach_pudb(log=None):

    # need to move here to avoid circular import
    from .threads import gettid

    host = '127.0.0.1'
  # host = gethostip()
    tid  = gettid()
    port = tid + 10000

    if log:
        log.info('debugger open: telnet %s %d', host, port)
    else:
        print('debugger open: telnet %s %d' % (host, port))

    try:
        import pudb                                      # pylint: disable=E0401
        from   pudb.remote import set_trace              # pylint: disable=E0401

        pudb.DEFAULT_SIGNAL = signal.SIGALRM

        set_trace(host=host, port=port, term_size=(200, 50))

    except Exception as e:
        if log:
            log.warning('failed to attach pudb (%s)', e)


# ------------------------------------------------------------------------------
#
_SNIPPET_PATHS = ['%s/.radical/snippets/' % os.environ.get('HOME', '/tmp')]


def add_snippet_path(path):
    '''
    add a path to the search path for dynamically loaded python snippets
    (see `ru.get_snippet()`).
    '''

    if 'RADICAL_DEBUG' in os.environ:

        global _SNIPPET_PATHS                            # pylint: disable=W0603

        if path not in _SNIPPET_PATHS:
            _SNIPPET_PATHS.append(path)


# ------------------------------------------------------------------------------
#
def get_snippet(sid):
    '''
    RU exposes small python snippets for runtime code insertion.  The usage is
    intended as follows:

      * a programmer implements a class
      * for some experiment or test, that class's behavior must be controled at
        runtime.
      * in all places where such an adaptation is expected to take place, the
        programmer inserts a hook like this:

            exec(ru.get_snippet('my_class.init_hook'))

      * this will trigger RU to search for python files of the name
        `my_class.init_hook.py` in `$HOME/.radical/snippets/' (default), and
        return their content for injection.

    The snippet search path can be extended by calling.

        ru.add_snippet_path(path)

    The `RADICAL_DEBUG` environment variable needs to be set for this method to
    do anything.  A snippet can use the following literal strinfgs which will be
    replaced by their actual values:

        '###SNIPPET_FILE###'  - filename from which snippet was loaded
        '###SNIPPET_PATH###'  - path in which the snippet file is located
        '###SNIPPET_ID###'    - the sid string used to identify the snippet
    '''

    if 'RADICAL_DEBUG' in os.environ:

        for path in _SNIPPET_PATHS:

            fname = '%s/%s.py' % (path, sid)

            try:
                with open(fname, 'r') as fin:
                    snippet = fin.read()
                    snippet = snippet.replace('###SNIPPET_FILE###', fname)
                    snippet = snippet.replace('###SNIPPET_PATH###', path)
                    snippet = snippet.replace('###SNIPPET_ID###',   sid)
                    return snippet
            except:
                pass

    return 'None'


# ------------------------------------------------------------------------------
#
class DebugHelper(object):
    '''
    When instantiated, and when 'RADICAL_DEBUG' is set in the environment, this
    class will install a signal handler for SIGUSR1.  When that signal is
    received, a stacktrace for all threads is printed to stdout.
    We also check if SIGINFO is available, which is generally bound to CTRL-T.

    Additionally, a call to 'dh.fs_block(info=None)' will create a file system
    based barrier: it will create a unique file in /tmp/ (based on 'name' if
    given), and dump the stack trace and any 'info' into it.  It then waits
    until that file has changed (touched or removed etc), and then returns.
    The wait is a simple pull based 'os.stat()' (once per sec).
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None, info=None):
        '''
        name: string to identify fs barriers
        info: static info to dump into fs barriers
        '''

        self.name   = name
        self.info   = info
        self.locks  = dict()
        self.rlocks = dict()

        if not self.name:
            self.name = str(id(self))

        if 'MainThread' not in mt.current_thread().name:
            # python only supports signals in main threads :-/
            return

        if 'RADICAL_DEBUG' in os.environ:
            signal.signal(signal.SIGUSR1, print_stacktraces)  # signum 30
            signal.signal(signal.SIGQUIT, print_stacktraces)  # signum  3

            try:
                assert signal.SIGINFO
                signal.signal(signal.SIGINFO, print_stacktraces)  # signum 29

            except AttributeError:  # stack unwind in progress
                pass


    # --------------------------------------------------------------------------
    #
    def register_lock(self, name, lock):
        assert(name not in self.locks), name
        self.locks[name] = lock


    # --------------------------------------------------------------------------
    #
    def register_rlock(self, name, rlock):
        assert(name not in self.rlocks), name
        self.rlocks[name] = rlock


    # --------------------------------------------------------------------------
    #
    def unregister_lock(self, name):
        assert(name in self.locks), name
        del(self.locks[name])


    # --------------------------------------------------------------------------
    #
    def unregister_rlock(self, name):
        assert(name in self.rlocks), name
        del(self.rlocks[name])


    # --------------------------------------------------------------------------
    #
    def fs_block(self, info=None):
        '''
        Dump state, info in barrier file, and wait for it tou be touched or
        read or removed, then continue.  Leave no trace.
        '''

        if 'RADICAL_DEBUG' not in os.environ:
            return

        try:
            pid = os.getpid()
            tid = mt.currentThread().ident

            fb  = '/tmp/ru.dh.%s.%s.%s' % (self.name, pid, tid)
            fd  = open(fb, 'w+')

            fd.seek(0,0)
            fd.write('\nSTACK TRACE:\n%s\n%s\n' % (time.time(), get_trace()))
            fd.write('\nSTATIC INFO:\n%s\n\n' % pprint.pformat(self.info))
            fd.write('\nINFO:\n%s\n\n' % pprint.pformat(info))
            fd.flush()

            new = os.stat(fb)
            old = new

            while old == new:
                new = os.stat(fb)
                time.sleep(0.1)

        except:
            # we don't care (much)...
            pass

        finally:
            if fd : fd.close()
            try   : os.unlink(fb)
            except: pass


# ------------------------------------------------------------------------------
#
_debug_helper = None
if 'RADICAL_DEBUG_HELPER' in os.environ:
    if not _debug_helper:
        _debug_helper = DebugHelper()


# ------------------------------------------------------------------------------
#
class Lock(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None):

        self.lock  = mt.Lock()
        self.owner = None
        self.waits = list()
        self.name  = name

        if not self.name:
            self.name = generate_id('lock')

        if _debug_helper:
            _debug_helper.register_lock(self.name, self)


    def __enter__(self):
        self.acquire()

    def __exit__(self, a, b, c):
        self.release()


    # --------------------------------------------------------------------------
    #
    def acquire(self, blocking=True):

        self.waits.append(get_thread_name())
        ret = self.lock.acquire(blocking=blocking)

        if ret is not False:
            self.owner = get_thread_name()

        self.waits.pop()
        return ret


    # --------------------------------------------------------------------------
    #
    def release(self):

        ret = self.lock.release()

        self.owner = None

        return ret


# ------------------------------------------------------------------------------
#
class RLock(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None):

        self.lock  = mt.RLock()
        self.owner = None
        self.waits = list()
        self.name  = name

        if not self.name:
            self.name = generate_id('rlock')

        if _debug_helper:
            _debug_helper.register_rlock(self.name, self)


    def __enter__(self):
        self.acquire()

    def __exit__(self, a, b, c):
        self.release()


    # --------------------------------------------------------------------------
    #
    def acquire(self, blocking=1):

        self.waits.append(get_thread_name())
        ret = self.lock.acquire(blocking=blocking)

        if ret is not False:
            self.owner = get_thread_name()

        self.waits.pop()

        return ret


    # --------------------------------------------------------------------------
    #
    def release(self):

        ret = self.lock.release()

        self.owner = None

        return ret


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
#
# as import_module, but without the import part :-P
#
def find_module(name):

    package = pkgutil.get_loader(name)

    if not package:
        return None

    if '_NamespaceLoader' in str(package):
        # since Python 3.5, loaders differ between modules and namespaces
        return package._path._path[0]                    # pylint: disable=W0212
    else:
        return os.path.dirname(package.get_filename())


# ------------------------------------------------------------------------------
#
# a helper to load functions and classes from user provided source file which
# are *not* installed as modules.  All symbols from that file are loaded, and
# returned is a dictionary with the following structure:
#
# symbols = {'classes'  : {'Foo': <class 'mod_0001.Foo'>,
#                          'Bar': <class 'mod_0001.Bar'>,
#                          ...
#                         },
#            'functions': {'foo': <function foo at 0x7f532d241d40>,
#                          'bar': <function bar at 0x7f532d241d40>,
#                          ...
#                         }
#           }
#
def import_file(path):

    uid  = generate_id('mod_')
    spec = importlib.util.spec_from_file_location(uid, path)
    mod  = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(mod)

    symbols = {'functions': dict(),
               'classes'  : dict()}

    for k,v in mod.__dict__.items():
        if not k.startswith('__'):
            if inspect.isclass(v):    symbols['classes'  ][k] = v
            if inspect.isfunction(v): symbols['functions'][k] = v

    return symbols


# ------------------------------------------------------------------------------

