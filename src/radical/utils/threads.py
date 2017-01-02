
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import signal
import thread
import threading
import traceback

import misc  as rumisc


# ------------------------------------------------------------------------------
#
NEW     = 'New'
RUNNING = 'Running'
DONE    = 'Done'
FAILED  = 'Failed'


# ------------------------------------------------------------------------------
#
def Event(*args, **kwargs):
    return threading.Event(*args, **kwargs)


# ------------------------------------------------------------------------------
#
class RLock(object):
    """
    This threading.RLock wrapper is supportive of lock debugging.  The only
    semantic difference to threading.RLock is that a lock acquired via the
    'with' statement can be released within the 'with' scope, w/o penalty when
    leaving the locked scope.  This supports up-calling callback semantics, but
    should be used with utter care, and rarely (such as on close()).

    see http://stackoverflow.com/questions/6780613/
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, obj=None):

        self._lock = threading.RLock()


    # --------------------------------------------------------------------------
    #
    def acquire(self):

        self._lock.acquire()


    # --------------------------------------------------------------------------
    #
    def release(self):

        try:
            self._lock.release()

        except RuntimeError as e:
            # lock has been released meanwhile - we allow that
            pass


    # --------------------------------------------------------------------------
    #
    def __enter__(self)                        : self.acquire() 
    def __exit__ (self, type, value, traceback): self.release()



# ------------------------------------------------------------------------------
#
class Thread(threading.Thread):
    """
    This `Thread` class is a thin wrapper around Python's native
    `threading.Thread` class, which adds some convenience methods.  
    """

    # TODO: create `run()` wrapper and initializers/finalizers similar to
    #       the ru.Process class

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None, cprofile=False, 
                       call=None, args=[], kwargs={}):

        if not callable(call):
            raise ValueError("Thread requires a callable to function, not %s" \
                            % (repr(call)))

        threading.Thread.__init__(self)

        self._name      = name
        self._cprofile  = cprofile
        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._result    = None
        self._exception = None
        self._traceback = None
        self._term      = mt.Event()
        self.daemon     = True  # we always use daemon threads to simplify
                                # the overall termination process


    # --------------------------------------------------------------------------
    #
    @classmethod
    def Run(self, call, *args, **kwargs):

        assert(call)
        assert(isinstance(call, callable))

        t = self(call=call, args=args, kwargs=kwargs)
        t.start()
        return t


    # --------------------------------------------------------------------------
    #
    @property 
    def tid(self):
        return self.tid


    # --------------------------------------------------------------------------
    #
    def run(self):
        if self._cprofile:
            import cprofile
            cprofiler = cProfile.Profile()
            try:
                return cprofiler.runcall(self._run)
            finally:
                self_thread = mt.current_thread()
                cprofiler.dump_stats('%s.cprof' % (self_thread.name))

        else:
            return self._run()


    # --------------------------------------------------------------------------
    #
    def _run(self):
        '''
        The RU Thread calss has two execution modes:

          * If a `call` was specified during construction, that call is
            executed, the result is stored.

          * If no `call` was specified, then we expect this class to be 
            inherited, and the inheriting class then *MUST* overload the
            `work()` method.  This method will be called repeatedly until 
            either one of the following conditions applies:

              * `work()` returns `False`, or
              * `work()` raises an exception
              * `stop()` is called on the Thread instance

            This mode is intentioanally very similar to the `ru.Process` syntax
            and semantics - the same assumptions hold for initializers, error
            modes, etc.  The main difference is that no attempt is made to watch
            thread health, and no communication channel is established.  

        NOTE: the underlying Python thread is a daemon thread, and all
              respective restrictions apply.  Specifically, daemon threads can
              cause the main application to hang on termination, and daemon
              threads cannot spawn processes, at least not via the
              multiprocessing module.
        '''

        # TODO: implement stop() -> self._term.set(); self.join()

        # 'call' mode
        if self._call:
            try:
                self._state     = RUNNING
                self._result    = self._call(*self._args, **self._kwargs)
                self._state     = DONE

            except Exception as e:
                tb = traceback.format_exc()
                self._traceback = tb
                self._exception = e
                self._state     = FAILED

            # all is done and said - begone!
            return

        # 'work' mode
        else:
            try:
                # initialize thread class
                self._rup_initialize()

                # enter the main loop and repeatedly call 'work()'.  
                #
                # If `work()` ever returns `False`, we break out of the loop to call the
                # finalizers and terminate.
                while not self._term.is_set():
                
                    # des Pudel's Kern
                    if not self.work():
                        self._log.debug('work finished')
                        break

                    time.sleep(0.001)  # FIXME: make configurable

            except BaseException as e:

                # This is a very global except, also catches 
                # sys.exit(), keyboard interrupts, etc.  
                # Ignore pylint and PEP-8, we want it this way!
                self._rup_log.exception('abort')


            try:
                # note that we always try to call the finalizers, even if an
                # exception got raised during initialization or in the work loop
                # initializers failed for some reason or the other...
                self._rup_finalize()

            except BaseException as e:
                self._rup_log.exception('finalization error')

            self._rup_log.exception('terminating')

            # all is done and said - begone!
            return


    # --------------------------------------------------------------------------
    #
    def wait(self):

        if  self.is_alive():
            self.join()


    # --------------------------------------------------------------------------
    #
    def get_state(self):
        return self._state 

    state = property(get_state)


    # --------------------------------------------------------------------------
    #
    def get_result(self):
        '''
        Changing this method to 

            self.join()
            return self._result

        would effectively turn this Thread class into a Future.  As it is, it is
        implemented as a somewhat bleak future, from which you can't expect
        much...
        '''

        if  self._state == DONE:
            return self._result

        return None

    result = property(get_result)


    # --------------------------------------------------------------------------
    #
    def get_exception(self):

        return self._exception

    exception = property(get_exception)


    # --------------------------------------------------------------------------
    #
    def get_traceback(self):

        return self._traceback

    traceback = property(get_traceback)


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):
        
        self._term.set()
        self.join(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):
        
        # this currently only exists to make the thread watchable by ru.Process
        # watcher
        super(Thread, self).join(self, timeout=timeout)



# ------------------------------------------------------------------------------
#
def is_main_thread():

    return isinstance(threading.current_thread(), threading._MainThread)


# ------------------------------------------------------------------------------
#
_signal_lock = threading.Lock()
_signal_sent = dict()
def cancel_main_thread(signame=None, once=False):
    """
    This method will call thread.interrupt_main from any calling subthread.
    That will cause a 'KeyboardInterrupt' exception in the main thread.  This
    can be excepted via `except KeyboardInterrupt`
    
    The main thread MUST NOT have a SIGINT signal handler installed (other than
    the default handler or SIGIGN), otherwise this call will cause an exception
    in the core python signal handling (see http://bugs.python.org/issue23395).

    The thread will exit after this, via sys.exit(0), and can then be joined
    from the main thread.

    When being called *from* the main thread, no interrupt will be generated,
    but sys.exit() will still be called.  This can be excepted in the code via 
    `except SystemExit:`.

    Another way to avoid the SIGINT problem is to send a different signal to the
    main thread.  We do so if `signal` is specified.

    After sending the signal, any sub-thread will call sys.exit(), and thus
    finish.  We leave it to the main thread thogh to decide if it will exit at
    this point or not.  Either way, it will have to handle the signal first.

    If `once` is set to `True`, we will send the given signal at most once.
    This will mitigate races between multiple error causes, specifically during
    finalization.
    """

    global _signal_lock
    global _signal_sent

    if signame: signal = get_signal_by_name(signame)
    else      : signal = None

    with _signal_lock:

        if once:
            if signal in _signal_sent:
                # don't signal again
                return

        if signal:
            # send the given signal to the process to which this thread belongs
            os.kill(os.getpid(), signal)
        else:
            # this sends a SIGINT, resulting in a KeyboardInterrupt.
            # NOTE: see http://bugs.python.org/issue23395 for problems on using
            #       SIGINT in combination with signal handlers!
            thread.interrupt_main()

        # record the signal sending
        _signal_sent[signal] = True

    # the sub thread will at this point also exit.
    if not is_main_thread():
        sys.exit()


# ------------------------------------------------------------------------------
#
def get_signal_by_name(signame):
    """
    Translate a signal name into the respective signal number.  If `signame` is
    not found to be a valid signal name, this method will raise a `KeyError`
    exception.  Lookup is case insensitive.
    """

    table = {'abrt'    : signal.SIGABRT,
             'alrm'    : signal.SIGALRM,
             'bus'     : signal.SIGBUS,
             'chld'    : signal.SIGCHLD,
             'cld'     : signal.SIGCLD,
             'cont'    : signal.SIGCONT,
             'fpe'     : signal.SIGFPE,
             'hup'     : signal.SIGHUP,
             'ill'     : signal.SIGILL,
             'int'     : signal.SIGINT,
             'io'      : signal.SIGIO,
             'iot'     : signal.SIGIOT,
             'kill'    : signal.SIGKILL,
             'pipe'    : signal.SIGPIPE,
             'poll'    : signal.SIGPOLL,
             'prof'    : signal.SIGPROF,
             'pwr'     : signal.SIGPWR,
             'quit'    : signal.SIGQUIT,
             'rtmax'   : signal.SIGRTMAX,
             'rtmin'   : signal.SIGRTMIN,
             'segv'    : signal.SIGSEGV,
             'stop'    : signal.SIGSTOP,
             'sys'     : signal.SIGSYS,
             'term'    : signal.SIGTERM,
             'trap'    : signal.SIGTRAP,
             'tstp'    : signal.SIGTSTP,
             'ttin'    : signal.SIGTTIN,
             'ttou'    : signal.SIGTTOU,
             'urg'     : signal.SIGURG,
             'usr1'    : signal.SIGUSR1,
             'usr2'    : signal.SIGUSR2,
             'vtalrm'  : signal.SIGVTALRM,
             'winch'   : signal.SIGWINCH,
             'xcpu'    : signal.SIGXCPU,
             'xfsz'    : signal.SIGXFSZ,
             }
    
    return table[signame.lower()]


# ------------------------------------------------------------------------------
#
class ThreadExit(SystemExit):
    pass

class SignalRaised(SystemExit):

    def __init__(self, msg, signum=None):
        if signum:
            msg = '%s [signal: %s]' % (msg, signum)
        SystemExit.__init__(self, msg)


# ------------------------------------------------------------------------------
#
def get_thread_name():

    return threading.current_thread().name


# ------------------------------------------------------------------------------
#
def get_thread_id():

    return threading.current_thread().ident


# ------------------------------------------------------------------------------
#
def raise_in_thread(e=None, tname=None, tident=None):
    """
    This method uses an internal Python function to inject an exception 'e' 
    into any given thread.  That thread can be specified by its name ('tname')
    or thread id ('tid').  If not specified, the exception is sent to the
    MainThread.

    The target thread will receive the exception with some delay.  More
    specifically, it needs to call up to 100 op codes before the exception 
    is evaluated and raised.

    The default exception raised is 'radical.utils.ThreadExit' which inherits
    from 'SystemExit'.

    NOTE: this is not reliable: the exception is not raised immediately, but is
          *scheduled* for raising at some point, ie. in general after about 100
          opcodes (`sys.getcheckinterval()`).  Depending on when exactly the 
          exception is finally raised, the interpreter might silently swallow
          it, if that happens in a generic try/except clause.  Those exist in
          the Python core, even if discouraged by some PEP or the other.

          See https://bugs.python.org/issue1779233


    NOTE: we can only raise exception *types*, not exception *instances*

          See https://bugs.python.org/issue1538556
    """

    if not tident:
        if not tname:
            tname = 'MainThread'

        for th in threading.enumerate():
            if tname  == th.name:
                tident = th.ident
                break

    if not tident:
        raise ValueError('no target thread given/found')

    if not e:
        e = ThreadExit

    self_thread = threading.current_thread()
    if self_thread.ident == tident:
        # if we are in the target thread, we simply raise the exception.  
        # This specifically also applies to the main thread.
        # Alas, we don't have a decent message to use...
        raise e('raise_in_thread')

    else:
        import ctypes
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tident),
                                                   ctypes.py_object(e))


# ------------------------------------------------------------------------------
#
def fs_event_create(fname, msg=None):
    """
    Atomically create a file at the given `fname` (relative to pwd),
    with `msg` as content (or empty otherwise).

    NOTE: this expects a POSIX compliant file system to be accessible by the two
          entities which use this fs event mechanism.
    NOTE: this also assumes `os.rename()` to be an atomic operation.
    """

    pid = os.getpid()

    if not msg:
        msg = ''

    with open('%s.%d.in' % (fname, pid), 'w') as f:
        f.write(msg)

    os.rename('%s.%d.in' % (fname, pid), fname)


# ------------------------------------------------------------------------------
#
def fs_event_wait(fname, timeout=None):
    """
    Wait for a file ate the given `fname` to appear.  Return `None` at timeout,
    or the file content otherwise.
    """

    msg   = None
    pid   = os.getpid()
    start = time.time()

    while True:

        try:
            with open(fname, 'r') as f:
                msg = f.read()
        except Exception as e:
            print 'e: %s' % type(e)
            print 'e: %s' % e
            pass

        if msg != None:
            try:
                os.rename(fname, '%s.%d.out' % (fname, pid))
                os.unlink('%s.%d.out' % (fname, pid))
            except Exception as e:
                # there is not much we can do at this point...
                print 'unlink: %s' % e
                pass
            return msg

        if timeout and start + timeout <= time.time():
            return None

        time.sleep(0.1)


# ------------------------------------------------------------------------------

