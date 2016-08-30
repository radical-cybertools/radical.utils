
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import signal
import thread
import threading
import traceback

import misc  as rumisc


_out_lock = threading.RLock()


# ------------------------------------------------------------------------------
#
NEW     = 'New'
RUNNING = 'Running'
DONE    = 'Done'
FAILED  = 'Failed'


# ------------------------------------------------------------------------------
#
def lout(txt, stream=sys.stdout):

    with _out_lock:
        stream.write(txt)
        stream.flush()


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
         is-it-possible-to-subclass-lock-objects-in-python-if-not-other-ways-to-debug
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, obj=None):

        self._lock = threading.RLock()

      # with self._lock:
      #     self._obj = obj
      #     self._cnt = 0


    # --------------------------------------------------------------------------
    #
    def acquire(self):

      # ind = (self._cnt)*' '+'>'+(30-self._cnt)*' '
      # lout("%s -- %-10s %50s acquire  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        self._lock.acquire()

      # self._cnt += 1
      # ind = (self._cnt)*' '+'|'+(30-self._cnt)*' '
      # lout("%s    %-10s %50s acquired - %s\n" % (ind, threading.current_thread().name, self, self._lock))


    # --------------------------------------------------------------------------
    #
    def release(self):

      # ind = (self._cnt)*' '+'-'+(30-self._cnt)*' '
      # lout("%s    %-10s %50s release  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        try:
            self._lock.release()
        except RuntimeError as e:
            # lock has been released meanwhile - we allow that
          # print 'ignore double lock release'
            pass

      # self._cnt -= 1
      # ind = (self._cnt)*' '+'<'+(30-self._cnt)*' '
      # lout("%s -- %-10s %50s released - %s\n" % (ind, threading.current_thread().name, self, self._lock))


    # --------------------------------------------------------------------------
    #
    def __enter__(self)                        : self.acquire() 
    def __exit__ (self, type, value, traceback): self.release()


# ------------------------------------------------------------------------------
#
class Thread(threading.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, call, *args, **kwargs):

        if not callable(call):
            raise ValueError("Thread requires a callable to function, not %s" \
                            % (str(call)))

        threading.Thread.__init__(self)

        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._result    = None
        self._exception = None
        self._traceback = None
        self.daemon     = True


    # --------------------------------------------------------------------------
    #
    @classmethod
    def Run(self, call, *args, **kwargs):

        t = self(call, *args, **kwargs)
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

        try:
            self._state     = RUNNING
            self._result    = self._call(*self._args, **self._kwargs)
            self._state     = DONE

        except Exception as e:
            tb = traceback.format_exc()
            self._traceback = tb
            self._exception = e
            self._state     = FAILED


    # --------------------------------------------------------------------------
    #
    def wait(self):

        if  self.isAlive():
            self.join()


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        # FIXME: this is not really implementable generically, so we ignore 
        # cancel requests for now.
        pass


    # --------------------------------------------------------------------------
    #
    def get_state(self):
        return self._state 

    state = property(get_state)


    # --------------------------------------------------------------------------
    #
    def get_result(self):

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

    import ctypes
    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tident),
                                               ctypes.py_object(e))


# ------------------------------------------------------------------------------

