
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import signal
import ctypes
import _thread

import threading    as mt


# ------------------------------------------------------------------------------
#
def get_thread_name():

    if not mt.current_thread():
        return None

    return mt.current_thread().name


# ------------------------------------------------------------------------------
#
def get_thread_id():

    return mt.current_thread().ident


# ------------------------------------------------------------------------------
#
def gettid():
    """
    Python is not able to give us the native thread ID.  We thus use a syscall
    to do so.  Since this is not portable, we fall back to None in case of error
    (Hi MacOS).
    """
    try:
        SYS_gettid = 186
        libc = ctypes.cdll.LoadLibrary('libc.so.6')
        return int(libc.syscall(SYS_gettid))
    except:
        return None


# ------------------------------------------------------------------------------
#
def is_main_thread(t=None):

    if t:
        assert(isinstance(t, mt.Thread))
    else:
        t = this_thread()

    return isinstance(t, mt._MainThread)                 # pylint: disable=W0212


# ------------------------------------------------------------------------------
#
def is_this_thread(t):
    '''
    check if the given thread (type: threading.Thread) is the same as the
    current thread of execution.
    '''

    assert(isinstance(t, mt.Thread))

    return(t == this_thread())


# ------------------------------------------------------------------------------
#
def main_thread():
    '''
    return a handle to the main thread of execution in this process
    '''

    for t in mt.enumerate():
        if isinstance(t, mt._MainThread):                # pylint: disable=W0212
            return t

    assert(False), 'main thread not found'


# ------------------------------------------------------------------------------
#
def this_thread():
    '''
    return a handle to the current thread of execution
    '''

    return mt.current_thread()


# ------------------------------------------------------------------------------
#
_signal_lock = mt.Lock()
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
    `except SystemExit`.

    Another way to avoid the SIGINT problem is to send a different signal to the
    main thread.  We do so if `signal` is specified.

    After sending the signal, any sub-thread will call sys.exit(), and thus
    finish.  We leave it to the main thread though to decide if it will exit at
    this point or not.  Either way, it will have to handle the signal first.

    If `once` is set to `True`, we will send the given signal at most once.
    This will mitigate races between multiple error causes, specifically during
    finalization.
    """

    global _signal_lock                                  # pylint: disable=W0603
    global _signal_sent                                  # pylint: disable=W0603

    if signame: signum = get_signal_by_name(signame)
    else      : signum = None

    with _signal_lock:

        if once:
            if signum in _signal_sent:
                # don't signal again
                return

        if signum:
            # send the given signal to the process to which this thread belongs
            os.kill(os.getpid(), signum)
        else:
            # this sends a SIGINT, resulting in a KeyboardInterrupt.
            # NOTE: see http://bugs.python.org/issue23395 for problems on using
            #       SIGINT in combination with signal handlers!
            try:
                _thread.interrupt_main()
            except TypeError:
                # this is known to be a side effect of `thread.interrup_main()`
                pass

        # record the signal sending
        _signal_sent[signum] = True

    # the sub thread will at this point also exit.
    if not is_main_thread():
        sys.exit()


# ------------------------------------------------------------------------------
# this is the counterpart for the `cancel_main_thread` method above: any main
# thread should call `set_cancellation_handler`, which will set a signal handler
# for `SIGUSR2`, and upon catching it will raise a `KeyboardInterrupt`
# exception, which can be caught by any interested library or application.
#
# RU claims ownership of `SIGUSR2` -- it will complain if any other signal
# handler is installed for that signal.
#
# This method can safely be called multiple times.  This method can be called
# from threads, but it will have no effect then (Python allows signal handlers
# to only be installed in the main thread)
#
# FIXME: `cancel_main_thread()` supports arbitrary signals --
#        `set_cancellation_handler()` should, too.
#
def _sigusr2_handler(signum, frame):
    print('caught sigusr2 (%s)' % os.getpid())
    # we only want to get this exception once, so we unset the signal handler
    # before we raise it
    signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    raise KeyboardInterrupt('sigusr2')


def set_cancellation_handler():

    # check if any handler exists
    old_handler = signal.getsignal(signal.SIGUSR2)
    if old_handler not in [signal.SIG_DFL, signal.SIG_IGN, None] and \
       old_handler != _sigusr2_handler:                 # pylint:  disable=W0143
        raise RuntimeError('handler for SIGUSR2 is already present')

    try:
        signal.signal(signal.SIGUSR2, _sigusr2_handler)
    except ValueError:
        # this fails in subthreads - ignore
        pass


def unset_cancellation_handler():

    try:
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    except ValueError:
        # this fails in subthreads - ignore
        pass


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
def raise_in_thread(e=None, tname=None, tident=None):
    """
    This method uses an internal Python function to inject an exception 'e'
    into any given thread.  That thread can be specified by its name ('tname')
    or thread id ('tid').  If not specified, the exception is sent to the
    MainThread.

    The target thread will receive the exception with some delay.  More
    specifically, it needs to call up to 100 op codes before the exception
    is evaluated and raised.  The thread interruption can thus be delayed
    significantly, like when the thread sleeps.

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

    Example:

        # ----------------------------------------------------------------------
        def sub():
            time.sleep(0.1)
            ru.raise_in_thread()

        try:
            t = mt.Thread(target=sub)
            t.start()

            while True:
                time.sleep(0.01)

        except ru.ThreadExit:  print 'thread exit'
        except Exception as e: print 'except: %s' % e
        except SystemExit:     print 'exit'
        else:                  print 'unexcepted'
        finally:               t.join()
        # ----------------------------------------------------------------------
    """

    if not tident:
        if not tname:
            tname = 'MainThread'

        for th in mt.enumerate():
            if tname  == th.name:
                tident = th.ident
                break

    if not tident:
        raise ValueError('no target thread given/found')

    if not e:
        e = ThreadExit

    self_thread = mt.current_thread()
    if self_thread.ident == tident:
        # if we are in the target thread, we simply raise the exception.
        # This specifically also applies to the main thread.
        # Alas, we don't have a decent message to use...
        raise e('raise_in_thread')

    else:
        # otherwise we inject the exception into the main thread's async
        # exception scheduler
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tident),
                                                   ctypes.py_object(e))


# ------------------------------------------------------------------------------

