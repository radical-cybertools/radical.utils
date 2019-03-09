
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import signal
import thread
import traceback

import Queue        as queue
import threading    as mt
import setproctitle as spt

from .logger  import Logger


# ------------------------------------------------------------------------------
#
_ALIVE_MSG     = 'alive'    # message to use as alive signal
_START_TIMEOUT = 20.0       # time to wait for thread's startup signal.
                            # startup signal: 'alive' message
                            # is sent in both directions to ensure correct setup
_STOP_TIMEOUT  = 3.0        # time between temination signal and killing child
_BUFSIZE       = 1024 * 10  # default buffer size for socket recvs


# ------------------------------------------------------------------------------
#
class Thread(mt.Thread):
    '''
    This `Thread` class is a thin wrapper around `mt.Thread` which specifically
    manages the thread lifetime in a cautious and copperative way: *no* attempt
    on signal handling is made, no exceptions and interrupts are used, we expect
    to exclusively communicate between parent and child via a `mt.Events` and
    `Queue.Queue` instances.

    NOTE: At this point we do not implement the full `mt.Thread` constructor.

    The class also implements a number of initialization and finalization
    methods which can be overloaded by any deriving class.  While this can at
    first be a confusing richness of methods to overload, it significantly
    simplifies the implementation of non-trivial child threads.  By default,
    none of the initialized and finalizers needs to be overloaded.

    An important semantic difference are the `start()` and `stop()` methods:
    both accept an optional `timeout` parameter, and both guarantee that the
    child threads successfully started and completed upon return, respectively.
    If that does not happen within the given timeout, an exception is raised.
    Not that the child startup is *only* considered complete once all of its
    initialization methods have been completed -- the start timeout value must
    thus be chosen very carefully.  Note further that the *parent*
    initialization methods are also part of the `start()` invocation, and must
    also be considered when choosing the timeout value.  Parent initializers
    will only be executed once the child is known to be alive, and the parent
    can thus expect the child bootstrapping to be completed (avoiding the need
    for additional handshakes etc).  Any error in child or parent initialization
    will result in an exception, and the child will be terminated.

    Along the same lines, the parent and child finalizers are executed in the
    `stop()` method, prompting similar considerations for the `timeout` value.

    Any class which derives from this Thread class must overload the 'work_cb()`
    method.  That method is repeatedly called by the child thread's main loop,
    until:

      - an exception occurs (causing the child to fail with an error)
      - `False` is returned by `work_cb()` (child finishes w/o error)
      - the parent thread finishes
    '''

    # FIXME: assert that start() was called for some / most methods

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, target=None, log=None):

        # At this point, we only initialize members which we need before start
        self._ru_log = log  # ru.logger for debug output

        if not name:
            name = 'noname'

        # most importantly, we create an `mt.Event` and an `mt.Queue`.  Both
        # parent and child will watch the event, which thus acts as a lifeline
        # between the threads, to detect abnormal termination in threads.
        # The queue endpoint is used to send messages back and forth.
        self._ru_term     = mt.Event()
        self._ru_endpoint = queue.Queue()

        # NOTE: threadlocal storage *must* be defined on module level, and
        #       cannot be instance specific
        self._ru_local    = mt.local()   # keep some vars thread-local

        # note that some members are only really initialized when calling the
        # `start()`/`run()` methods, and the various initializers.
        self._ru_name              = name   # communicate name to thread
        self._ru_local.name        = name   # use for logfiles etc
        self._ru_local.started     = False  # start() has been called
        self._ru_local.spawned     = None   # set in start(), run()
        self._ru_local.initialized = False  # set to signal bootstrap success
        self._ru_local.terminating = False  # set to signal active termination
        self._ru_local.is_parent   = None   # set in start()
        self._ru_local.is_child    = None   # set in run()

        if not self._ru_log:
            # if no logger is passed down, log to null
            self._ru_log = Logger('radical.util.threads')

        self._ru_log.debug('parent name: %s' % self._ru_local.name)

        if target:
            # we don't support `arguments`, yet
            self.work_cb = target

        # when cprofile is requested but not available, 
        # we complain, but continue unprofiled
        self._ru_cprofile = False
        if self._ru_name in os.environ.get('RADICAL_CPROFILE', '').split(':'):
            try:
                self._ru_log.error('enable cprofile for %s', self._ru_local.name)
                import cprofile
                self._ru_cprofile = True
            except:
                self._ru_log.error('cannot import cprofile - disable')

        # base class initialization
        super(Thread, self).__init__(name=self._ru_local.name)


    # --------------------------------------------------------------------------
    #
    def _ru_msg_send(self, msg):
        '''
        send new message to self._ru_endpoint.  We make sure that the
        message is not larger than the _BUFSIZE define in the RU source code.
        '''

        if not self._ru_local.spawned:
            # no child, no communication channel
            raise RuntimeError("can't communicate w/o child")

        self._ru_log.info('put message: [%s] %s', self._ru_local.name, msg)
        self._ru_endpoint.put('%s' % msg)


    # --------------------------------------------------------------------------
    #
    def _ru_msg_recv(self, timeout=None):
        '''
        receive a message from self._ru_endpoint.

        This call is non-blocking: if no message is available, return an empty 
        string.
        '''

        if not self._ru_local.spawned:
            # no child, no communication channel
            raise RuntimeError("can't communicate w/o child")

        try:
            if timeout:
                msg = self._ru_endpoint.get(block=True, timeout=timeout)
            else:
                msg = self._ru_endpoint.get(block=False)
            self._ru_log.info('get msg: %s', msg)
            return msg

        except queue.Empty:
            self._ru_log.warn('get msg timed out')
            return ''


    # --------------------------------------------------------------------------
    #
    def is_alive(self, strict=True):
        '''
        Since our threads are daemon threads we don't need to wait for them to
        actually die, but consider it sufficient to have the terminationm signal
        set.  However, that can potentially cause unexpected results when locks
        are used, as one would not expect to have locks hold by threads which
        are not alive.  For that reason we default to the 'true' (strict) alive
        notion of the native Python thread - but the non-strict version can be
        requates via `strict=False`.
        '''

        alive  = super(Thread, self).is_alive() 
        termed = self._ru_term.is_set()

        if strict:
            return alive
        else:
            return alive and not termed


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):
        '''
        This method provides a self-check, and will call `stop()` if that check
        fails and `term` is set.  If `term` is not set, the return value of
        `is_alive()` is passed through.

        The check will basically assert that `is_alive()` is `True`.   The
        purpose is to call this check frequently, e.g. at the begin of each
        method invocation and during longer loops, as to catch failing
        sub-thread and to then terminate.

        This method will always return True if `start()` was not called, yet.
        '''

        if not self._ru_local.initialized:
            return True

        alive = self.is_alive(strict=False)

        if not alive and term:
            self._ru_log.warn('alive check failed, stop [%s - %s]', alive, term)
            self.stop()
        else:
            return alive


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True, timeout=None):
        '''
        Overload the `mt.Thread.start()` method, and block (with timeout) until
        the child signals to be alive via a message over our queue.  Also
        run all initializers.

        If spawn is set to `False`, then no child thread is actually created,
        but the parent initializers will be executed nonetheless.

        '''

        self._ru_log.debug('start thread')

        if timeout is None:
            timeout = _START_TIMEOUT

        if spawn:
            # start `self.run()` in the child thread, and wait for it's
            # initalization to finish, which will send the 'alive' message.
            super(Thread, self).start()
            self._ru_local.spawned = True

        # this is the parent now - set role flags.
        self._ru_local.is_parent = True
        self._ru_local.is_child  = False

        # from now on we need to invoke `self.stop()` for clean termination.
        try: 

            if self._ru_local.spawned:
                # we expect an alive message message from the child, within
                # timeout
                #
                # NOTE: If the child does not bootstrap fast enough, the timeout
                #       will kick in, and the child will be considered dead,
                #       failed and/or hung, and will be terminated!  Timeout can
                #       be set as parameter to the `start()` method.
                msg = self._ru_msg_recv(timeout=timeout)

                if msg != _ALIVE_MSG:
                    raise RuntimeError('Unexpected child message from %s (%s)'
                                      % (self._ru_local.name, msg))


            # When we got the alive messages, only then will we call the parent
            # initializers.  This way those initializers can make some
            # assumptions about successful child startup.
            self._ru_initialize()

        except Exception:
            self._ru_log.exception('%s init failed', self._ru_local.name)
            self.stop()
            raise

        # if we got this far, then all is well, we are done.
        self._ru_log.debug('child thread %s started', self._ru_local.name)

        # child is alive and initialized, parent is initialized - Wohoo!


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This method MUST NOT be overloaded!

        This is the workload of the child thread.  It will first call the child
        initializers and then repeatedly call `self.work_cb()`, until being
        terminated.  When terminated, it will call the child finalizers and
        exit.  Note that the child will also terminate once `work_cb()` returns
        `False`.

        The implementation of `work_cb()` needs to make sure that this thread is
        not spinning idly -- if there is nothing to do in `work_cb()` at any
        point in time, the routine should at least sleep for a fraction of
        a second or something.

        The child thread will automatically terminate (incl. finalizer calls)
        when the parent thread dies. It is thus not possible to create orphaned
        threads -- which is an explicit purpose of this implementation.  
        '''

        # set local data
        self._ru_local.name = self._ru_name + '.thread'
        self._ru_log.debug('child name: %s' % self._ru_local.name)

        self._ru_local.started     = True   # start() has been called
        self._ru_local.spawned     = True   # set in start(), run()
        self._ru_local.initialized = False  # set to signal bootstrap success
        self._ru_local.terminating = False  # set to signal active termination
        self._ru_local.is_parent   = False  # set in start()
        self._ru_local.is_child    = True   # set in run()

        # if no profiling is wanted, we just run the workload and exit
        if not self._ru_cprofile:
            self._run()

        # otherwise we run under the profiler, obviously
        else:
            import cprofile
            cprofiler = cprofile.Profile()
            cprofiler.runcall(self._run)
            cprofiler.dump_stats('%s.cprof' % (self._ru_local.name))


    # --------------------------------------------------------------------------
    #
    def _run(self):

        # FIXME: ensure that this is not overloaded
        # TODO:  how?

      # _main_thread = main_thread()

        try:
            # we consider the invocation of the child initializers to be part of
            # the bootstrap process, which includes starting the watcher thread
            # to watch the parent's health (via the socket healt).
            try:
                self._ru_initialize()

            except BaseException as e:
                self._ru_log.exception('abort')
                self._ru_msg_send(repr(e))
                sys.stderr.write('initialization error in %s: %s\n'
                        % (self._ru_local.name, repr(e)))
                sys.stderr.flush()
                self._ru_term.set()

            # initialization done - we only now send the alive signal, so the
            # parent can make some assumptions about the child's state
            if not self._ru_term.is_set():
                self._ru_log.info('send alive')
                self._ru_msg_send(_ALIVE_MSG)

            # enter the main loop and repeatedly call 'work_cb()'.  
            #
            # If `work_cb()` ever returns `False`, we break out of the loop to
            # call the finalizers and terminate.
            #
            # In each iteration, we also check if the Event is set -- if this is
            # the case, we assume the parent to be dead and terminate (break the
            # loop).
            while not self._ru_term.is_set():

              # _main_thread.join(0)
              # if not _main_thread.is_alive():
              #     # parent thread is gone - finish also
              #     break

                # des Pudel's Kern
                if not self.work_cb():
                    self._ru_msg_send('work finished')
                    break

        except BaseException as e:

            # This is a very global except, also catches 
            # sys.exit(), keyboard interrupts, etc.  
            # Ignore pylint and PEP-8, we want it this way!
            self._ru_log.exception('abort: %s', repr(e))
            self._ru_msg_send(repr(e))

        try:
            # note that we always try to call the finalizers, even if an
            # exception got raised during initialization or in the work loop
            # initializers failed for some reason or the other...
            self._ru_finalize()

        except BaseException as e:
            self._ru_log.exception('finalization error')
            self._ru_msg_send('finalize(): %s' % repr(e))

        self._ru_msg_send('terminating')

        # all is done and said - begone!
        return


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):
        '''
        `stop()` is symetric to `start()`, in that it can only be called by the
        parent thread


        NOTE: `stop()` implies `join()`!  Use `terminate()` if that is not
              wanted.
        '''

        # FIXME: This method should reduce to 
        #           self.terminate(timeout)
        #           self.join(timeout)
        #        ie., we should move some parts to `terminate()`.

        if not hasattr(self._ru_local, 'name'):

            # This thread is now being stopped from *another* thread, ie.
            # neither the parent nor the child thread, so we can't access either
            # thread local storage.  we thus only set the termination signal
            # (which is accessible), and leave all other handling to somebody
            # else.
            #
            # Specifically, we will not be able to join this thread, and no
            # timeout is enforced
            self._ru_log.info('signal stop for %s', get_thread_name())
            self._ru_term.set()
            return

        if not timeout:
            timeout = _STOP_TIMEOUT

        # if stop() is called from the child, we just set term and leave all
        # other handling to the parent.
        if self._ru_local.is_child:
            self._ru_log.info('child calls stop()')
            self._ru_term.set()
          # cancel_main_thread()
            return

        self._ru_log.info('parent stops child')

        # make sure we don't recurse
        if self._ru_local.terminating:
            return
        self._ru_local.terminating = True

        # call finalizers - this sets `self._ru_term`
        self._ru_finalize()

        # After setting the termination event, the child should begin
        # termination immediately.  Well, whenever it realizes the event is set,
        # really.  We wait for that termination to complete.
        self.join()


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):

      # raise RuntimeError('call stop instead!')
      #
      # we can't really raise the exception above, as that would break symmetry
      # with the Process class -- see documentation there.
      #
      # FIXME: not that `join()` w/o `stop()` will not call the parent 
      #        finalizers.  We should call those in both cases, but only once.

        if not hasattr(self._ru_local, 'name'):

            # This thread is now being stopped from *another* thread, ie.
            # neither the parent nor the child thread, so we can't access either
            # thread local storage.  we thus only set the termination signal
            # (which is accessible), and leave all other handling to somebody
            # else.
            #
            # Specifically, we will not be able to join this thread, and no
            # timeout is enforced
            self._ru_log.info('signal stop for %s', get_thread_name())
            self._ru_term.set()
            return

        if not timeout:
            timeout = _STOP_TIMEOUT

        if self._ru_local.is_parent:
            try:
                super(Thread, self).join(timeout=timeout)
            except Exception as e:
                self._ru_log.warn('ignoring %s' % e)


    # --------------------------------------------------------------------------
    #
    def _ru_initialize(self):
        '''
        Perform basic settings, then call common and parent/child initializers.
        '''

        try:
            # call parent and child initializers, respectively
            if self._ru_local.is_parent:
                self._ru_initialize_common()
                self._ru_initialize_parent()

                self.ru_initialize_common()
                self.ru_initialize_parent()

            elif self._ru_local.is_child:
                self._ru_initialize_common()
                self._ru_initialize_child()

                self.ru_initialize_common()
                self.ru_initialize_child()

            self._ru_local.initialized = True

        except Exception as e:
            self._ru_log.exception('initialization error')
            raise RuntimeError('initialize: %s' % repr(e))


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_common(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_parent(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_child(self):

        # TODO: should we also get an alive from parent?
        #
        # FIXME: move to _ru_initialize_common
        #

        self._ru_log.info('child (me) initializing')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_common(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, for both the parent and the child thread (individually).  If
        this fails on either side, the thread startup is considered failed.
        '''

        self._ru_log.debug('ru_initialize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the parent thread.  If this fails, the thread startup is
        considered failed.
        '''

        self._ru_log.debug('ru_initialize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the child thread.  If this fails, the thread startup is
        considered failed.
        '''

        self._ru_log.debug('ru_initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _ru_finalize(self):
        '''
        Call common and parent/child initializers.  

        Note that finalizers are called in inverse order of initializers.
        '''

        try:
            # signal termination
            self._ru_term.set()

            # call parent and child finalizers, respectively
            if self._ru_local.is_parent:
                self.ru_finalize_parent()
                self.ru_finalize_common()

                self._ru_finalize_parent()
                self._ru_finalize_common()

            elif self._ru_local.is_child:
                self.ru_finalize_child()
                self.ru_finalize_common()

                self._ru_finalize_child()
                self._ru_finalize_common()

        except Exception as e:
            self._ru_log.exception('finalization error')
            raise RuntimeError('finalize: %s' % repr(e))


    # --------------------------------------------------------------------------
    #
    def _ru_finalize_common(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _ru_finalize_parent(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _ru_finalize_child(self):

        pass


    # --------------------------------------------------------------------------
    #
    def ru_finalize_common(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or thread child termination, in the parent thread, in both
        the parent and the child thread (individually).
        '''

        self._ru_log.debug('ru_finalize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_finalize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or thread child termination, in the parent thread.
        '''

        self._ru_log.debug('ru_finalize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_finalize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or thread child termination, in the child thread.
        '''

        self._ru_log.debug('ru_finalize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def work_cb(self):
        '''
        This method MUST be overloaded.  It represents the workload of the
        thread, and will be called over and over again.

        This has several implications:

          * `work_cb()` needs to enforce any call rate limits on its own!
          * in order to terminate the child, `work_cb()` needs to either raise
            an exception, or call `sys.exit()` (which actually also raises an
            exception).

        Before the first invocation, `self.ru_initialize_child()` will be
        called.  After the last invocation, `self.ru_finalize_child()` will be
        called, if possible.  The latter will not always be possible if the
        child is terminated by a signal, such as when the parent thread calls
        `child.terminate()` -- `child.stop()` should be used instead.

        The overloaded method MUST return `True` or `False` -- the child will
        continue to work upon `True`, and otherwise (on `False`) begin
        termination.
        '''

        raise NotImplementedError('ru.Thread.work_cb() MUST be overloaded')


# ------------------------------------------------------------------------------
#
# thread-related utility classes and methods
#
class RLock(object):
    """
    This mt.RLock wrapper is supportive of lock debugging.  The only
    semantic difference to mt.RLock is that a lock acquired via the
    'with' statement can be released within the 'with' scope, w/o penalty when
    leaving the locked scope.  This supports up-calling callback semantics, but
    should be used with utter care, and rarely (such as on close()).

    see http://stackoverflow.com/questions/6780613/
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, obj=None):

        self._lock = mt.RLock()


    # --------------------------------------------------------------------------
    #
    def acquire(self):

        self._lock.acquire()


    # --------------------------------------------------------------------------
    #
    def release(self):

        try:
            self._lock.release()

        except RuntimeError:
            # lock has been released meanwhile - we allow that
            pass


    # --------------------------------------------------------------------------
    #
    def __enter__(self)                        : self.acquire() 
    def __exit__ (self, type, value, traceback): self.release()


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
        import ctypes
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

    return isinstance(t, mt._MainThread)


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
        if isinstance(t, mt._MainThread):
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
            try:
                thread.interrupt_main()
            except TypeError:
                # this is known to be a side effect of `thread.interrup_main()`
                pass

        # record the signal sending
        _signal_sent[signal] = True

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
    print 'caught sigusr2 (%s)' % os.getpid()
    # we only want to get this exception once, so we unset the signal handler
    # before we raise it
    signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    raise KeyboardInterrupt('sigusr2')


def set_cancellation_handler():

    # check if any handler exists
    old_handler = signal.getsignal(signal.SIGUSR2)
    if  old_handler not in [signal.SIG_DFL, signal.SIG_IGN, None] and \
        old_handler != _sigusr2_handler:
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
        import ctypes
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tident),
                                                   ctypes.py_object(e))


# ------------------------------------------------------------------------------

