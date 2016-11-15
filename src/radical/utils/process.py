
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import ctypes
import multiprocessing as mp

_ALIVE_TIMEOUT = 5.0  # time to wait for process startup signal
_TERM_TIMEOUT  = 5.0  # time to wait for voluntary process termination


# ------------------------------------------------------------------------------
#
# This Process class is a thin wrapper around multiprocessing.Process which
# specifically manages the process lifetime in a more cautious and copperative
# way than the base class:  
#
#  - self._rup_alive evaluates True after the child bootstrapped successfully
#  - self._rup_term  signals child to terminate voluntarily before being killed
#  - self.work()     is the childs main loop (must be overloaded), and is called 
#                    as long as self._term is not set
#  - *no* attempt on signal handling is made, we expect to exclusively
#    communicate via the above events.
#
# NOTE: At this point we do not implement the full mp.Process constructor.
#
# Semantics:
#
#   def start(timeout): mp.Process.start()
#                       self._alive.wait(timeout)
#   def run(self):      # must NOT be overloaded
#                         self.initialize() # overload
#                         self._alive.set()
#
#                         while True:
#                           if self._rup_term.is_set():
#                             self._rup_terminfo = 'terminated'
#                             break
#
#                           if not parent.is_alive():
#                             self._rup_terminfo = 'orphaned'
#                             break
#
#                           try:
#                               self.work()
#                           except:
#                             self._rup_terminfo = 'exception %s'
#                             break
#
#                         self.finalize()  # overload
#                         sys.exit()
#
#   def stop():         self._rup_term.set()
#   def terminate():    mp.Process.terminate()
#   def join(timeout):  mp.Process.join(timeout)
#
# 
class Process(mp.Process):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log=None):

        self._rup_alive    = mp.Event()   # set after child process startup,
                                          # unset after demise
        self._rup_term     = mp.Event()   # set in stop() to terminate child
        self._rup_terminfo = mp.Value(ctypes.c_char_p, '', lock=True) 
                                          # report on termination causes
        self._rup_log      = log          # ru.logger for debug output
        self._rup_ppid     = os.getpid()  # get parent process ID

        mp.Process.__init__(self)


    # --------------------------------------------------------------------------
    #
    def is_alive(self):
        '''
        Overload the `mp.Process.is_alive()` method, and check both the job
        state *and* the state of the `self._rup_alive` flag.
        '''

        if  self._rup_log:
            self._rup_log.debug('is_alive flag/proc: %s/%s', 
                                self._rup_alive.is_set(), mp.Process.is_alive(self))

        if not self._rup_alive.is_set():
            return False

        return mp.Process.is_alive(self)


    # --------------------------------------------------------------------------
    #
    def start(self, timeout=None):
        '''
        Overload the `mp.Process.start()` method, and block (with timeout) until
        the child signals to be alive via `self._rup_alive`.
        '''

        if  self._rup_log:
            self._rup_log.debug('start process')

        if not timeout:
            timeout = _ALIVE_TIMEOUT

        # start `self.run()` in the child process, and wait for it's
        # initalization to finish, which will set `self._rup_alive`.
        mp.Process.start(self)
        self._rup_alive.wait(timeout)

        # if that did't work out, we consider te child failed.
        if not self.is_alive():

            # startup failed.  Terminate whatever is left and raise
            if  self._rup_log:
                self._rup_log.debug('start process failed', self._rup_terminfo)

            try:
                # child is likely dead - but we'll make sure
                self.terminate()  # hard kill
                self.join()       # collect process
            except:
                pass
            raise RuntimeError('child startup failed [%s]' % self._rup_terminfo)

        if  self._rup_log:
            self._rup_log.debug('start process ok')


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This method can be overloaded, and will then be executed *once* before
        the class' `work()` method.  The child is only considered to be 'alive'
        after this method has been completed without error.
        '''

        if  self._rup_log:
            self._rup_log.debug('initialized (NOOP)')


    # --------------------------------------------------------------------------
    #
    def work(self):
        '''
        This method MUST be overloaded.  It represents the workload of the
        process, and will be called over and over again.  

        This has several implications:

          * `work()` needs to enforce any call rate limits on its own!
          * in order to terminate the child, `work()` needs to either raise an
            exception, or call `sys.exit()` (which actually also raises an
            exception).

        Before the first invocation, `self.initialize()` will be called.  After
        the last invocation, `self.finalize()` will be called, if possible.  The
        latter will not always be possible if the child is terminated by
        a signal, such as when the parent process calls `child.terminate()` --
        `child.stop()` should be used instead.
        '''
        raise NotImplementedError('ru.Process.work() MUST be overloaded')


    # --------------------------------------------------------------------------
    #
    def finalize(self):
        '''
        This method can be overloaded, and will then be executed *once* after
        the class' `work()` method.  The child is considered 'alive' until this
        method has been completed.

        note that `self.finalize()` will not be called when `self.initalize()`
        failed!
        '''

        if  self._rup_log:
            self._rup_log.debug('finalize (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _parent_is_alive(self):
        '''
        This private method checks if the parent process is still alive.  This
        obviously only makes sense when being called in the child process.

        Note that there is a (however unlikely) race: PIDs are reused, and the
        process could be replaced by another process with the same PID inbetween
        tests.  We thus also except *all* exception, including permission
        errors, to capture at least some of those races.
        '''

        try:
            os.kill(self._rup_ppid, 0)
            return True

        except:
            return False


    # --------------------------------------------------------------------------
    #
    def run(self):
       '''
       This method MUST NOT be overloaded!

       This is the workload of the child process.  It will first call
       `self.initialize()`, and then repeatedly call `self.work()`, until being
       terminated.  When terminated, it will call `self.finalize()` and exit.

       The implementation of `work()` needs to make sure that this process is
       not spinning idly -- if there is nothing to do in `work()` at any point
       in time, the routine should at least sleep for a fraction of a second or
       something.

       `finalize()` is only guaranteed to get executed on `self.stop()` --
       a hard kill via `self.terminate()` may or may not be trigger to run
       `self.finalize()`.

       The child process will automatically terminate when the parent process
       dies (then including the call to `self.finalize()`).  It is not possible
       to create daemon or orphaned processes -- which is an explicit purpose of
       this implementation.
       '''

       self.initialize()     # overloaded
       self._rup_alive.set() # signal successful child startup
 
       # enter loop to repeatedly call 'work()'.
       while True:

           if self._rup_term.is_set():
               self._rup_terminfo = 'terminated'
               break
 
           if not self._parent_is_alive():
               self._rup_terminfo = 'orphaned'
               break
 
           try:
               self.work()
           except BaseException as e:
               # this is a very global except, and also catches sys.exit(),
               # keyboard interrupts, etc.  Ignore pylint and PEP-8, we want 
               # it this way!
               if  self._rup_log:
                   self._rup_log.exception('work() raised exception: %s', e)
               self._rup_terminfo = 'exception %s' % repr(e)
               break
 
       if  self._rup_log:
           self._rup_log.info('child is terminating: %s', self._rup_terminfo)

       self.finalize()  # overloaded
       sys.exit()       # terminate child process


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''
        signal termination via `self._rup_term.set()`
        '''

        if not self.is_alive():
            # nothing to do
            if  self._rup_log:
                self._rup_log.debug('nothing to stop')
            return

        if  self._rup_log:
            self._rup_log.debug('stop child')

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def terminate(self):
        '''
        Hard process termination via a kill signal.
        '''

        # we don't check `self.is_alive()` -- we *always* send the kill signal

        if  self._rup_log:
            self._rup_log.debug('kill child')

        return mp.Process.terminate(self)


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):
        '''
        Collect the child process.
        '''

        if  self._rup_log:
            self._rup_log.debug('join child')

        return mp.Process.join(self, timeout)


# ------------------------------------------------------------------------------

