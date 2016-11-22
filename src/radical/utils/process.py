
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import ctypes
import select
import socket
import threading       as mt
import multiprocessing as mp

from .debug import print_stacktrace, print_stacktraces

_ALIVE_TIMEOUT = 5.0  # time to wait for process startup signal
_WATCH_TIMEOUT = 0.1  # time between thread and process health polls


def _role(to_watch):
    if   to_watch == 'parent': return 'child'
    elif to_watch == 'child' : return 'parent'
    else                     : return 'unknonwn'


# ------------------------------------------------------------------------------
#
# This Process class is a thin wrapper around multiprocessing.Process which
# specifically manages the process lifetime in a more cautious and copperative
# way than the base class:  
#
#  - self._rup_alive evaluates True after the child bootstrapped successfully
#  - self._rup_term  signals child to terminate voluntarily before being killed
#  - self.work()     is the childs main loop (must be overloaded), and is called 
#                    as long as self._rup_term is not set
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
#                             self._rup_terminfo.value = 'terminated'
#                             break
#
#                           if not parent.is_alive():
#                             self._rup_terminfo.value = 'orphaned'
#                             break
#
#                           try:
#                               self.work()
#                           except:
#                             self._rup_terminfo.value = 'exception %s'
#                             break
#
#                         self.finalize()  # overload
#                         sys.exit()
#
#   def stop():         self._rup_term.set()
#   def terminate():    mp.Process.terminate()
#   def join(timeout):  mp.Process.join(timeout)
#
# TODO: we should switch to fork/*exec*, if possible...
#
# 
class Process(mp.Process):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None, log=None):

        self._rup_alive    = mp.Event()   # set after child process startup,
                                          # unset after demise
        self._rup_term     = mt.Event()   # set in stop() to terminate child
        self._rup_terminfo = mp.Array(ctypes.c_char, 1024*' ', lock=True) 
                                          # report on termination causes
        self._rup_log      = log          # ru.logger for debug output
        self._rup_ppid     = os.getpid()  # get parent process ID
        self._rup_to_watch = list()       # threads and processes to watch
        self._rup_lock     = mt.RLock()   # thread lock

        self._tmp = False

        mp.Process.__init__(self, name=name)

        # we don't want processes to linger around, waiting for children to
        # terminate, and thus create sub-processes as daempns.
        #
        # NOTE: this requires the `patchy.mc_patchface()` fix in `start()`.
      # self.daemon = True                # 


    # --------------------------------------------------------------------------
    #
    def _rup_watcher(self, to_watch):
        '''
        When `start()` is called, the parent process will create a socket pair.
        after fork, one end of that pair will be watched by the parent and
        client, respectively, in a separate watcher thread.  If any error
        condition or hangup is detected on the end, it is assumed that the
        process on the other end died, and termination is initiated.

        The rup_watcher will also watch whatever entity is found in the
        `self._rup_tu_watch` list, which is expected to contain `mt.Thread` and
        `mp.Process` instances.  If any of those is found to be 
        `not thing.is_alive()`, termination is initiated.

        Since the watch happens in a subthread, any termination requires the
        attention and cooperation of the main thread.  No attempt is made on
        interrupting the main thread.
        '''

      # print "### %s: enumerate: %s" % (os.getpid(), [t.name for t in mt.enumerate()])

        # make sure we watch different ends:
        if to_watch == 'parent':
            self._rup_sp[0].close()
            fd = self._rup_sp[1]
        elif to_watch == 'child':
            self._rup_sp[1].close()
            fd = self._rup_sp[0]
        else:
            raise RuntimeError('invalid proc watcher role')

        poller = select.poll()
        poller.register(fd, select.POLLERR | select.POLLHUP)

        # we watch threads and proces as long as we live
        while not self._rup_term.is_set():

          # print '%s: watch %s' % (os.getpid(), to_watch)

            # first check health of parent/child relationship
            event = poller.poll(0)  # zero timeout: non-blocking

            if event:
              # print '%s: %s dies' % (os.getpid(), to_watch)
                # the socket is dead - terminate!
                einfo = self._rup_terminfo.value.strip()
                self._rup_terminfo.value = '%s died %s: %s' % (to_watch, event, einfo)
                break

            # now watch all other registered watchables
            abort = False
            for thing in self._rup_to_watch:
           
                if not thing.is_alive():
                    self._rup_terminfo.value = '%s died' % (thing.name)
                    abort = True
                    break  #  don't bother about the others
           
            if abort:
                break # stop watching, terminate

            # all is well -- sleep a bit to avoid busy poll
            time.sleep(_WATCH_TIMEOUT)

        if not self._rup_term.is_set():

            # we broke above, and need to terminate
            if self._rup_log:
                self._rup_log.info(self._rup_terminfo.value)
            self._rup_term.set()


        # nmake sure to actually *exit* the thread (we are not a daemon!) - but
        # remember that the MainThread will not be notified, nor exited this way
        # https://bugs.python.org/issue6634
        sys.exit()


    # --------------------------------------------------------------------------
    #
    def to_watch(self, thing):
        '''
        The rup_watcher thread monitors the class' child process, but cajn also
        watch other processes and threads, as needed.  This method allows to
        register `mt.Thread` and `mp.Process` instances for watching.    If any
        of them is found to be not alive (tested via `thing.is_alive()`), all
        watched threads and processes (including the original child process) are 
        stopped, and then terminated.
        '''

        assert(isinstance(thing, mt.Thread) or isinstance(thing, mp.Process))

        with self._rup_lock:
            self._rup_to_watch.append(thing)


    # --------------------------------------------------------------------------
    #
    def is_alive(self):
        '''
        Overload the `mp.Process.is_alive()` method, and check both the job
        state *and* the state of the `self._rup_alive` flag.
        '''

      # print '%s: is_alive flag/proc: %s/%s' % (os.getpid(), 
      #                                          self._rup_alive.is_set(), 
      #                                          mp.Process.is_alive(self))

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


        # Daemon processes can't fork child processes in Python, because...
        # Well, they just can't.  We want to use daemons though to avoid hanging
        # processes if, for some reason, communication of termination conditions
        # fails.
        #
        # Patchy McPatchface to the rescue (no, I am not kidding): we remove
        # that useless assert (of all things!) on the fly.
        #
        # NOTE: while this works, we seem to have the socketpair-based detection
        #       stable enough to not need the monkeypatch.
        #
      # _daemon_fork_patch = '''\
      #     *** process_orig.py  Sun Nov 20 20:02:23 2016
      #     --- process_fixed.py Sun Nov 20 20:03:33 2016
      #     ***************
      #     *** 5,12 ****
      #           assert self._popen is None, 'cannot start a process twice'
      #           assert self._parent_pid == os.getpid(), \\
      #                  'can only start a process object created by current process'
      #     -     assert not _current_process._daemonic, \\
      #     -            'daemonic processes are not allowed to have children'
      #           _cleanup()
      #           if self._Popen is not None:
      #               Popen = self._Popen
      #     --- 5,10 ----
      #     '''
      #
      # import patchy
      # patchy.mc_patchface(mp.Process.start, _daemon_fork_patch)

        # before we call `mp.Process.start(), ie. before invoking the `fork()`
        # system call, we create a socketpair.  Both parent and child will watch
        # one end of that socketpair, which thus acts as a lifeline between the
        # processes, to detect abnormal termination in the process tree.
        self._rup_sp = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)

        # start `self.run()` in the child process, and wait for it's
        # initalization to finish, which will set `self._rup_alive`.
        mp.Process.start(self)

        # immediately after the fork we start watching our socketpair end in
        # a separate thread
        proc_watcher = mt.Thread(target=self._rup_watcher, args=['child'])
        proc_watcher.daemon = True
        proc_watcher.start()

      # print '%s: p 1 %s' % (os.getpid(), proc_watcher.name)

        # wait until we find any of:
        #   - that child has died  <-- self._rup_term.is_set() == True
        #   - child sends alive event
        #   - timeout
        start = time.time()
        while not self._rup_term.is_set():
            if not mp.Process.is_alive(self): break
            if self._rup_alive.is_set()     : break
            if time.time()-start > timeout  : break
            time.sleep(0.1)

      # print "%s: -------" % os.getpid()
      # print "%s: %s" % (os.getpid(), mp.Process.is_alive(self))
      # print "%s: %s" % (os.getpid(), self._rup_alive.is_set())
      # print "%s: %s" % (os.getpid(), time.time()-start)

        # if that did't work out, we consider the child failed.  To be on the
        # safe side, we send it a kill signal, and then stop.
        if not self.is_alive():

            if  self._rup_log:
                self._rup_log.debug('start process failed: %s',
                                    self._rup_terminfo.value)

            try:
                self.stop()
            except:
                pass

            if self._rup_alive.is_set():
                # the child actually became alive -- we leave error detection to
                # the watcher
                pass
            else:
                # the child never made it - report error straight away
                raise RuntimeError('child failed [%s]' %
                                   self._rup_terminfo.value)

        # if we got this far, then all is well, we are done.
        if  self._rup_log:
            self._rup_log.debug('start process ok')


    # --------------------------------------------------------------------------
    #
    def _rup_initialize_child(self):
        '''
        Call custom child initializer.
        '''

        self.initialize_child()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* before
        the class' `work()` method.  The child is only considered to be 'alive'
        after this method has been completed without error.
        '''

        if  self._rup_log:
            self._rup_log.debug('initialize_child (NOOP)')


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

        Before the first invocation, `self.initialize_child()` will be called.
        After the last invocation, `self.finalize_child()` will be called, if
        possible.  The latter will not always be possible if the child is
        terminated by a signal, such as when the parent process calls
        `child.terminate()` -- `child.stop()` should be used instead.
        '''

        raise NotImplementedError('ru.Process.work() MUST be overloaded')


    # --------------------------------------------------------------------------
    #
    def _rup_finalize_child(self):
        '''
        Call custom child finalizer.
        '''

      # for thing in self._rup_to_watch:
      #     print "%s === stop %s" % (os.getpid(), thing.name) 
      #     thing.stop()
      #
      # for thing in self._rup_to_watch:
      #     print "%s === term %s" % (os.getpid(), thing.name) 
      #     thing.terminate()
      #
      # for thing in self._rup_to_watch:
      #     print "%s === join %s" % (os.getpid(), thing.name) 
      #     thing.join()

        self.finalize_child()


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* after
        the class' `work()` method.  The child is considered 'alive' until this
        method has been completed.

        NOTE: `self.finalize_child()` will not be called when
        `self.initalize_child()` failed!
        '''

        if  self._rup_log:
            self._rup_log.debug('finalize_child (NOOP)')


  # # --------------------------------------------------------------------------
  # #
  # def _rup_finalize_parent(self):
  #     '''
  #     Call custom parent finalizer.
  #     '''
  #
  #     for thing in self._rup_to_watch:
  #         print "%s === stop %s" % (os.getpid(), thing.name) 
  #         thing.stop()
  #
  #     for thing in self._rup_to_watch:
  #         print "%s === term %s" % (os.getpid(), thing.name) 
  #         thing.terminate()
  #
  #     for thing in self._rup_to_watch:
  #         print "%s === join %s" % (os.getpid(), thing.name) 
  #         thing.join()
  #
  #     self.finalize_parent()
  #
  #
  # # --------------------------------------------------------------------------
  # #
  # def finalize_parent(self):
  #     '''
  #     This method can be overloaded, and will then be executed during
  #     the class' `stop()` method.
  #     '''
  #
  #     if  self._rup_log:
  #         self._rup_log.debug('finalize_parent (NOOP)')
  #
  #
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
          # print '%s: check parent %s' % (os.getpid(), self._rup_ppid)
            os.kill(self._rup_ppid, 0)
          # print '%s: check parent %s ok' % (os.getpid(), self._rup_ppid)
            return True

        except Exception as e:
          # print "%s: %s" % (os.getpid(), e)
            return False

        except:
          # print '%s: check parent %s nok' % (os.getpid(), self._rup_ppid)
            return False


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This method MUST NOT be overloaded!

        This is the workload of the child process.  It will first call
        `self.initialize_child()`, and then repeatedly call `self.work()`, until
        being terminated.  When terminated, it will call `self.finalize_child()`
        and exit.

        The implementation of `work()` needs to make sure that this process is
        not spinning idly -- if there is nothing to do in `work()` at any point
        in time, the routine should at least sleep for a fraction of a second or
        something.

        `finalize_child()` is only guaranteed to get executed on `self.stop()`
        -- a hard kill via `self.terminate()` may or may not be trigger to run
        `self.finalize_child()`.

        The child process will automatically terminate when the parent process
        dies (then including the call to `self.finalize_child()`).  It is not
        possible to create daemon or orphaned processes -- which is an explicit
        purpose of this implementation.
        '''

        # first order of business: watch parent health
        proc_watcher = mt.Thread(target=self._rup_watcher, args=['parent'])
        proc_watcher.daemon = True
        proc_watcher.start()

      # print '%s: =============== c 1 %s' % (os.getpid(), proc_watcher.name)
      # if proc_watcher.name == 'Thread-2':
      #     print_stacktraces()

        try:
            self._rup_initialize_child()
        except BaseException as e:
            if  self._rup_log:
                self._rup_log.exception('initialize_child() raised exception: %s', e)
            self._rup_terminfo.value = 'exception %s' % repr(e)
            sys.exit(1)

        self._rup_alive.set() # signal successful child startup
      # print '%s: c 2' % os.getpid()
 
        # enter loop to repeatedly call 'work()'.
        while True:

            if self._rup_term.is_set():
                self._rup_terminfo.value = 'terminated'
                break
          # print '%s: c 3' % os.getpid()
 
            if not self._parent_is_alive():
                self._rup_terminfo.value = 'orphaned'
                self._rup_term.set()
                break
          # print '%s: c 4' % os.getpid()
 
            try:
                self.work()
            except BaseException as e:
                # this is a very global except, and also catches sys.exit(),
                # keyboard interrupts, etc.  Ignore pylint and PEP-8, we want 
                # it this way!
              # print '%s: work failed: %s: %s' % (os.getpid(), type(e), e)
                if  self._rup_log:
                    self._rup_log.exception('work() raised exception: %s', e)
                self._rup_terminfo.value = 'exception %s' % repr(e)
                self._rup_term.set()
                break
          # print '%s: c 5' % os.getpid()

        # we sould have no other way of getting here
        assert(self._rup_term.is_set())
 
        if  self._rup_log:
            self._rup_log.info('child is terminating: %s',
                               self._rup_terminfo.value)

        try:
          # print '%s: c 6a' % os.getpid()
            self._rup_finalize_child()
          # print '%s: c 6b' % os.getpid()
        except BaseException as e:
          # print '%s: c 6c : %s' % (os.getpid(), e)
            if  self._rup_log:
                self._rup_log.exception('finalize_child() raised exception: %s', e)
            self._rup_terminfo.value = 'exception %s' % repr(e)
            sys.exit(1)

      # print '%s: c 6d'
      # print '%s: c 7 [child] %s' % (os.getpid(), self._rup_term.is_set())
        proc_watcher.join(timeout=3.0)
      # if proc_watcher.is_alive():
      #     print '%s: c 8: could not join watcher %s' % (os.getpid(), proc_watcher.name)
      # else:
      #     print '%s: c 8: could join watcher %s' % (os.getpid(), proc_watcher.name)
      # print_stacktraces()
      # print '%s: c 9 [child] OK' % os.getpid()

        sys.exit(0) # terminate child process


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''
        Signal termination via `self._rup_term.set()`.
        Fall back to `terminate()` if child does not die.
        '''

        if not mp.Process.is_alive(self):
            # nothing to do
            if  self._rup_log:
                self._rup_log.debug('nothing to stop')
            return

        if  self._rup_log:
            self._rup_log.debug('stop child')

        # FIXME: this does not work since switching to mt.Event.  Close the pipe
        # to trigger termination!
        self._rup_term.set()

        # FIXME: make timeout configurable
        start = time.time()
        while mp.Process.is_alive(self):
            if time.time() - start > 5.0:
                break
            time.sleep(0.1)

        if mp.Process.is_alive(self):
            # let it die already...
            self.terminate()  # hard kill

        # collect process
        self.join()


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

