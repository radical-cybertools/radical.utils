
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

from .debug  import print_stacktrace, print_stacktraces
from .logger import get_logger

_ALIVE_MSG     = 'alive'  # message to use as alive signal
_ALIVE_TIMEOUT = 5.0      # time to wait for process startup signal.
                          # startup signal: 'alive' message on the socketpair;
                          # is sent in both directions to ensure correct setup
_WATCH_TIMEOUT = 0.1      # time between thread and process health polls.
                          # health poll: check for recv, error and abort
                          # on the socketpair; is done in a watcher thread.
_START_TIMEOUT = 5.0      # time between starting child and finding it alive
_STOP_TIMEOUT  = 5.0      # time between temination signal and killing child
_BUFSIZE       = 1024     # default buffer size for socket recvs


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
# TODO: we should switch to fork/*exec*, if possible...
# 
class Process(mp.Process):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log=None):

        self._rup_alive    = mt.Event()     # set after child process startup,
                                            # unset after demise # FIXME: check
        self._rup_term     = mt.Event()     # set to terminate threads, 
                                            # including the MainThread
        self._rup_termlock = mt.Lock()      # lock terminfo manipulation
        self._rup_terminfo = ''             # report on termination causes
        self._rup_log      = log            # ru.logger for debug output
        self._rup_ppid     = os.getpid()    # get parent process ID
        self._rup_to_watch = list()         # threads and processes to watch
        self._rup_lock     = mt.RLock()     # thread lock
        self._rup_timeout  = _ALIVE_TIMEOUT # interval to check peer health
        self._rup_watcher  = None           # watcher thread

        self._tmp = False

        if not self._rup_log:
            # if no logger is passed down, log to stdout
            self._rup_log = get_logger('radical.util.process', target='null')

        mp.Process.__init__(self, name=name)

        # we don't want processes to linger around, waiting for children to
        # terminate, and thus create sub-processes as daemons.
        #
        # NOTE: this requires the `patchy.mc_patchface()` fix in `start()`.
      # self.daemon = True


    # --------------------------------------------------------------------------
    #
    def _rup_add_terminfo(self, msg):
        '''
        append a new message to self._rup_terminfo.  Ultimately that will
        accumulate a newline delimited set of error messages and termination
        notifications, to be flushed down the socketpair upon  termination.
        '''

        with self._rup_termlock:
            self._rup_log.info('terminfo: %s' % msg.strip())
            self._rup_terminfo += '%s\n' % msg.strip()



    # --------------------------------------------------------------------------
    #
    def _rup_watch(self):
        '''
        When `start()` is called, the parent process will create a socket pair.
        after fork, one end of that pair will be watched by the parent and
        client, respectively, in separate watcher threads.  If any error
        condition or hangup is detected on the socket, it is assumed that the
        process on the other end died, and termination is initiated.

        The rup_watcher will also watch whatever entity is found in the
        `self._rup_to_watch` list, which is expected to contain `mt.Thread` and
        `mp.Process` instances.  If any of those is found to be in the condition
        `!thing.is_alive()`, termination is initiated.

        Since the watch happens in a subthread, any termination requires the
        attention and cooperation of the main thread.  No attempt is made on
        interrupting the main thread, we only set self._rup_term which needs to
        be checked by the main threads in certain intervals.
        '''

        pid = os.getpid()

        # Both parent and child share the same socketpair -- but the socket
        # roles (send/recv) are inversed.  Make sure we watch different ends,
        # depending on our role.
        no_endpoint = None 
        sp_endpoint = None
        if self._rup_is_child:
            sp_endpoint = self._rup_sp[0]
            no_endpoint = self._rup_sp[1]

        elif self._rup_is_parent:
            no_endpoint = self._rup_sp[0]
            sp_endpoint = self._rup_sp[1]

        else:
            raise RuntimeError('invalid rup_watcher role')

        no_endpoint.close()

        # The watcher will only be started once initialization via
        # `self.initialize_child()` and `self.initialize_parent` etc. are done,
        # so starting the watcher completes the intialization.  The parent will
        # wait for an 'ALIVE' message for the child.
        #
        # FIXME: move to _initialize_common
        #
        # only the child sends an alive message ...
        if self._rup_is_child: 

            try:
                self._rup_log.debug('child sends alive')
                sp_endpoint.send(_ALIVE_MSG)

            except Exception as e:
                # we can't do much at this point but bail out...
                self._rup_log.exception('alive send error')
                sp_endpoint.close()
                raise

        # ... and only the parent receives it, with a timeout.
        #
        # NOTE: If the child does not bootstrap fast enough, the timeout will
        #       kick in, and the child will be considered dead, failed and/or
        #       hung, and will be terminated!  Timeout can be set as parameter
        #       to the `start()` method.
        if self._rup_is_parent:

            sp_endpoint.settimeout(self._rup_timeout)
            try:
                msg = sp_endpoint.recv(len(_ALIVE_MSG))

                if msg != 'alive':
                    self._rup_add_terminfo('unexpected child message (%s)' % msg)
                    raise RuntimeError('abort')

                self._rup_log.info('child is alive (%s)', msg)
                self._rup_alive.set()  # inform main thread

            except socket.timeout:
                self._rup_add_terminfo('no alive message from child')
                self._rup_term.set()

            except Exception as e:
                self._rup_log.exception('alive read error')
                self._rup_add_terminfo('no alive message from child: %s' % e)
                self._rup_term.set()

        # if we survived setup and alive messaging, we prepare for polling the
        # socketpair for health
        if not self._rup_term.is_set():

            # for further alive checks, we poll socket state for 
            #   * data:   generally termination messages, terminate
            #   * error:  child failed   - terminate
            #   * hangup: child finished - terminate
            # Basically, whatever happens, we terminate... :-P
            poller = select.poll()
            poller.register(sp_endpoint, select.POLLERR | select.POLLHUP | select.POLLIN)


        # we watch threads and processes as long as we live
        while not self._rup_term.is_set():

            # first check health of parent/child relationship
            event = poller.poll(_WATCH_TIMEOUT)

            if event:
                # uh, something happened on the other end, we are about to die
                # out of solidarity (or panic?).  Lets first though check if we
                # can get any information about the remote termination cause.

                msg = None
                try:
                    msg = sp_endpoint.recv(_BUFSIZE)
                except Exception as e:
                    self._rup_log.exception('message recv failed')
                    msg = '[message recv error]'

                self._rup_add_terminfo('termination triggered: %s' % (msg))
                break

            # now watch all other registered watchables
            abort = False
            with self._rup_lock:
                for thing in self._rup_to_watch:
                    if not thing['handle'].is_alive():
                        self._rup_add_terminfo('%s died' % (thing['handle'].name))
                        abort = True
                        break  #  don't bother about the others
           
            if abort:
                break # stop watching, terminate


        if not self._rup_term.is_set():

            # we broke above, so we terminate all things we watch, and terminate
            # ourself, too.
            with self._rup_lock:
                for thing in self._rup_to_watch:
                    self._rup_log.info('terminate %s', thing['handle'].name_)
                    thing['term'].set()

            self._rup_term.set()


        # before we finish the watcher thread, we *always* send terminfo to the
        # other end, and then close the socketpair.  This will either let the
        # child watche know that is has to die, or will let the parent watcher
        # know that the child is about to die.
        with self._rup_termlock:
            if self._rup_terminfo:
                try:
                    sp_endpoint.send(self._rup_terminfo)
                except:
                    # ignore any errors here
                    self._rup_log.warn('sending terminfo failed')
                    pass

        sp_endpoint.close()
        sp_endpoint.close()


        # Make sure to actually *exit* the thread - but remember that the
        # MainThread will not be notified, nor exited this way
        # https://bugs.python.org/issue6634
        self._rup_log.info('watcher terminate') 
        sys.exit()


    # --------------------------------------------------------------------------
    #
    def to_watch(self, thing, term): 
        '''
        The `_rup_watch` thread monitors the class' child process, but can
        also watch other processes and threads, as needed.  The parameters are
        'thing', a handle to watch (`thing.is_alive()` needs to be callable on
        that handle), and `term`, a signal for that thing to terminate
        (`term.set() needs to be callable on the signal).
        
        If any of the known threads or processes is found to be not alive
        (tested via `thing.is_alive()`), all watched threads and processes
        (including the original child process) are terminated (via
        `term.set()`).  No attemp is made to kill the things otherwise, or to
        collect them via join().
        '''

        # FIXME: require `stop()` ?

        assert(isinstance(thing, mt.Thread) or isinstance(thing, mp.Process))
        assert(isinstance(term,  mt.Event ) or isinstance(term,  mp.Event  ))

        with self._rup_lock:
            if thing.name in self._rup_to_watch:
                self._rup_log.warn('ignore double watch request [%s]' % thing.name)
            self._rup_to_watch[thing.name] = {'handle': thing,
                                              'term'  : event}


    # --------------------------------------------------------------------------
    #
    def start(self, timeout=_START_TIMEOUT):
        '''
        Overload the `mp.Process.start()` method, and block (with timeout) until
        the child signals to be alive via a message over our socket pair.
        '''

        self._rup_log.debug('start process')

        if timeout != None:
            self._rup_timeout = timeout

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

        # set role flags.
        # The run method sets those flags inversed for the child.
        self._rup_is_child  = False
        self._rup_is_parent = True

        # start `self.run()` in the child process, and wait for it's
        # initalization to finish, which will send the 'alive' message.
        mp.Process.start(self)

        # immediately after the fork we start watching our socketpair end in
        # a separate thread.  The watcher will also intercept the 'alive'
        # message and set self._rup_alive.
        
        # NOTE: 
        # - https://bugs.python.org/issue1856  (01/2008)
        #   `sys.exit()` can segfault Python if daemon threads are active.
        # - https://bugs.python.org/issue21963 (07/2014)
        #   This will not be fixed in python 2.x.
        #
        # We make the Watcher thread a daemon anyway: 
        #
        # - when `sys.exit()` is called in a child process, we don't care about
        #   the process anymore anyway, and all terminfo data are sent to the
        #   parent anyway.
        # - when `sys.exit()` is called in the parent on unclean shutdown, the
        #   same holds.
        # - when `sys.exit()` is called in the parent on clean shutdown, then
        #   the watcher threads should already be terminated when the
        #   `sys.exit()` invocation happens
        #
        # FIXME: check the conditions above
        self._rup_watcher = mt.Thread(target=self._rup_watch)
        self._rup_watcher.start()
        time.sleep(0.1)

        # wait until we find any of:
        #   - that child has died  <-- self._rup_term.is_set() == True
        #   - child sends alive event
        #   - timeout
        start = time.time()
        while not self._rup_term.is_set():
            if not mp.Process.is_alive(self): break  # process is gone
            if self._rup_alive.is_set()     : break  # alive is set
            if time.time()-start > timeout  : break  # timeout met
            time.sleep(0.1)


        # if that did't work out, we consider the child failed.
        if not self.is_alive() or not self._rup_alive.is_set():

            self._rup_log.debug('start process failed: %s / %s',
                    self.is_alive(), self._rup_alive.is_set())

            try:
                self.stop()
            except:
                pass

            raise RuntimeError('child startup failed: %s', self._rup_terminfo)

        # if we got this far, then all is well, we are done.
        self._rup_log.debug('child process started')

        self._rup_initialize()


    # --------------------------------------------------------------------------
    #
    def _rup_initialize(self):
        '''
        Call custom initializers.
        '''

        if self._rup_is_child:
            self.initialize_common()
            self.initialize_child()

        elif self._rup_is_parent:
            self.initialize_common()
            self.initialize_parent()

        else:
            raise RuntimeError('invalid process role')


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, for both the parent and the child process (individually).  If
        this fails on either side, the process startup is considered failed.
        '''

        self._rup_log.debug('initialize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the parent process.  If this fails, the process startup is
        considered failed.
        '''

        self._rup_log.debug('initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the child process.  If this fails, the process startup is 
        considered failed.
        '''

        self._rup_log.debug('initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _rup_finalize(self):
        '''
        Call custom finalizers.
        '''

        if self._rup_is_child:
            self.finalize_child()
            self.finalize_common()

        elif self._rup_is_parent:
            self.finalize_parent()
            self.finalize_common()

        else:
            raise RuntimeError('invalid process role')


    # --------------------------------------------------------------------------
    #
    def finalize_common(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or process child termination, in the parent process, in both
        the parent and the child process (individually).
        '''

        self._rup_log.debug('finalize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or process child termination, in the parent process.
        '''

        self._rup_log.debug('finalize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or process child termination, in the child process.
        '''

        self._rup_log.debug('finalize_child (NOOP)')


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
    def _parent_is_alive(self):
        '''
        This private method checks if the parent process is still alive.  This
        obviously only makes sense when being called in the child process.

        Note that there is a (however unlikely) race: PIDs are reused, and the
        process could be replaced by another process with the same PID inbetween
        tests.  We thus also except *all* exception, including permission
        errors, to capture at least some of those races.
        '''

        # This method is an additional fail-safety check to the socketpair
        # watching performed by the watcher thread -- either one should
        # actually suffice.

        assert(self._rup_is_child)

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

        # set role flags.  
        # The start method sets those flags inversed for the parent
        self._rup_is_child  = True
        self._rup_is_parent = False

        # Both parent and child share the same socketpair -- but the socket
        # roles (send/recv) are inversed.  Make sure we watch different ends,
        # depending on our role.
        sp_endpoint = None 
        no_endpoint = None
        if self._rup_is_child:
            sp_endpoint = self._rup_sp[0]
            no_endpoint = self._rup_sp[1]

        elif self._rup_is_parent:
            no_endpoint = self._rup_sp[0]
            sp_endpoint = self._rup_sp[1]

        else:
            raise RuntimeError('invalid rup_watcher role')

        no_endpoint.close()
        #  FIXME: send terminfo on, well, termination


        # first order of business: watch parent health
        self._rup_watcher = mt.Thread(target=self._rup_watch)
        self._rup_watcher.start()

        try:
            self._rup_initialize()
        except BaseException as e:
            self._rup_log.exception('child initialization error')
            self._rup_add_terminfo('initialize() raised %s' % repr(e))
            self._rup_term.set()

        # enter loop to repeatedly call 'work()'.
        while not self._rup_term.is_set():

            if not self._parent_is_alive():
                self._rup_add_terminfo('orphaned')
                self._rup_term.set()
                break
 
            try:
                self.work()

            except Exception as e:
                self._rup_log.exception('child worker error')
                self._rup_add_terminfo('work() raised %s' % repr(e))
                self._rup_term.set()
                break

            except BaseException as e:
                # This is a very global except, and also catches sys.exit(),
                # keyboard interrupts, etc.  Ignore pylint and PEP-8, we want 
                # it this way!
                self._rup_add_terminfo('work() signaled for exit %s' % repr(e))
                self._rup_term.set()
                break

        # we should have no other way of getting here
        assert(self._rup_term.is_set())
 
        self._rup_add_terminfo('terminating')

        try:
            # note that we *always* call the finalizers, even if the
            # initializers failed for some reason or the other...
            self._rup_finalize()
        except BaseException as e:
            self._rup_log.exception('child finalization error')
            self._rup_add_terminfo('finalize() raised %s' % repr(e))

        # tear down child watcher
        if self._rup_watcher:
            self._rup_watcher.join(_STOP_TIMEOUT)

        # all is done and said - begone!
        sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=_STOP_TIMEOUT):
        '''
        `stop()` can only be called by the parent (symmetric to `start()`).
        
        We wait for some  `timeout` seconds to make sure the child is dead, and
        otherwise send a hard kill signal.  The default timeout is 5 seconds.
        Note that `stop()` implies `join()`!
        '''

        # We leave the actual stop handling via the socketpair to the watcher.
        #
        # The parent will fall back to a terminate if the watcher does not
        # appear to be able to kill the child

        assert(self._rup_is_parent)

        self._rup_log.info('parent stops child')

        # call finalizers
        self._rup_finalize()

        # tear down watcher
        self._rup_term.set()
        if self._rup_watcher:
            self._rup_watcher.join(timeout)

        mp.Process.join(self, timeout)
        
        # make sure child is gone
        if mp.Process.is_alive(self):
            self._rup_log.error('failed to stop child - terminate')
            self.terminate()  # hard kill

        # don't exit - but signal if child survives
        if mp.Process.is_alive(self):
            raise RuntimeError('failed to stop child')


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=_STOP_TIMEOUT):

      # raise RuntimeError('call stop instead!')
      #
      # we can't really raise the exception above, as the mp module calls this
      # join via at_exit :/
        mp.Process.join(self)


# ------------------------------------------------------------------------------

