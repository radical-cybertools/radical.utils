
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import select
import socket
import threading       as mt
import multiprocessing as mp
import setproctitle    as spt

from .logger import get_logger

from .threads import is_main_thread

# ------------------------------------------------------------------------------
#
_ALIVE_MSG     = 'alive'  # message to use as alive signal
_START_TIMEOUT = 5.0      # time to wait for process startup signal.
                          # startup signal: 'alive' message on the socketpair;
                          # is sent in both directions to ensure correct setup
_WATCH_TIMEOUT = 0.5      # time between thread and process health polls.
                          # health poll: check for recv, error and abort
                          # on the socketpair; is done in a watcher thread.
_STOP_TIMEOUT  = 5.0      # time between temination signal and killing child
_BUFSIZE       = 1024     # default buffer size for socket recvs


# ------------------------------------------------------------------------------
#
class Process(mp.Process):
    '''
    This Process class is a thin wrapper around multiprocessing.Process which
    specifically manages the process lifetime in a more cautious and copperative
    way than the base class: *no* attempt on signal handling is made, we expect
    to exclusively communicate between parent and child via a socket.
    A separate thread in both processes will watch that socket: if the socket
    disappears, we interpret that as the other process being terminated, and
    begin process termination, too.
   
    NOTE: At this point we do not implement the full mp.Process constructor.
   
    The class also implements a number of initialization and finalization
    methods which can be overloaded by any deriving class.  While this can at
    first be a confusing richness of methods to overload, it significantly
    simplifies the implementation of non-trivial child processes.  By default,
    none of the initialized and finalizers needs to be overloaded.
   
    An important semantic difference are the `start()` and `stop()` methods:
    both accept an optional `timeout` parameter, and both guarantee that the
    child process successfully started and completed upon return, respectively.
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
   
    Any class which derives from this Process class *must* overload the 'work()`
    method.  That method is repeatedly called by the child process' main loop,
    until:
      - an exception occurs (causing the child to fail with an error)
      - `False` is returned by `work()` (causing the child to finish w/o error)
   
    TODO: We should switch to fork/*exec*, if possible.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log=None):

        # At this point, we only initialize members which we need before fork...
        self._rup_log       = log            # ru.logger for debug output

        # ... or are *shared* between parent and child process.
        self._rup_ppid      = os.getpid()    # get parent process ID

        # most importantly, we create a socketpair.  Both parent and child will
        # watch one end of that socket, which thus acts as a lifeline between
        # the processes, to detect abnormal termination in the process tree.
        # The socket is also used to send messages back and forth.
        self._rup_sp = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)

        # all other members are set to None (or similar) to make pylint happy,
        # but are actually initialized later in `start()`/`run()`, and the
        # various initializers.
        self._rup_name        = name           # use for setproctitle
        self._rup_started     = False          # start() has been called
        self._rup_spawned     = None           # set in start(), run()
        self._rup_is_parent   = None           # set in start()
        self._rup_is_child    = None           # set in run()
        self._rup_endpoint    = None           # socket endpoint for sent/recv
        self._rup_term        = None           # set to terminate watcher
        self._rup_watcher     = None           # watcher thread
        self._rup_things      = None           # list of threads created in
                                               # (parent or child) process
        self._rup_things_lock = None           # lock for the above

        # FIXME: assert that start() was called for some / most methods

        if not self._rup_log:
            # if no logger is passed down, log to null
            self._rup_log = get_logger('radical.util.process', target='null')

        # base class initialization
        super(Process, self).__init__(name=name)

        # we don't want processes to linger around, waiting for children to
        # terminate, and thus create sub-processes as daemons.
        #
        # NOTE: this requires the `patchy.mc_patchface()` fix in `start()`.
        #
      # self.daemon = True


    # --------------------------------------------------------------------------
    #
    def _rup_msg_send(self, msg):
        '''
        send new message to self._rup_endpoint.  We make sure that the
        message is not larger than the _BUFSIZE define in the RU source code.
        '''

        if not self._rup_spawned:
            # no child, no communication channel
            raise RuntimeError("can't communicate w/o child")

        # NOTE:  this method should only be called by the watcher thread, which
        #        owns the endpoint.
        # FIXME: BUFSIZE should not be hardcoded
        # FIXME: check for socket health

        if len(msg) > _BUFSIZE:
            raise ValueError('message is larger than %s: %s' % (_BUFSIZE, msg))

        try:
            self._rup_log.info('send message: %s', msg)
            self._rup_endpoint.send('%s\n' % msg)

        except Exception as e:
            self._rup_log.exception('failed to send message %s', msg)


    # --------------------------------------------------------------------------
    #
    def _rup_msg_recv(self, size=_BUFSIZE):
        '''
        receive a message from self._rup_endpoint.  We only check for messages
        of *up to* `size`.  This call is non-blocking: if no message is
        available, return an empty string.
        '''

        if not self._rup_spawned:
            # no child, no communication channel
            raise RuntimeError("can't communicate w/o child")

        # NOTE:  this method should only be called by the watcher thread, which
        #        owns the endpoint (no lock used!)
        # FIXME: BUFSIZE should not be hardcoded
        # FIXME: check for socket health

        try:
            msg = self._rup_endpoint.recv(size, socket.MSG_DONTWAIT)
            self._rup_log.info('recv message: %s', msg)
            return msg

        except Exception as e:
            self._rup_log.exception('failed to recv message')
            return ''


    # --------------------------------------------------------------------------
    #
    def _rup_watch(self):
        '''
        When `start()` is called, the parent process will create a socket pair.
        after fork, one end of that pair will be watched by the parent and
        client, respectively, in separate watcher threads.  If any error
        condition or hangup is detected on the socket, it is assumed that the
        process on the other end died, and termination is initiated.

        Since the watch happens in a subthread, any termination requires the
        attention and cooperation of the main thread.  No attempt is made on
        interrupting the main thread, we only set self._rup_term which needs to
        be checked by the main threads in certain intervals.
        '''

        # we watch sockets and threads as long as we live, ie. until the main
        # thread sets `self._rup_term`.
        try:

            self._rup_poller = select.poll()
            self._rup_poller.register(self._rup_endpoint, select.POLLERR | select.POLLHUP | select.POLLIN)

            last = 0.0  # we never watched anything until now
            while not self._rup_term.is_set() :

                # only do any watching if time is up
                now = time.time()
                if now - last < _WATCH_TIMEOUT:
                    time.sleep(1)  #
                    continue

                self._rup_watch_socket()
                self._rup_watch_things()

                # FIXME: also *send* any pending messages to the child.
              # # check if any messages need to be sent.  
              # while True:
              #     try:
              #         msg = self._rup_msg_out.get_nowait()
              #         print 'out: %s' % msg
              #         self._rup_msg_send(msg)
              #
              #     except Queue.Empty:
              #         # nothing more to send
              #         break


        except Exception as e:
            # mayday... mayday...
            self._rup_log.exception('watcher failed')

        finally:
            # no matter why we fell out of the loop: let the other end of the
            # socket know by closing the socket endpoint.
            self._rup_endpoint.close()

            # `self.stop()` will be called from the main thread upon checking
            # `self._rup_term` via `self.is_alive()`.
            # FIXME: check
            self._rup_term.set()

    # --------------------------------------------------------------------------
    #
    def _rup_watch_socket(self):

        # check health of parent/child relationship
        events = self._rup_poller.poll(0.0)   # don't block
        for _,event in events:

            # for alive checks, we poll socket state for
            #   * data:   some message from the other end, logged
            #   * error:  child failed   - terminate
            #   * hangup: child finished - terminate

            # check for error conditions
            if  event & select.POLLHUP or  \
                event & select.POLLERR     :

                # something happened on the other end, we are about to die
                # out of solidarity (or panic?).  
                self._rup_log.warn('endpoint disappeard')
                raise RuntimeError('endpoint disappeard')
            
            # check for messages
            elif event & select.POLLIN:
        
                # we get a message!
                #
                # FIXME: BUFSIZE should not be hardcoded
                # FIXME: we do nothing with the message yet, should be
                #        stored in a message queue.
                msg = self._rup_msg_recv(_BUFSIZE)
                self._rup_log.info('message received: %s' % msg)


    # --------------------------------------------------------------------------
    #
    def register_watchable(self, thing):
        '''
        Add an object to watch.  If the object is at any point found to be not
        alive (`thing.is_alive()` returns `False`), an exception is raised which
        will cause the watcher thread to terminate, and will thus also terminate
        this `ru.Process` instance.  All registered things will be terminated on
        `stop()`.

        The given `thing` is expected to have these four methods/properties

          - `thing.is_alive()`  -- perform health check
          - `thing.name`        -- a unique name identifying the thing
          - `thing.stop()`      -- method to terminate thing on regular shutdown
          - `thing.join()`      -- method to collect   thing on regular shutdown

        `thing.stop()` and `thing.join()` are expected to accept a timeout
        parameter, and are expected to raise exceptions if the operation fails.

        In other words, `things` are really expected to be radical.utils threads
        and processes, but we don't enforce this in terms of type checks.
        '''

        assert(thing.is_alive)
        assert(thing.name)
        assert(thing.stop)
        assert(thing.join)

        with self._rup_things_lock:
            if thing.name in self._rup_things:
                raise ValueError('already watching %s' % thing.name)
            self._rup_things[thing.name] = thing


    # --------------------------------------------------------------------------
    #
    def unregister_watchable(self, name):

        with self._rup_things_lock:
            if not name in self._rup_things:
                raise ValueError('%s is not watched' % name)

            del(self._rup_things[name])


    # --------------------------------------------------------------------------
    #
    def _rup_watch_things(self):

        with self._rup_things_lock:
            for thing in self._rup_things:
                if not thing.is_alive():
                    raise RuntimeError('%s died' % thing.name)


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True, timeout=None):
        '''
        Overload the `mp.Process.start()` method, and block (with timeout) until
        the child signals to be alive via a message over our socket pair.  Also
        run all initializers.

        If spawn is set to `False`, then no child process is actually create,
        but the parent initializers will be executed nonetheless.

        '''

        self._rup_log.debug('start process')

        if timeout is None:
            timeout = _START_TIMEOUT

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

        if spawn:
            # start `self.run()` in the child process, and wait for it's
            # initalization to finish, which will send the 'alive' message.
            super(Process, self).start()
            self._rup_spawned = True


        # this is the parent now - set role flags.
        self._rup_is_parent = True
        self._rup_is_child  = False

        # select different ends of the socketpair for further communication.
        self._rup_endpoint  = self._rup_sp[0]
        self._rup_sp[1].close()

        # from now on we need to invoke `self.stop()` for clean termination.
        # Having said that: the daemonic watcher thread and the socket lifeline
        # to the child should ensure that both will terminate in all cases, but
        # possibly somewhat delayed and apruptly.
        #
        # Either way: use a try/except to ensure `stop()` being called.
        try: 

            # we expect an alive message message from the child, within timeout
            #
            # NOTE: If the child does not bootstrap fast enough, the timeout will
            #       kick in, and the child will be considered dead, failed and/or
            #       hung, and will be terminated!  Timeout can be set as parameter
            #       to the `start()` method.
            try:
                self._rup_endpoint.settimeout(timeout)
                msg = self._rup_msg_recv(len(_ALIVE_MSG))

                if msg != 'alive':
                    # attempt to read remainder of message and barf
                    msg += self._rup_msg_recv()
                    raise RuntimeError('unexpected child message (%s)' % msg)

            except socket.timeout:
                raise RuntimeError('no alive message from child')


            # When we got the alive messages, only then will we call the parent
            # initializers.  This way those initializers can make some
            # assumptions about successful child process startup.
            self._rup_initialize()

        except Exception as e:
            self._rup_log.exception('initialization failed')
            self.stop()
            raise

        # if we got this far, then all is well, we are done.
        self._rup_log.debug('child process started')

        # child is alive and initialized, parent is initialized, watcher thread
        # is started - Wohoo!


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This method MUST NOT be overloaded!

        This is the workload of the child process.  It will first call the child
        initializers and then repeatedly call `self.work()`, until being
        terminated.  When terminated, it will call the child finalizers and
        exit.  Note that the child will also terminate once `work()` returns
        `False`.

        The implementation of `work()` needs to make sure that this process is
        not spinning idly -- if there is nothing to do in `work()` at any point
        in time, the routine should at least sleep for a fraction of a second or
        something.

        Child finalizers are only guaranteed to get called on `self.stop()` --
        a hard kill via `self.terminate()` may or may not be able to trigger to
        run the child finalizers.

        The child process will automatically terminate (incl. finalizer calls)
        when the parent process dies. It is thus not possible to create daemon
        or orphaned processes -- which is an explicit purpose of this
        implementation.  
        '''

        # FIXME: ensure that this is not overloaded

        # this is the child now - set role flags
        self._rup_is_parent = False
        self._rup_is_child  = True
        self._rup_spawned   = True

        # select different ends of the socketpair for further communication.
        self._rup_endpoint  = self._rup_sp[1]
        self._rup_sp[0].close()

        # set child name based on name given in c'tor, and use as procitle
        self._rup_name = self._rup_name + '.child'
        spt.setproctitle(self._rup_name)

        try:
            # we consider the invocation of the child initializers to be part of
            # the bootstrap process, which includes starting the watcher thread
            # to watch the parent's health (via the socket healt).
            try:
                self._rup_initialize()

            except BaseException as e:
                self._rup_log.exception('abort')
                self._rup_msg_send(repr(e))
                sys.stderr.write('initialization error in %s: %s\n' % (self._rup_name, repr(e)))
                sys.stderr.flush()
                self._rup_term.set()

            # initialization done - we only now send the alive signal, so the
            # parent can make some assumptions about the child's state
            if self._rup_term.is_set():
                self._rup_msg_send(_ALIVE_MSG)

            # enter the main loop and repeatedly call 'work()'.  
            #
            # If `work()` ever returns `False`, we break out of the loop to call the
            # finalizers and terminate.
            #
            # In each iteration, we also check if the socket is still open -- if it
            # is closed, we assume the parent to be dead and terminate (break the
            # loop).  We consider the eicket closed if `self._rup_term` was set
            # by the watchewr thread.
            while not self._rup_term.is_set() and \
                      self._parent_is_alive()     :
            
                # des Pudel's Kern
                if not self.work():
                    self._rup_msg_send('work finished')
                    break

                time.sleep(0.001)  # FIXME: make configurable

        except BaseException as e:

            # This is a very global except, also catches 
            # sys.exit(), keyboard interrupts, etc.  
            # Ignore pylint and PEP-8, we want it this way!
            self._rup_log.exception('abort')
            self._rup_msg_send(repr(e))
            sys.stderr.write('work error in %s: %s\n' % (self._rup_name, repr(e)))
            sys.stderr.flush()


        try:
            # note that we always try to call the finalizers, even if an
            # exception got raised during initialization or in the work loop
            # initializers failed for some reason or the other...
            self._rup_finalize()

        except BaseException as e:
            self._rup_log.exception('finalization error')
            self._rup_msg_send('finalize(): %s' % repr(e))
            sys.stderr.write('finalize error in %s: %s\n' % (self._rup_name, repr(e)))
            sys.stderr.flush()

        self._rup_msg_send('terminating')

        # tear down child watcher
        if self._rup_watcher:
            self._rup_term.set()
            self._rup_watcher.join(_STOP_TIMEOUT)

        # stop all things we watch
        with self._rup_things_lock:
            for thing in self._rup_things:
                try:
                    thing.stop(timeout=_STOP_TIMEOUT)
                except Exception as e:
                    self._rup_log.exception('could not stop %s [%s]', thing.name, e)

        # All watchables should have stopped.  For some of them,
        # `stop()` will have implied `join()` already - but an additional
        # `join()` will cause little overhead, so we don't bother
        # distinguishing.

        with self._rup_things_lock:
            for thing in self._rup_things:
                try:
                    thing.join(timeout=_STOP_TIMEOUT)
                except Exception as e:
                    self._rup_log.exception('could not join %s [%s]', thing.name, e)

        # all is done and said - begone!
        sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=_STOP_TIMEOUT):
        '''
        `stop()` is symetric to `start()`, in that it can only be called by the
        parent process.  It MUST be called from the main thread.  Both
        conditions are asserted.  If a subthread or the child needs to trigger
        the termination of the parent, it should simply terminate/exit its own
        scope, and let the parent detect that via its watcher thread.  That will
        eventually cause `is_alive()` to return `False`, signalling the
        application that `stop()` should be called.

        This method sets the thread termination signal, and call `stop()` on all
        watchables.  It then calls `join()` on all watchables, with the given
        timeout, and then also joins the child process with the given timeout.

        If any of the join calls fails or times out, an exception will be
        raised.  All join's will be attempted though, to collect as many threads
        and processes as possible.


        NOTE: `stop()` implies `join()`!  Use `terminate()` if that is not
              wanted.  Terminate will not stop watchables though.

        NOTE: The given timeout is, as described above, applied multiple times,
        once for the child process, and once for each watchable.
        '''

        # FIXME: we can't really use destructors to make sure stop() is called,
        #        but we should consider using `__enter__`/`__leave__` scopes to
        #        ensure clean termination.

        # FIXME: This method should reduce to 
        #           self.terminate(timeout)
        #           self.join(timeout)
        #        ie., we should move some parts to `terminate()`.

        assert(self._rup_is_parent)
        assert(is_main_thread())

        self._rup_log.info('parent stops child')

        # keep a list of error messages for an eventual exception message
        errors = list()

        # call parent finalizers
        self._rup_finalize()

        # tear down watcher - we wait for it to shut down, to avoid races
        if self._rup_watcher:
            self._rup_term.set()
            self._rup_watcher.join(timeout)

        # stop all things we watch
        with self._rup_things_lock:
            for thing in self._rup_things:
                try:
                    thing.stop(timeout=timeout)
                except Exception as e:
                    errors.append('could not stop %s [%s]' % (thing.name, e))

        # stopping the watcher will close the socket, and the child should begin
        # termination immediately.  Well, whenever it realizes the socket is
        # gone, really.  We wait for that termination to complete.
        super(Process, self).join(timeout)

        # make sure child is gone
        if super(Process, self).is_alive():
            self._rup_log.warn('failed to stop child - terminate')
            self.terminate()  # hard kill
            super(Process, self).join(timeout)

        # check again
        if super(Process, self).is_alive():
            errors.append('could not join child process %s' % self.pid)

        # meanwhile, all watchables should have stopped, too.  For some of them,
        # `stop()` will have implied `join()` already - but an additional
        # `join()` will cause little overhead, so we don't bother
        # distinguishing.
        with self._rup_things_lock:
            for thing in self._rup_things:
                try:
                    thing.join(timeout=timeout)
                except Exception as e:
                    errors.append('could not join %s [%s]' % (thing.name, e))

        # don't exit - but signal if the child or any watchables survived
        if errors:
            for error in errors:
                self._rup_log.error(error)
            raise RuntimeError(errors)


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=_STOP_TIMEOUT):

      # raise RuntimeError('call stop instead!')
      #
      # we can't really raise the exception above, as the mp module calls this
      # join via `at_exit`.  Which kind of explains hangs on unterminated
      # children...
      #
      # FIXME: not that `join()` w/o `stop()` will not call the parent finalizers.  
      #        We should call those in both cases, but only once.
        super(Process, self).join(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def _rup_initialize(self):
        '''
        Perform basic settings, then call common and parent/child initializers.
        '''

        try:
            # call parent and child initializers, respectively
            if self._rup_is_parent:
                self._rup_initialize_common()
                self._rup_initialize_parent()

                self.initialize_common()
                self.initialize_parent()

            elif self._rup_is_child:
                self._rup_initialize_common()
                self._rup_initialize_child()

                self.initialize_common()
                self.initialize_child()

        except Exception as e:
            self._rup_log.exception('initialization error')
            raise RuntimeError('initialize: %s' % repr(e))


    # --------------------------------------------------------------------------
    #
    def _rup_initialize_common(self):

        self._rup_things      = list()
        self._rup_things_lock = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def _rup_initialize_parent(self):

        if not self._rup_spawned:
            # no child, so we won't need a watcher either
            return

        # Start a separate thread which watches our end of the socket.  If that
        # thread detects any failure on that socket, it will set
        # `self._rup_term`, to signal its demise and prompt an exception from
        # the main thread.  
        #
        # NOTE: For several reasons, the watcher thread has no valid/stable
        #       means of directly signaling the main thread of any error
        #       conditions, it is thus necessary to manually check the child
        #       state from time to time, via `self.is_alive()`.
        #
        # NOTE: https://bugs.python.org/issue1856  (01/2008)
        #       `sys.exit()` can segfault Python if daemon threads are active.
        #       https://bugs.python.org/issue21963 (07/2014)
        #       This will not be fixed in python 2.x.
        #
        #       We make the Watcher thread a daemon anyway:
        #
        #       - when `sys.exit()` is called in a child process, we don't care
        #         about the process anymore anyway, and all terminfo data are
        #         sent to the parent anyway.
        #       - when `sys.exit()` is called in the parent on unclean shutdown,
        #         the same holds.
        #       - when `sys.exit()` is called in the parent on clean shutdown,
        #         then the watcher threads should already be terminated when the
        #         `sys.exit()` invocation happens
        #
        # FIXME: check the conditions above
        #
        # FIXME: move to _rup_initialize_common
        #
        self._rup_term    = mt.Event()
        self._rup_watcher = mt.Thread(target=self._rup_watch)
      # self._rup_watcher.daemon = True
        self._rup_watcher.start()

        self._rup_log.info('child is alive')


    # --------------------------------------------------------------------------
    #
    def _rup_initialize_child(self):

        # TODO: should we also get an alive from parent?
        #
        # FIXME: move to _rup_initialize_common
        #

        # start the watcher thread
        self._rup_term    = mt.Event()
        self._rup_watcher = mt.Thread(target=self._rup_watch)
      # self._rup_watcher.daemon = True
        self._rup_watcher.start()

        self._rup_log.info('child (me) is alive')


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

        self._rup_log.debug('initialize_parent (NOOP)')


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
        Call common and parent/child initializers.  
        
        Note that finalizers are called in inverse order of initializers.
        '''

        try:
            # call parent and child finalizers, respectively
            if self._rup_is_parent:
                self.finalize_parent()
                self.finalize_common()

                self._rup_finalize_parent()
                self._rup_finalize_common()

            elif self._rup_is_child:
                self.finalize_child()
                self.finalize_common()

                self._rup_finalize_child()
                self._rup_finalize_common()

        except Exception as e:
            self._rup_log.exception('finalization error')
            raise RuntimeError('finalize: %s' % repr(e))


    # --------------------------------------------------------------------------
    #
    def _rup_finalize_common(self):
    
        pass


    # --------------------------------------------------------------------------
    #
    def _rup_finalize_parent(self):
    
        pass


    # --------------------------------------------------------------------------
    #
    def _rup_finalize_child(self):
    
        pass


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

        The overloaded method MUST return `True` or `False` -- the child will
        continue to work upon `True`, and otherwise (on `False`) begin
        termination.
        '''

        raise NotImplementedError('ru.Process.work() MUST be overloaded')


    # --------------------------------------------------------------------------
    #
    def is_alive(self):

        return super(Process, self).is_alive()


    # --------------------------------------------------------------------------
    #
    def _parent_is_alive(self):
        '''
        This private method checks if the parent process is still alive.  This
        obviously only makes sense when being called in the child process.

        Note that there exists a (however unlikely) race: PIDs are reused, and
        the process could be replaced by another process with the same PID
        inbetween tests.  We thus also except *all* exception, including
        permission errors, to capture at least some of those races.
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


# ------------------------------------------------------------------------------

