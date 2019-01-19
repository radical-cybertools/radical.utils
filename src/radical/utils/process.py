
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import time
import signal
import socket
import threading       as mt
import multiprocessing as mp
import setproctitle    as spt

from .logger  import Logger
from .threads import Thread as ru_Thread
from .        import poll   as ru_poll


# ------------------------------------------------------------------------------
#
_ALIVE_MSG     = 'alive'  # message to use as alive signal
_START_TIMEOUT = 20.0     # time to wait for process startup signal.
                          # startup signal: 'alive' message on the socketpair;
                          # is sent in both directions to ensure correct setup
_WATCH_TIMEOUT = 0.2      # time between thread and process health polls.
                          # health poll: check for recv, error and abort
                          # on the socketpair; is done in a watcher thread.
_STOP_TIMEOUT  = 2.0      # time between temination signal and killing child
_BUFSIZE       = 1024     # default buffer size for socket recvs


# ------------------------------------------------------------------------------
#
def pid_watcher(pid=None, tgt=None, timeout=0.1, sig=None, uid=None):
    '''
    Watch the given `pid` in a separate thread, and if it disappears, kill the
    process with `tgt`, too.  The spwned thread is a daemon thread and does
    not need joining - it will disappear once it has done its job, or once the
    parent process dies - whichever happens first.

    We use `kill(pid, 0)` to test watched process' health, which will raise an
    exception if no process with `pid` exists, and otherwise has no side
    effects.  While this has a race condition on PID reuse, the advantages
    outweight that slim rarcing probability which has only the timout window to
    happen (which is unlikely even on a very busy system with limited PID
    range):
      - very lightweigt with little runtime overhead;
      - does not need a cooperative process; and
      - does not suffer from Python level process management bugs.
    '''

    if not pid: pid = os.getppid()
    if not tgt: tgt = os.getpid()
    if not sig: sig = signal.SIGKILL
    if not uid: uid = '?'

    def _watch():

        try:
            while True:
              # sys.stderr.write('--- %s watches %s\n' % (uid, pid))
              # sys.stderr.flush()
                time.sleep(timeout)
                os.kill(pid, 0)

        except OSError:
          # sys.stderr.write('--- watcher for %s kills %s\n' % (pid, tgt))
          # sys.stderr.flush()
            os.kill(tgt, sig)

        except Exception as e:
          # sys.stderr.write('--- watcher for %s failed: %s\n' % (pid, e))
          # sys.stderr.flush()
            pass

    if pid == 1:
        sys.stderr.write('refuse to watch init process [1]\n')
        sys.stderr.flush()
        return
      # raise ValueError('refuse to watch init process [1]')

  # sys.stderr.write('--- %s [%s] watches %s n' % (uid, tgt, pid))
  # sys.stderr.flush()

    watcher = mt.Thread(target=_watch)
    watcher.daemon = True
    watcher.start()


# ------------------------------------------------------------------------------
#
class Process(mp.Process):
    '''
    This `Process` class is a thin wrapper around `mp.Process` which
    specifically manages the process lifetime in a more cautious and copperative
    way than the base class: *no* attempt on signal handling is made, we expect
    to exclusively communicate between parent and child via a socket.
    A separate thread in both processes will watch that socket: if the socket
    disappears, we interpret that as the other process being terminated, and
    begin process termination, too.

    NOTE: At this point we do not implement the full `mp.Process` constructor.

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

    Any class which derives from this Process class *must* overload the 'work_cb()`
    method.  That method is repeatedly called by the child process' main loop,
    until:
      - an exception occurs (causing the child to fail with an error)
      - `False` is returned by `work_cb()` (causing the child to finish w/o error)
    '''

    # TODO: We should switch to fork/*exec*, if possible.


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log=None):

        # At this point, we only initialize members which we need before start
        self._ru_name      = name  # use for setproctitle
        self._ru_childname = self._ru_name + '.child'

        # make sure we have a valid logger
        self._ru_set_logger(log)

        # ... or are *shared* between parent and child process.
        self._ru_ppid = os.getpid()    # get parent process ID

        # most importantly, we create a socketpair.  Both parent and child will
        # watch one end of that socket, which thus acts as a lifeline between
        # the processes, to detect abnormal termination in the process tree.
        # The socket is also used to send messages back and forth.
        self._ru_sp = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)

        # note that some members are only really initialized when calling the
        # `start()`/`run()` methods, and the various initializers.
        self._ru_spawned     = None           # set in start(), run()
        self._ru_is_parent   = None           # set in start()
        self._ru_is_child    = None           # set in run()
        self._ru_endpoint    = None           # socket endpoint for sent/recv
        self._ru_term        = None           # set to terminate watcher
        self._ru_initialized = False          # set to signal bootstrap success
        self._ru_terminating = False          # set to signal active termination
        self._ru_watcher     = None           # watcher thread
        self._ru_things_lock = mt.Lock()      # lock for the above
        self._ru_things      = dict()         # registry of threads created in
                                              # (parent or child) process

        # FIXME: assert that start() was called for some / most methods

        # when cprofile is requested but not available,
        # we complain, but continue unprofiled
        self._ru_cprofile = False
        if self._ru_name in os.environ.get('RADICAL_CPROFILE', '').split(':'):
            try:
                self._ru_log.error('enable cprofile for %s', self._ru_name)
                import cprofile
                self._ru_cprofile = True
            except:
                self._ru_log.error('cannot import cprofile - disable')

        # base class initialization
        super(Process, self).__init__(name=self._ru_name)

        # we don't want processes to linger around, waiting for children to
        # terminate, and thus create sub-processes as daemons.
        #
      # NOTE: this requires the `patchy.mc_patchface()` fix in `start()`.
      #
      # self.daemon = True


    # --------------------------------------------------------------------------
    #
    @property
    def ru_name(self):
        return self._ru_name


    # --------------------------------------------------------------------------
    #
    @property
    def ru_childname(self):
        return self._ru_childname


    # --------------------------------------------------------------------------
    #
    def _ru_set_logger(self, log=None):

        self._ru_log = log

        if not self._ru_log:
            # if no logger is passed down, log to null (FIXME)
            self._ru_log = Logger('radical.util.process')
            self._ru_log.debug('name: %s' % self._ru_name)


    # --------------------------------------------------------------------------
    #
    def _ru_msg_send(self, msg):
        '''
        send new message to self._ru_endpoint.  We make sure that the
        message is not larger than the _BUFSIZE define in the RU source code.
        '''

        if not self._ru_spawned:
            # no child, no communication channel
            info = "%s\ncan't communicate w/o child %s -> %s [%s]" \
                    % (msg, os.getpid(), self.pid, self._ru_name)
            raise RuntimeError(info)

        # NOTE:  this method should only be called by the watcher thread, which
        #        owns the endpoint.
        # FIXME: BUFSIZE should not be hardcoded
        # FIXME: check for socket health

        if len(msg) > _BUFSIZE:
            raise ValueError('message is larger than %s: %s' % (_BUFSIZE, msg))

        self._ru_log.info('send message: [%s] %s', self._ru_name, msg)
        try:
            self._ru_endpoint.send(msg)
        except Exception as e:
            # this should only happen once the EP is done for - terminate
            self._ru_log.warn('send failed (%s) - terminate', e)
            if self._ru_term is not None:
                self._ru_term.set()



    # --------------------------------------------------------------------------
    #
    def _ru_msg_recv(self, size=_BUFSIZE, timeout=None):
        '''
        receive a message from self._ru_endpoint.  We only check for messages
        of *up to* `size`.

        This call is non-blocking: if no message is available, return an empty
        string.
        '''

        if not self._ru_spawned:
            # no child, no communication channel
            raise RuntimeError("can't communicate w/o child")

        # NOTE:  this method should only be called by the watcher thread, which
        #        owns the endpoint (no lock used!)
        # FIXME: BUFSIZE should not be hardcoded
        # FIXME: check for socket health

        try:
            if timeout:
                self._ru_endpoint.settimeout(timeout)
            msg = self._ru_endpoint.recv(size)
            self._ru_log.info('recv message: %s', msg)
            return msg

        except socket.timeout:
            self._ru_log.warn('recv timed out')

        except Exception as e:
            # this should only happen once the EP is done for - terminate
            self._ru_log.warn('recv failed (%s) - terminate', e)
            self._ru_term.set()


    # --------------------------------------------------------------------------
    #
    def _ru_watch(self):
        '''
        When `start()` is called, the parent process will create a socket pair.
        after fork, one end of that pair will be watched by the parent and
        client, respectively, in separate watcher threads.  If any error
        condition or hangup is detected on the socket, it is assumed that the
        process on the other end died, and termination is initiated.

        Since the watch happens in a subthread, any termination requires the
        attention and cooperation of the main thread.  No attempt is made on
        interrupting the main thread, we only set self._ru_term which needs to
        be checked by the main threads in certain intervals.
        '''

        # we watch sockets and threads as long as we live, ie. until the main
        # thread sets `self._ru_term`.
        try:

            self._ru_poller = ru_poll.poll(logger=self._ru_log)
            self._ru_poller.register(self._ru_endpoint,
                    ru_poll.POLLIN  | ru_poll.POLLERR | ru_poll.POLLHUP)
                 #  ru_poll.POLLPRI | ru_poll.POLLNVAL)

            last = 0.0  # we never watched anything until now
            while not self._ru_term.is_set() :

                # only do any watching if time is up
                now = time.time()
                if now - last < _WATCH_TIMEOUT:
                    time.sleep(0.1)  # FIXME: configurable, load tradeoff
                    continue

                if  not self._ru_watch_socket() or \
                    not self._ru_watch_things()    :
                    return

                last = now

                # FIXME: also *send* any pending messages to the child.
              # # check if any messages need to be sent.
              # while True:
              #     try:
              #         msg = self._ru_msg_out.get_nowait()
              #         self._ru_msg_send(msg)
              #
              #     except Queue.Empty:
              #         # nothing more to send
              #         break


        except Exception as e:
            # mayday... mayday...
            self._ru_log.exception('watcher failed')

        finally:
            # no matter why we fell out of the loop: let the other end of the
            # socket know by closing the socket endpoint.

            # Fix radical-cybertools/radical.utils issue #120
            # We need to call SHUTDOWN before we close the socket...
            # From: https://docs.python.org/2/howto/sockets.html#disconnecting
            self._ru_log.info('watcher closes')
            self._ru_endpoint.shutdown(socket.SHUT_RDWR)
            self._ru_endpoint.close()
            self._ru_poller.close()

            # `self.stop()` will be called from the main thread upon checking
            # `self._ru_term` via `self.is_alive()`.
            # FIXME: check
            self._ru_term.set()


    # --------------------------------------------------------------------------
    #
    def _ru_watch_socket(self):

        # check health of parent/child relationship
        events = self._ru_poller.poll(0.1)   # block just a little
        for _,event in events:

            # for alive checks, we poll socket state for
            #   * data:   some message from the other end, logged
            #   * error:  child failed   - terminate
            #   * hangup: child finished - terminate

            # check for messages
            if event & ru_poll.POLLIN:

                # we get a message!
                #
                # FIXME: BUFSIZE should not be hardcoded
                # FIXME: we do nothing with the message yet, should be
                #        stored in a message queue.

                msg = self._ru_msg_recv(_BUFSIZE)
                self._ru_log.info('message received: %s' % msg)

                if msg in [None, '']:
                    self._ru_log.warn('no message, parent closed ep!')
                    return False

                elif msg.strip() == 'STOP':
                    self._ru_log.info('STOP received: %s' % msg)
                    return False

            # check for error conditions
            if  event & ru_poll.POLLHUP or  \
                event & ru_poll.POLLERR     :

                # something happened on the other end, we are about to die
                # out of solidarity (or panic?).
                self._ru_log.warn('endpoint disappeard')
                return False

          # if event & ru_poll.POLLPRI:
          #     self._ru_log.info('POLLPRI : %s', self._ru_endpoint.fileno())
          # if event & ru_poll.POLLNVAL:
          #     self._ru_log.info('POLLNVAL: %s', self._ru_endpoint.fileno())

        time.sleep(0.1)

      # self._ru_log.debug('endpoint watch ok')
        return True


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

        with self._ru_things_lock:
            if thing.name in self._ru_things:
                raise ValueError('already watching %s' % thing.name)
            self._ru_things[thing.name] = thing


    # --------------------------------------------------------------------------
    #
    def unregister_watchable(self, name):

        with self._ru_things_lock:
            if name not in self._ru_things:
                raise ValueError('%s is not watched' % name)

            del(self._ru_things[name])


    # --------------------------------------------------------------------------
    #
    def _ru_watch_things(self):

        with self._ru_things_lock:
            for tname,thing in self._ru_things.iteritems():
                if not thing.is_alive():
                    self._ru_log.warn('%s died')
                    return False

        return True


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True, timeout=None):
        '''
        Overload the `mp.Process.start()` method, and block (with timeout) until
        the child signals to be alive via a message over our socket pair.  Also
        run all initializers.

        If spawn is set to `False`, then no child process is actually created,
        but the parent initializers will be executed nonetheless.

        '''

        self._ru_log.debug('start process')

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
            self._ru_spawned = True


        # this is the parent now - set role flags.
        self._ru_is_parent = True
        self._ru_is_child  = False

        # select different ends of the socketpair for further communication.
        self._ru_endpoint  = self._ru_sp[0]
        self._ru_sp[1].close()

        # from now on we need to invoke `self.stop()` for clean termination.
        # Having said that: the daemonic watcher thread and the socket lifeline
        # to the child should ensure that both will terminate in all cases, but
        # possibly somewhat delayed and abruptly.
        #
        # Either way: use a try/except to ensure `stop()` being called.
        try:

            if self._ru_spawned:
                # we expect an alive message message from the child, within
                # timeout
                #
                # NOTE: If the child does not bootstrap fast enough, the timeout
                #       will kick in, and the child will be considered dead,
                #       failed and/or hung, and will be terminated!  Timeout can
                #       be set as parameter to the `start()` method.
                msg = self._ru_msg_recv(size=len(_ALIVE_MSG), timeout=timeout)

                if not msg:
                    raise RuntimeError('child %s failed to come up [%ss]' %
                                       (self._ru_childname, msg))

                elif msg != _ALIVE_MSG:
                    # attempt to read remainder of message and barf
                    msg += self._ru_msg_recv()
                    raise RuntimeError('%s got unexpected message (%s) [%s]' % 
                                       (self._ru_name, msg, timeout))


            # When we got the alive messages, only then will we call the parent
            # initializers.  This way those initializers can make some
            # assumptions about successful child startup.
            self._ru_initialize()

        except Exception as e:
            self._ru_log.exception('initialization failed (%s)' % e)
            self.stop()
            raise

        # if we got this far, then all is well, we are done.
        if self._ru_spawned:
            self._ru_log.debug('process class started child')
        else:
            self._ru_log.debug('process class started (no child)')

        # child is alive and initialized, parent is initialized, watcher thread
        # is started - Wohoo!


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This method MUST NOT be overloaded!

        This is the workload of the child process.  It will first call the child
        initializers and then repeatedly call `self.work_cb()`, until being
        terminated.  When terminated, it will call the child finalizers and
        exit.  Note that the child will also terminate once `work_cb()` returns
        `False`.

        The implementation of `work_cb()` needs to make sure that this process is
        not spinning idly -- if there is nothing to do in `work_cb()` at any point
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

        # if no profiling is wanted, we just run the workload and exit
        if not self._ru_cprofile:
            self._run()

        # otherwise we run under the profiler, obviously
        else:
            import cprofile
            cprofiler = cprofile.Profile()
            cprofiler.runcall(self._run)
            cprofiler.dump_stats('%s.cprof' % (self._ru_name))


    # --------------------------------------------------------------------------
    #
    def _run(self):

        # FIXME: ensure that this is not overloaded

        # this is the child now - set role flags
        self._ru_is_parent = False
        self._ru_is_child  = True
        self._ru_spawned   = True

        # select different ends of the socketpair for further communication.
        self._ru_endpoint  = self._ru_sp[1]
        self._ru_sp[0].close()

        # set child name based on name given in c'tor, and use as proctitle
        self._ru_name = self._ru_childname
        spt.setproctitle(self._ru_name)

        try:
            # we consider the invocation of the child initializers to be part of
            # the bootstrap process, which includes starting the watcher thread
            # to watch the parent's health (via the socket health).
            try:
                self._ru_initialize()

            except BaseException as e:
                self._ru_log.exception('abort: %s', repr(e))
                self._ru_msg_send('error: %s' % repr(e))
              # sys.stderr.write('initialization error in %s: %s\n' % (self._ru_name, repr(e)))
              # sys.stderr.flush()
                if self._ru_term is not None:
                    self._ru_term.set()

            # initialization done - we only now send the alive signal, so the
            # parent can make some assumptions about the child's state
            if not self._ru_term.is_set():
                self._ru_log.info('send alive')
                self._ru_msg_send(_ALIVE_MSG)

            # enter the main loop and repeatedly call 'work_cb()'.
            #
            # If `work_cb()` ever returns `False`, we break out of the loop to call the
            # finalizers and terminate.
            #
            # In each iteration, we also check if the socket is still open -- if it
            # is closed, we assume the parent to be dead and terminate (break the
            # loop).  We consider the socket closed if `self._ru_term` was set
            # by the watcher thread.
            while not self._ru_term.is_set() and \
                      self._parent_is_alive()    :

                # des Pudel's Kern
                if not self.work_cb():
                    self._ru_msg_send('work finished')
                    break

        except BaseException as e:

            # This is a very global except, also catches
            # sys.exit(), keyboard interrupts, etc.
            # Ignore pylint and PEP-8, we want it this way!
            self._ru_log.exception('abort: %s', repr(e))
            try:
                self._ru_msg_send('abort: %s' % repr(e))
            except Exception as e:
                self._ru_log.exception('abort info not sent: %s', repr(e))

        try:
            # note that we always try to call the finalizers, even if an
            # exception got raised during initialization or in the work loop
            # initializers failed for some reason or the other...
            self._ru_finalize()

        except BaseException as e:
            self._ru_log.error('finalization error: %s' % repr(e))
            try:
                self._ru_msg_send('finalize: %s' % repr(e))
            except Exception as e2:
                self._ru_log.exception('finalize error not sent: %s', repr(e2))

        try:
            self._ru_msg_send('terminating')
        except Exception as e:
            self._ru_log.warn('term msg error not sent: %s', repr(e))

        # tear down child watcher
        if self._ru_watcher:
            self._ru_term.set()
            self._ru_watcher.join(_STOP_TIMEOUT)

        # stop all things we watch
        with self._ru_things_lock:
            for tname,thing in self._ru_things.iteritems():
                try:
                    thing.stop(timeout=_STOP_TIMEOUT)
                except Exception as e:
                    self._ru_log.exception('Could not stop %s [%s]', tname, e)

        # All watchables should have stopped.  For some of them,
        # `stop()` will have implied `join()` already - but an additional
        # `join()` will cause little overhead, so we don't bother
        # distinguishing.

        with self._ru_things_lock:
            for tname,thing in self._ru_things.iteritems():
                try:
                    thing.join(timeout=_STOP_TIMEOUT)
                except Exception as e:
                    self._ru_log.exception('3 could not join %s [%s]', tname, e)

        # all is done and said - begone!
        sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):
        '''
        `stop()` is symmetric to `start()`, in that it can only be called by the
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

        if not timeout:
            timeout = _STOP_TIMEOUT

        # make sure we don't recurse
        if self._ru_terminating:
            return
        self._ru_terminating = True

      # if not is_main_thread():
      #     self._ru_log.info('reroute stop to main thread (%s)' % self._ru_name)
      #     sys.exit()

        self._ru_log.info('parent stops child  %s -> %s [%s]', os.getpid(),
                self.pid, self._ru_name)
        if self._ru_spawned:
            self._ru_msg_send('STOP')

        # keep a list of error messages for an eventual exception message
        errors = list()

        # call parent finalizers
        self._ru_finalize()

        # tear down watcher - we wait for it to shut down, to avoid races
        if self._ru_watcher:
            self._ru_term.set()
            self._ru_watcher.stop(timeout)

        # stop all things we watch
        with self._ru_things_lock:
            for tname,thing in self._ru_things.iteritems():
                try:
                    thing.stop(timeout=timeout)
                except Exception as e:
                    errors.append('could not stop %s [%s]' % (tname, e))

        # process termination is only relevant for the parent, and only if
        # a child was actually spawned
        if self._ru_is_parent and self._ru_spawned:

            # stopping the watcher will close the socket, and the child should
            # begin termination immediately.  Well, whenever it realizes the
            # socket is gone, really.  We wait for that termination to complete.
            super(Process, self).join(timeout)

            # make sure child is gone
            if super(Process, self).is_alive():
                self._ru_log.warn('failed to stop child - terminate: %s -> %s [%s]', os.getpid(), self.pid, self._ru_name)
                self.terminate()  # hard kill
                super(Process, self).join(timeout)

          # # check again
          # if super(Process, self).is_alive():
          #     # we threat join errors as non-fatal here - at this point, there
          #     # is not much we can do other than calling `terminate()#join()`
          #     # -- which is exactly what we just did.
          #     self._ru_log.warn('could not join child process %s', self.pid)

        # meanwhile, all watchables should have stopped, too.  For some of them,
        # `stop()` will have implied `join()` already - but an additional
        # `join()` will cause little overhead, so we don't bother
        # distinguishing.
        with self._ru_things_lock:
            for tname,thing in self._ru_things.iteritems():
                try:
                    thing.join(timeout=timeout)
                except Exception as e:
                    self._ru_log.exception('could not join %s [%s]', tname, e)
                    errors.append('could not join %s [%s]' % (tname, e))

        # don't exit - but signal if the child or any watchables survived
        if errors:
            for error in errors:
                self._ru_log.error(error)
            raise RuntimeError(errors)


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):

      # raise RuntimeError('call stop instead!')
      #
      # we can't really raise the exception above, as the mp module calls this
      # join via `at_exit`.  Which kind of explains hangs on unterminated
      # children...
      #
      # FIXME: not that `join()` w/o `stop()` will not call the parent finalizers.
      #        We should call those in both cases, but only once.
      # FIXME: `join()` should probably block by default

        if not timeout:
            timeout = _STOP_TIMEOUT

        assert(self._ru_is_parent)

        if self._ru_spawned:
            super(Process, self).join(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def _ru_initialize(self):
        '''
        Perform basic settings, then call common and parent/child initializers.
        '''

        try:
            # private initialization
            self._ru_term = mt.Event()

            # call parent and child initializers, respectively
            if self._ru_is_parent:
                self.ru_initialize_common()
                self.ru_initialize_parent()

                self._ru_initialize_common()
                self._ru_initialize_parent()

            elif self._ru_is_child:
                self.ru_initialize_common()
                self.ru_initialize_child()

                self._ru_initialize_common()
                self._ru_initialize_child()

            self._ru_initialized = True

        except Exception as e:
            self._ru_log.exception('initialization error')
            raise


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_common(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_parent(self):

        if not self._ru_spawned:
            # no child, so we won't need a watcher either
            return

        # Start a separate thread which watches our end of the socket.  If that
        # thread detects any failure on that socket, it will set
        # `self._ru_term`, to signal its demise and prompt an exception from
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
        # FIXME: move to _ru_initialize_common
        #
        self._ru_watcher = ru_Thread(name='%s.watch' % self._ru_name,
                                     target=self._ru_watch,
                                     log=self._ru_log)
        self._ru_watcher.start()

        self._ru_log.info('child is alive')


    # --------------------------------------------------------------------------
    #
    def _ru_initialize_child(self):

        # TODO: should we also get an alive from parent?
        #
        # FIXME: move to _ru_initialize_common
        #

        self._ru_log.info('child (me) initializing')

        # start the watcher thread
        self._ru_watcher = ru_Thread(name='%s.watch' % self._ru_name,
                                     target=self._ru_watch,
                                     log=self._ru_log)
        self._ru_watcher.start()

        self._ru_log.info('child (me) is alive')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_common(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, for both the parent and the child process (individually).  If
        this fails on either side, the process startup is considered failed.
        '''

        self._ru_log.debug('ru_initialize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the parent process.  If this fails, the process startup is
        considered failed.
        '''

        self._ru_log.debug('ru_initialize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_initialize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `start()`, in the child process.  If this fails, the process startup is
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
            # call parent and child finalizers, respectively
            if self._ru_is_parent:
                self.ru_finalize_parent()
                self.ru_finalize_common()

                self._ru_finalize_parent()
                self._ru_finalize_common()

            elif self._ru_is_child:
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
        `stop()` or process child termination, in the parent process, in both
        the parent and the child process (individually).
        '''

        self._ru_log.debug('ru_finalize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_finalize_parent(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or process child termination, in the parent process.
        '''

        self._ru_log.debug('ru_finalize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def ru_finalize_child(self):
        '''
        This method can be overloaded, and will then be executed *once* during
        `stop()` or process child termination, in the child process.
        '''

        self._ru_log.debug('ru_finalize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def work_cb(self):
        '''
        This method MUST be overloaded.  It represents the workload of the
        process, and will be called over and over again.

        This has several implications:

          * `work_cb()` needs to enforce any call rate limits on its own!
          * in order to terminate the child, `work_cb()` needs to either raise an
            exception, or call `sys.exit()` (which actually also raises an
            exception).

        Before the first invocation, `self.ru_initialize_child()` will be called.
        After the last invocation, `self.ru_finalize_child()` will be called, if
        possible.  The latter will not always be possible if the child is
        terminated by a signal, such as when the parent process calls
        `child.terminate()` -- `child.stop()` should be used instead.

        The overloaded method MUST return `True` or `False` -- the child will
        continue to work upon `True`, and otherwise (on `False`) begin
        termination.
        '''

        raise NotImplementedError('ru.Process.work_cb() MUST be overloaded')


    # --------------------------------------------------------------------------
    #
    def is_alive(self, strict=True):
        '''
        Check if the child process is still alive, and also ensure that
        termination is not yet initiated.  If `strict` is set (default), then
        only the process state is checked.
        '''

        if not self._ru_spawned:
            # its not an error if the child was never spawned.
            alive = True

        elif self._ru_is_child:
            # if this  *is* the child, then it's alive
            alive = True

        else:
            # this is the parent, and it spawned: check for real
            alive = super(Process, self).is_alive()
            if not alive:
                self._ru_log.warn('super: alive check failed [%s]', alive)

        if self._ru_term is None:
            # child is not yet started
            self._ru_log.warn('startup: alive check failed [%s]', alive)
            return False

        termed = self._ru_term.is_set()


        if strict: return alive
        else     : return alive and not termed


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
        sub-processes or threads and to then terminate.

        This method will always return True if `start()` was not called, yet.
        '''

        if self._ru_terminating or not self._ru_initialized:
            self._ru_log.debug('alive check in term')
            return True

        alive = self.is_alive(strict=False)

        if not alive and term:
            self._ru_log.warn('alive check: proc invalid - stop [%s - %s]', alive, term)
            self.stop()
        else:
            return alive


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

        assert(self._ru_is_child)

        try:
            os.kill(self._ru_ppid, 0)
            return True

        except:
            return False


# ------------------------------------------------------------------------------

