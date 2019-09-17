
import sys
import os
import time
import atexit
import signal
import multiprocessing as mp

# from
# http://stackoverflow.com/questions/1417631/python-code-to-daemonize-a-process

# pylint: disable=try-except-raise


# ------------------------------------------------------------------------------
#
def daemonize(main, stdout=None, stderr=None, stdin=None, timeout=None):
    '''
    Create a damon process and run the given method in it.   For that, do the
    UNIX double-fork magic, see Stevens' "Advanced Programming in the UNIX
    Environment" for details (ISBN 0201563177)

    The method will return the PID of the spawned damon process, or `None` on
    failure to create it.  stdout, stderr, stdin file names are interpreted by
    the daemon process, and are expected to be path names which can be opened
    and read / written in their respective capabilities.
    '''

    assert(callable(main))

    pid   = None
    queue = mp.Queue()

    # first fork
    try:
        f1_pid = os.fork()
        if  f1_pid > 0:

            # wait for daemon pid from second parent
            # TODO: timeout
            if timeout:
                try:
                    pid = queue.get(timeout=timeout)
                except Queue.Empty:
                    raise TimeoutError('daemon startup timed out')

            else:
                pid = queue.get()

            if not pid:
                raise RuntimeError('daemon startup failed')

            # we are done...
            return pid

    except OSError as e:
        raise RuntimeError("Fork failed: %d (%s)\n"
                          % (e.errno, e.strerror))

    except Exception as e:
        raise RuntimeError("Failed to start daemon: %d (%s)\n"
                          % (e.errno, e.strerror))


    # decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # second fork
    try:
        f2_pid = os.fork()

        if  f2_pid > 0:
            # communicate pid to first parent
            queue.put(f2_pid)

            # exit from second parent
            sys.exit(0)

    except OSError as e:
        queue.put(None)  # unblock parent
        sys.exit(1)

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    if stdin:
        si = open(stdin, 'r')
        os.dup2(si.fileno(), sys.stdin.fileno())

    if stdout:
        so = open(stdout, 'a+')
        os.dup2(so.fileno(), sys.stdout.fileno())

    if stderr:
        se = open(stderr, 'a+')
        os.dup2(se.fileno(), sys.stderr.fileno())

    # we are successfully daemonized - run the workload
    main()

    # work is done
    sys.exit(0)


# ------------------------------------------------------------------------------
#
class Daemon(object):
    '''
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, stdin='/dev/null',
                       stdout='/dev/null',
                       stderr='/dev/null',
                       target=None):

        if target:
            assert(callable(target))

        self.stdin   = stdin
        self.stdout  = stdout
        self.stderr  = stderr
        self.pid     = None
        self.queue   = mp.Queue()
        self.target  = target


    # --------------------------------------------------------------------------
    #
    def start(self):

        # start the daemon, and in the demon process, run the workload
        self.pid = daemonize(main=self.run, stdin=self.stdin,
                                            stdout=self.stdout,
                                            stderr=self.stderr)
        return self.pid


    # --------------------------------------------------------------------------
    #
    def stop(self, pid=None):
        '''
        Stop the daemon.  If a pid is passed, then stop the daeon process with
        that pid.
        '''

        if not pid:
            pid = self.pid

        if not pid:
            raise RuntimeError('no pid - daemon not started, yet?')

        # Try killing the daemon process
        os.kill(pid, signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def restart(self, pid=None):
        '''
        Stop the daemon and restart it.  If a pid is passed, then stop the daeon
        process with that pid and replace it with a daemon process represented
        by this class instance.  It is the caller's responsibility to ensure
        that this makes semantic sense.

        This method returns the pid of the new daemon process (see `start()`).
        '''

        self.stop(pid=pid)
        return self.start()


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        You should override this method when you subclass Daemon and do not pass
        `target` to the class constructor.  This method will be called after the
        daemon process has been created by start() or restart().
        '''

        if self.target:
            self.target()

        else:
            raise NotImplementedError("daemon workload undefined")


# ------------------------------------------------------------------------------

