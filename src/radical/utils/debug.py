
import os
import sys
import time
import pprint
import signal
import threading
import traceback


# --------------------------------------------------------------------
#
def get_trace():

    trace = sys.exc_info ()[2]

    if  trace :
        stack           = traceback.extract_tb  (trace)
        traceback_list  = traceback.format_list (stack)
        return "".join (traceback_list)

    else :
        stack           = traceback.extract_stack ()
        traceback_list  = traceback.format_list (stack)
        return "".join (traceback_list[:-1])


# ------------------------------------------------------------------------------
#
class DebugHelper (object) :
    """
    When instantiated, and when "RADICAL_DEBUG" is set in the environment, this
    class will install a signal handler for SIGUSR1.  When that signal is
    received, a stacktrace for all threads is printed to stdout.
    We also check if SIGINFO is available, which is generally bound to CTRL-T.

    Additionally, a call to 'dh.fs_block(info=None)' will create a file system
    based barrier: it will create a unique file in /tmp/ (based on 'name' if
    given), and dump the stack trace and any 'info' into it.  It then waits
    until that file has changed (touched or removed etc), and then returns.  The
    wait is a simple pull based 'os.stat()' (once per sec).
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, name=None, info=None):
        """
        name: string to identify fs barriers
        info: static info to dump into fs barriers
        """

        self._name       = name
        self._info       = info

        if not self._name:
            self._name = str(id(self))

        if 'MainThread' not in threading.current_thread().name:
            # python only supports signals in main threads :/
            return

        if 'RADICAL_DEBUG' in os.environ :
            signal.signal(signal.SIGUSR1, print_stacktraces) # signum 30
            signal.signal(signal.SIGQUIT, print_stacktraces) # signum  3

            try:
                assert signal.SIGINFO
                signal.signal(signal.SIGINFO, print_stacktraces) # signum 29
            except AttributeError as e:
                pass


    # --------------------------------------------------------------------------
    #
    def fs_block(self, info=None):
        """
        Dump state, info in barrier file, and wait for it tou be touched or 
        read or removed, then continue.  Leave no trace.
        """

        try:
            pid = os.getpid()
            tid = threading.currentThread().ident

            fs_barrier = "/tmp/ru.dh.%s.%s.%s" % (self._name, pid, tid)
            fs_handle  = open(fs_barrier, 'w+')
            fs_handle.seek(0,0)
            fs_handle.write("\nSTACK TRACE:\n%s\n%s\n" % (time.time(), get_trace()))
            fs_handle.write("\nSTATIC INFO:\n%s\n\n" % pprint.pformat(self._info))
            fs_handle.write("\nINFO:\n%s\n\n" % pprint.pformat(info))
            fs_handle.flush()

            new = os.stat(fs_barrier)
            old = new

            while old == new:
                new = os.stat(fs_barrier)
                time.sleep(1)

            fs_handle.close()
            os.unlink(fs_barrier)

        except Exception as e:
            # we don't care (much)...
            print get_trace()
            print e
            pass


# ------------------------------------------------------------------------------
#
def print_stacktraces (signum=None, sigframe=None) :
    """
    signum, sigframe exist to satisfy signal handler signature requirements
    """

    this_tid = threading.currentThread().ident

    # if multiple processes (ie. a process group) get the signal, then all
    # traces are mixed together.  Thus we waid 'pid%100' milliseconds, in
    # the hope that this will stagger the prints.
    pid = int(os.getpid())
    time.sleep((pid%100)/1000)

    out  = "===============================================================\n"
    out += "RADICAL Utils -- Debug Helper -- Stacktraces\n"
    out += os.popen('ps -efw --forest | grep " %s " | grep -v grep' % pid).read() 
    try :
        info = get_stacktraces ()
    except Exception as e:
        out += 'skipping frame (%s)' % e
        info = None

    if info:
        for tid, tname in info :

            if tid == this_tid : marker = '[active]'
            else               : marker = ''
            out += "---------------------------------------------------------------\n"
            out += "Thread: %s %s\n" % (tname, marker)
            out += "  PID : %s \n"   % os.getpid()
            out += "  TID : %s \n"   % tid
            for fname, lineno, method, code in info[tid,tname] :

                if code: code = code.strip()
                else   : code = '<no code>'

              # # [:-1]: .py vs. .pyc :/
              # if not (__file__[:-1] in fname and \
              #         method in ['get_stacktraces', 'print_stacktraces']):
                if method not in ['get_stacktraces', 'print_stacktraces']:
                    out += "  File: %s, line %d, in %s\n" % (fname, lineno, method)
                    out += "        %s\n" % code

    out += "==============================================================="

    sys.stdout.write("%s\n" % out)

    if 'RADICAL_DEBUG' in os.environ:
        with open('/tmp/ru.stacktrace.%s.log' % pid, 'w') as f:
            f.write ("%s\n" % out)

    return True


# --------------------------------------------------------------------------
#
def get_stacktraces () :

    id2name = {}
    for th in threading.enumerate():
        id2name[th.ident] = th.name

    ret = dict()
    for tid, stack in sys._current_frames().items():
        if tid in id2name:
            ret[tid,id2name[tid]] = traceback.extract_stack(stack)
        else:
            ret[tid,'noname'] = traceback.extract_stack(stack)

    return ret


