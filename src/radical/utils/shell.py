
import os
import queue
import shlex
import select

import threading  as mt
import subprocess as sp

from .constants import RUNNING, DONE, FAILED
from .misc      import is_string


# ------------------------------------------------------------------------------
#
def sh_callout(cmd, stdout=True, stderr=True, shell=False):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''

    # convert string into arg list if needed
    if not shell and is_string(cmd): cmd = shlex.split(cmd)

    if stdout: stdout = sp.PIPE
    else     : stdout = None

    if stderr: stderr = sp.PIPE
    else     : stderr = None

    p = sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell)

    if not stdout and not stderr:
        ret = p.wait()
    else:
        stdout, stderr = p.communicate()
        ret            = p.returncode
    return stdout.decode("utf-8"), stderr.decode("utf-8"), ret


# ------------------------------------------------------------------------------
#
def sh_callout_bg(cmd, stdout=None, stderr=None, shell=False):
    '''
    call a shell command in the background.  Do not attempt to pipe STDOUT/ERR,
    but only support writing to named files.
    '''

    # pipes won't work - see sh_callout_async
    if stdout == sp.PIPE: raise ValueError('stdout pipe unsupported')
    if stderr == sp.PIPE: raise ValueError('stderr pipe unsupported')

    # openfile descriptors for I/O, if needed
    if is_string(stdout): stdout = open(stdout, 'w')
    if is_string(stderr): stderr = open(stderr, 'w')

    # convert string into arg list if needed
    if not shell and is_string(cmd): cmd = shlex.split(cmd)

    sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell)

    return


# ------------------------------------------------------------------------------
#
def sh_callout_async(cmd, stdin=True, stdout=True, stderr=True, shell=False):
    '''

    Run a command, and capture stdout/stderr if so flagged.  The call will
    return an PROC object instance on which the captured output can be retrieved
    line by line (I/O is line buffered).  When the process is done, a `None`
    will be returned on the I/O queues.

    Line breaks are stripped.

    stdout/stderr: True [default], False, string
      - False : discard I/O
      - True  : capture I/O as queue [default]
      - string: capture I/O as queue, also write to named file

    shell: True, False [default]
      - pass to popen

    PROC:
      - PROC.stdout         : `queue.Queue` instance delivering stdout lines
      - PROC.stderr         : `queue.Queue` instance delivering stderr lines
      - PROC.state          : ru.RUNNING, ru.DONE, ru.FAILED
      - PROC.rc             : returncode (None while ru.RUNNING)
      - PROC.stdout_filename: name of stdout file (when available)
      - PROC.stderr_filename: name of stderr file (when available)
    '''
    # NOTE: Fucking python screws up stdio buffering when threads are used,
    #       *even if the treads do not perform stdio*.  Its possible that the
    #       logging module interfers, too.  Either way, I am fed up debugging
    #       this shit, and give up.  This method does not work for threaded
    #       python applications.
    assert(False), 'this is broken for python apps'

    # --------------------------------------------------------------------------
    class _P(object):
        '''
        internal representation of a process
        '''

        # ----------------------------------------------------------------------
        def __init__(self, cmd, stdin, stdout, stderr, shell):

            cmd = cmd.strip()

            self._in_c  = bool(stdin)                # flag stdin  capture
            self._out_c = bool(stdout)               # flag stdout capture
            self._err_c = bool(stderr)               # flag stderr capture

            self._in_r , self._in_w  = os.pipe()     # put stdin  to   child
            self._out_r, self._out_w = os.pipe()     # get stdout from child
            self._err_r, self._err_w = os.pipe()     # get stderr from child

            self._in_o  = os.fdopen(self._in_r)      # file object for in  ep
            self._out_o = os.fdopen(self._out_r)     # file object for out ep
            self._err_o = os.fdopen(self._err_r)     # file object for err ep

            self._in_q  = queue.Queue()              # get stdin  from parent
            self._out_q = queue.Queue()              # put stdout to   parent
            self._err_q = queue.Queue()              # put stderr to   parent

            if is_string(stdout): self._out_f = open(stdout, 'w')
            else                : self._out_f = None

            if is_string(stderr): self._err_f = open(stderr, 'w')
            else                : self._err_f = None

            self.state = RUNNING
            self._proc = sp.Popen(cmd, stdin=self._in_r,
                                       stdout=self._out_w,
                                       stderr=self._err_w,
                                       shell=shell,
                                       bufsize=1)

            t = mt.Thread(target=self._watch)
            t.daemon = True
            t.start()

            self.rc = None  # return code


        @property
        def stdin(self):
            if not self._in_c:
                raise RuntimeError('stdin not captured')
            return self._in_q

        @property
        def stdout(self):
            if not self._out_c:
                raise RuntimeError('stdout not captured')
            return self._out_q

        @property
        def stderr(self):
            if not self._err_c:
                raise RuntimeError('stderr not captured')
            return self._err_q


        @property
        def stdout_filename(self):
            if not self._out_f:
                raise RuntimeError('stdout not recorded')
            return self._out_f.name

        @property
        def stderr_filename(self):
            if not self._err_f:
                raise RuntimeError('stderr not recorded')
            return self._err_f.name

        def kill(self):
            self._proc.terminate()

        # ----------------------------------------------------------------------
        def _watch(self):

            poller = select.poll()
            poller.register(self._out_r, select.POLLIN | select.POLLHUP)
            poller.register(self._err_r, select.POLLIN | select.POLLHUP)

            # try forever to read stdin, stdout and stderr, stop only when
            # either signals that process (parent or child) died
            while True:

                # check for input
                data = self._in_q.get_nowait()
                if data:
                    self._out_o.write(data)
                    self._out_f.write(data)

                active = False
                fds    = poller.poll(100)  # timeout configurable (ms)

                for fd,mode in fds:

                    if mode & select.POLLHUP:
                        # fd died - grab data from other fds
                        continue

                    if fd    == self._out_r:
                        o_in  = self._out_o
                        q_out = self._out_q
                        f_out = self._out_f

                    elif fd  == self._err_r:
                        o_in  = self._err_o
                        q_out = self._err_q
                        f_out = self._err_f

                    line = o_in.readline()  # `bufsize=1` in `popen`

                    if line:
                        # found valid data (active)
                        active = True
                        if q_out: q_out.put(line.rstrip('\n'))
                        if f_out: f_out.write(line)

                # no data received - check process health
                if not active and self._proc.poll() is not None:

                    # process is dead
                    self.rc = self._proc.returncode

                    if self.rc == 0: self.state = DONE
                    else           : self.state = FAILED

                    if self._out_q: self._out_q.put(None)  # signal EOF
                    if self._err_q: self._err_q.put(None)  # signal EOF

                    if self._out_q: self._out_q.join()     # ensure reads
                    if self._err_q: self._err_q.join()     # ensure reads

                    return  # finishes thread
    # --------------------------------------------------------------------------

    return _P(cmd=cmd, stdin=stdin, stdout=stdout, stderr=stderr, shell=shell)


# ------------------------------------------------------------------------------

