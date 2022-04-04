
import os
import queue
import shlex
import select

from typing import Optional, Dict

import threading  as mt
import subprocess as sp

from .constants import RUNNING, DONE, FAILED, CANCELED
from .misc      import is_string, as_bytes
from .misc      import ru_open



# ------------------------------------------------------------------------------
#
def sh_quote(data):
    '''
    quote a string and wrap it in double quotes so that it can passed to a POSIX
    shell.

    Examples:

      foo        -> "foo"
      foo"bar    -> "foo\\"bar"
      foo\\"bar  -> "foo\\\\\\"bar"
      $FOO'$BAR  -> "$FOO'$BAR"

    NOTE: this method does not attempt to strip out backticks or other code
          execution mechanisms from the string.

    '''

    if '\\' in data: data.replace('\\', '\\\\')
    if '"'  in data: data.replace('"',  '\\"')

    return '"%s"' % data


# ------------------------------------------------------------------------------
#
def sh_callout(cmd, stdout=True, stderr=True, shell=False, env=None):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''

    # convert string into arg list if needed
    if is_string(cmd) and \
       not shell: cmd    = shlex.split(cmd)

    if stdout   : stdout = sp.PIPE
    else        : stdout = None

    if stderr   : stderr = sp.PIPE
    else        : stderr = None

    p = sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell, env=env)

    if not stdout and not stderr:
        ret = p.wait()
    else:
        stdout, stderr = p.communicate()
        ret            = p.returncode

    return stdout.decode("utf-8"), stderr.decode("utf-8"), ret


# ------------------------------------------------------------------------------
#
def sh_callout_bg(cmd, stdout=None, stderr=None, shell=False, env=None):
    '''
    call a shell command in the background.  Do not attempt to pipe STDOUT/ERR,
    but only support writing to named files.
    '''

    # pipes won't work - see sh_callout_async
    if stdout == sp.PIPE: raise ValueError('stdout pipe unsupported')
    if stderr == sp.PIPE: raise ValueError('stderr pipe unsupported')

    # openfile descriptors for I/O, if needed
    if is_string(stdout): stdout = ru_open(stdout, 'w')
    if is_string(stderr): stderr = ru_open(stderr, 'w')

    # convert string into arg list if needed
    if not shell and is_string(cmd): cmd = shlex.split(cmd)

    sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell, env=env)

    return


# ------------------------------------------------------------------------------
#
def sh_callout_async(cmd, stdin=True, stdout=True, stderr=True,
                          shell=False, env=None):
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
  # assert(False), 'this is broken for python apps'

    # --------------------------------------------------------------------------
    #
    class _P(object):
        '''
        internal representation of a process
        '''

        # ----------------------------------------------------------------------
        def __init__(self, cmd, shell:  bool = False,
                                env:    Optional[Dict[str,str]] = None):

            cmd = cmd.strip()

            self._shell = shell
            self._env   = env

            self._buf_in   = b''
            self._buf_out  = b''
            self._buf_lock = mt.Lock()

            self._proc = sp.Popen(cmd, stdin=sp.PIPE,
                                       stdout=sp.PIPE,
                                       stderr=sp.STDOUT,
                                       shell=shell,
                                       env=env,
                                       bufsize=0)  # unbuffered

            t = mt.Thread(target=self._watch)
            t.daemon = True
            t.start()

            self.state = RUNNING
            self.rc    = None  # return code


        # ----------------------------------------------------------------------
        #
        def kill(self):
            self._state = CANCELED
            self._proc.terminate()


        # ----------------------------------------------------------------------
        #
        def _watch(self):

            pipe_in  = self._proc.stdin
            pipe_out = self._proc.stdout

            # help the type checker
            assert(pipe_in)
            assert(pipe_out)

            poller = select.poll()
            poller.register(pipe_out, select.POLLIN)

            while True:


                # handle stdin
                with self._buf_lock:
                    if self._buf_in:
                        pipe_in.write(self._buf_in)
                        pipe_in.flush()
                        self._buf_in = b''

                # handle stdout, stderr
                events = poller.poll(timeout=0.1)
                if pipe_out in events:
                    with self._buf_lock:
                        self._buf_out += pipe_out.read()

                # check process health
                if self._proc.poll():
                    break

            # we should only get here once the process terminates
            if self._state != CANCELED:
                if self._proc.returncode == 0:
                    self._state = DONE
                else:
                    self._state = FAILED


    # --------------------------------------------------------------------------

    return _P(cmd=cmd, stdin=stdin, stdout=stdout, stderr=stderr,
                       shell=shell, env=env)


# ------------------------------------------------------------------------------

