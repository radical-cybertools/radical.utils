
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2016, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys
import glob
import stat
import time
import shlex
import textwrap
import threading       as mt
import subprocess      as sp
import multiprocessing as mp

from .ids import generate_id, ID_CUSTOM, ID_PRIVATE


# ------------------------------------------------------------------------------
#
_TMP_ROOT = '/tmp/ru_sh_%s' % os.getuid()
_DEF_ROOT = '%s/.radical/utils/sh' % os.environ.get('HOME')
_ENV_ROOT = os.environ.get('RU_SH_ROOT')

if   _ENV_ROOT           : _ROOT = _ENV_ROOT
elif 'HOME' in os.environ: _ROOT = _DEF_ROOT
else                     : _ROOT = _TMP_ROOT


# ------------------------------------------------------------------------------
#
def sh_callout(cmd, shell=False, quiet=False):
    '''
    call a shell command, return `[stdout, stderr, retval]`.

      * `cmd`  : command string to execute
      * `shell`: run cmd in a shell (as `sh -c $cmd`)    (default: False)
      * `quiet`: capture stdout/stderr                   (default: True )
    '''

    # convert string into arg list if needed
    if not shell and isinstance(cmd, str):
        cmd = shlex.split(cmd)

    if quiet:
        p = sp.Popen(cmd, stdout=None, stderr=None, shell=shell)
    else:
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=shell)

    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode


# --------------------------------------------------------------------------
#
class Cmd(object):
    '''
    This class represents a command executed by a ru.SH instance.  That
    command's state is maintained in the file system - this class only has
    a local representation of that state.  Once the command is in any of the
    `FINAL` states, its stdout, stderr and return value will be available.

    Instances of this class should only be created by an ru.SH instances.
    '''

    # FIXME: make partial stdout/stderr available before reaching final state

    # state defines
    UNKNOWN  = 'unknown'    # before reconnect and state query
    NEW      = 'new'        # command is defined
    RUNNING  = 'running'    # command is being executed
    DONE     = 'done'       # execution completed w/o error
    FAILED   = 'failed'     # execution completed w/  error
    CANCELED = 'canceled'   # caller canceled exection
    PURGED   = 'purged'     # remote state will be unavailable

    FINAL    = [DONE, FAILED, CANCELED, PURGED]


    # ----------------------------------------------------------------------
    #
    def __init__(self, sh, cmd=None, uid=None):

        if not cmd and not uid:
            raise ValueError('nothing to do, nothing to reconnect')

        if cmd and uid:
            raise ValueError('cannot *re*connect to a new command')

        self._sh    = sh
        self._cmd   = cmd
        self._state = self.NEW

        if uid:
            self._uid = uid
            self._pwd = '%s/%s' % (self._sh.root, self._uid)
            self._state = self.UNKNOWN
            self._initialize()
            self._reconnect()

        else:
            self._uid = generate_id('cmd.%(item_counter)06d', mode=ID_CUSTOM,
                                    base=self._sh.root)
            self._pwd = '%s/%s' % (self._sh.root, self._uid)
            self._initialize()
            self._run()


    # ----------------------------------------------------------------------
    #
    def _initialize(self):

        self._sh_path  = '%s/cmd.sh' % self._pwd
        self._cmd_path = '%s/cmd'    % self._pwd
        self._out_path = '%s/out'    % self._pwd
        self._err_path = '%s/err'    % self._pwd
        self._ret_path = '%s/ret'    % self._pwd
        self._pid_path = '%s/pid'    % self._pwd

        self._out  = None  # only set once the command is done
        self._err  = None  # --             ''              --
        self._ret  = None  # --             ''              --
        self._pid  = None  # will be set on reconnect, too
        self._proc = None  # on reconnect, self._proc handle remains `None`


    # ----------------------------------------------------------------------
    #
    def _update(self):


        if self._ret is None:
            if os.path.isfile(self._ret_path):
                try:
                    with open(self._ret_path, 'r') as f:
                        self._ret = int(f.read().strip())
                    if self._ret == 0: self._state = self.DONE
                    else             : self._state = self.FAILED

                except Exception as e:
                    self._state = -255
                    self._state = self.FAILED

            elif self._pid:
                self._state = self.RUNNING


        if self._state in self.FINAL and self._ret is not None:

            if os.path.isfile(self._out_path):
                with open(self._out_path, 'r') as f:
                    self._out = f.read().rstrip()

            if os.path.isfile(self._err_path):
                with open(self._err_path, 'r') as f:
                    self._err = f.read().rstrip()


    # ----------------------------------------------------------------------
    #
    @property
    def sh(self) : return self._sh

    @property
    def uid(self): return self._uid

    @property
    def cmd(self): return self._cmd


    # ----------------------------------------------------------------------
    #
    @property
    def state(self):

        self._update()
        return self._state


    # ----------------------------------------------------------------------
    #
    @property
    def pid(self):

        if self._pid is None:
            if os.path.isfile(self._pid_path):
                with open(self._pid_path, 'r') as f:
                    self._pid = int(f.read().strip())

        return self._pid


    # ----------------------------------------------------------------------
    #
    @property
    def ret(self):

        self._update()
        return self._ret


    # ----------------------------------------------------------------------
    #
    @property
    def out(self):

        self._update()
        return self._out


    # ----------------------------------------------------------------------
    #
    @property
    def err(self):

        self._update()
        return self._err


    # ----------------------------------------------------------------------
    #
    def _reconnect(self):

        # ensure that uid and pwd are valid
        if not os.path.isdir(self._pwd):
            raise ValueError('invalid command uid %s' % self._uid)

        assert(os.path.isfile(self._cmd_path))
        assert(os.path.isfile(self._pid_path))

        with open(self._cmd_path, 'r') as fin:
            self._cmd = str(fin.read().strip())

        with open(self._pid_path, 'r') as fin:
            self._pid = int(fin.read().strip())

        self._update()


    # ----------------------------------------------------------------------
    #
    def _run(self):
        '''
        Create a shell script which will execute the requested command
        asynchronously and reconnectably in its pwd sandbox.
        '''

        if os.path.isdir(self._pwd):
            raise RuntimeError('cannot run cmd (pwd exists: %s)' % self._pwd)

        os.makedirs(self._pwd)

        script = '''\
                 #!/bin/sh

                 cd %s
                 . ./cmd 1> ./out \\
                         2> ./err
                 echo $?  > ./ret

                 ''' % self._pwd

        with open(self._sh_path, 'w') as fout:
            fout.write(textwrap.dedent(script))

        with open(self._cmd_path, 'w') as fout:
            fout.write('%s\n' % self._cmd)

        # make script executable
        st = os.stat(self._sh_path)
        os.chmod(self._sh_path, st.st_mode | stat.S_IEXEC)


        # the script now exists and can be executed.  The subroutine below is
        # the core of that, and will be executed in a separate, daemonized
        # process.
        def run_cmd():
            self._sh._q.put(self.uid)
            with open(self._pid_path, 'w') as fout:
                fout.write('%d\n' % os.getpid())
            sys.exit(sh_callout(self._sh_path, shell=True, quiet=True))

        self._proc = mp.Process(target=run_cmd)
        self._proc.daemon = True
        self._proc.start()

        # wait until we see the process, as otherwise the shell may close to
        # quickly for the command to spawn -- calling `is_alive()` is *not*
        # sufficient for that!
        # NOTE:  Well, even that is not enough actually, we need to wait until
        #        the process is daemonized.  There is no way to check for this
        #        via the mp API, so we ignore that part
        # FIXME: should we check for a change in the process' parent ID for
        #        daemon confirmation?
        assert(self.uid == self._sh._q.get())
        self._state = self.RUNNING


# ------------------------------------------------------------------------------
#
class SH(object):
    '''
    The class represents a service endpoint which will execute shell commands
    asynchronously.  It will store state of all tasks on disk (in `%s/$uid`).
    As the state is thus persistent, new instances of this class can reconnect
    to that state, by providing a `UID`.
    ''' % _ROOT

    def __init__(self, uid=None, root=None):
        '''
        If a session ID `uid` is given, we expect to reconnect to commands
        started at a previous point in time, and expect the respective command
        states to be available in the session root (`root`).
        '''

        self._uid  = uid
        self._root = root

        if  self._uid is None:
            self._uid = generate_id('ru.sh', ID_PRIVATE)

        if self._root is None:
            self._root = '%s/%s' % (_ROOT, self._uid)

        # we use a multiprocessing queue to ensure correct process spawning.
        # Note that this requires `run()` to be guarded to maintain
        # thread-safety.  We could also make the queue private to the `Cmd`
        # class, but that creates an overheadd we want to avoid: keeping the
        # queue on the `Shell` is thus an resource usage optimizaiton.
        self._q    = mp.Queue()

        # make sure the state root dir exists
        if not os.path.isdir(self._root):
            os.makedirs(self._root, 0o700)

        # pick up state for all known commands from disk
        self._cmds = dict()
        for cmd_dir in glob.glob('%s/cmd.*' % self.root):
            cmd = Cmd(self, uid=os.path.basename(cmd_dir))
            self._cmds[cmd.uid] = cmd


    # --------------------------------------------------------------------------
    #
    @property
    def root(self):
        return self._root
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    def run(self, cmd):

        _cmd = Cmd(self, cmd)
        self._cmds[_cmd.uid] = _cmd
        return _cmd


    # --------------------------------------------------------------------------
    #
    def list(self):

        return self._cmds.keys()


    # --------------------------------------------------------------------------
    #
    def get(self, uid):

        return self._cmds[uid]


# ------------------------------------------------------------------------------

