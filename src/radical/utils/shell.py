
import shlex
import subprocess as sp


# ------------------------------------------------------------------------------
#
def sh_callout(cmd, shell=False):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''

    # convert string into arg list if needed
    if not shell and isinstance(cmd, basestring):
        cmd = shlex.split(cmd)

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=shell)
    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode


# ------------------------------------------------------------------------------

