
import re
import os
import hashlib
import tempfile

import radical.utils as ru


# we know that some env vars are not worth preserving.  We explicitly exclude
# those which are common to have complex syntax and need serious caution on
# shell escaping:
BLACKLIST  = ['PS1', 'LS_COLORS', '_']

# Identical task `pre` settings will result in the same environment settings, so
# we cache those environments here.  We rely on a hash to ensure `pre` identity.
# Note that this assumes that settings do not depend on, say, the unit ID or
# similar, which needs very clear and prominent documentation.  Caching can be
# turned off by adding a unique noop string to the `pre` list - but we probably
# also add a config flag if that becomes a common issue.
_env_cache = dict()


# ------------------------------------------------------------------------------
#
def env_read(fname):
    '''
    helper to parse environment from a file: this method parses the output of
    `env` and returns a dict with the found environment settings.
    '''

    # POSIX definition of variable names
    key_pat = r'^[A-Za-z_][A-Za-z_0-9]*$'
    env     = dict()

    with open(fname, 'r') as fin:

        key = None
        val = ''

        for line in fin.readlines():

            # remove newline
            line = line.rstrip('\n')

            # search for new key
            if '=' not in line:
                # no key present - append linebreak and line to value
                val += '\n'
                val += line
                continue


            this_key, this_val = line.split('=', 1)

            if re.match(key_pat, this_key):
                # valid key - store previous key/val if we have any, and
                # initialize `key` and `val`
                if key and key not in BLACKLIST:
                    env[key] = val

                key = this_key
                val = this_val
            else:
                # invalid key - append linebreak and line to value
                val += '\n'
                val += line

        # store last key/val if we have any
        if key and key not in BLACKLIST:
            env[key] = val

    return env


# ------------------------------------------------------------------------------
#
def env_prep(src, rem=None, pre=None, tgt=None):
    '''
    create a shell script which restores the environment specified in `src` (a
    dict).  While doing so, ensure that all env variables *not* defined in src
    but defined in `rem` (list) are unset.  Also ensure that all commands
    provided in `pre_exec_env` (list of strings) are executed after these
    settings.

    Once the shell script is created, run it and dump the resulting env, then
    read it back via `env_read()` and return the resulting env dict - that can
    then be used for process fork/execve to run a process is the thus defined
    environment.

    The resulting environment will be cached: a subsequent call with the same
    set of parameters will simply return a previously cached environment if it
    exists.

    If `tgt` is given, a shell script will be created in the given location
    so that shell commands can source it and restore the specified environment.
    '''

    global _env_cache

    # defined in the current os env
    if  rem is None:
        rem = list(os.environ.keys())

    # empty pre settings are ok - just ensure correct type
    pre = ru.as_list(pre)

    # cache lookup
    cache_key = str(src) + str(rem) + str(pre)
    cache_md5 = hashlib.md5(cache_key.encode('utf-8')).hexdigest()
    if cache_md5 in _env_cache:

        cache_env = _env_cache[cache_md5]

    else:
        # cache miss

        # Write a temporary shell script which
        #
        #   - unsets all variables which are not defined in `src` but are defined
        #     in the `rem` env dict;
        #   - unset all blacklisted vars;
        #   - sets all variables defined in the `src` env dict;
        #   - runs the `pre` commands given;
        #   - dumps the resulting env in a temporary file;
        #
        # Then run that script and read the resulting env back into a dict to
        # return.  If `tgt` is specified, then also create a file at the given
        # name and fill it with `unset` and `tgt` statements to recreate that
        # specific environment: any shell sourcing that `tgt` file thus activates
        # the environment thus prepared.
        #
        # FIXME: better tmp file names to avoid collisions

        tmp_file, tmp_name = tempfile.mkstemp()

        # use a file object to simplify byte conversion
        fout = os.fdopen(tmp_file, 'w')
        try:
            if rem:
                fout.write('\n# unset\n')
                for k in rem:
                    if k not in src:
                        fout.write('unset %s\n' % k)
                fout.write('\n')

            if BLACKLIST:
                fout.write('# blacklist\n')
                for k in BLACKLIST:
                    fout.write('unset %s\n' % k)
                fout.write('\n')

            if src:
                fout.write('# export\n')
                for k, v in src.items():
                    # FIXME: shell quoting for value
                    if k not in BLACKLIST:
                        fout.write("export %s='%s'\n" % (k, v))
                fout.write('\n')

            if pre:
                fout.write('# pre\n')
                for cmd in pre:
                    fout.write('%s\n' % cmd)
                fout.write('\n')

        finally:
            fout.close()

        cmd = '/bin/sh -c ". %s && env | sort > %s.env"' % (tmp_name, tmp_name)
        os.system(cmd)
        env = env_read('%s.env' % tmp_name)
        os.unlink(tmp_name)

        _env_cache[cache_md5] = env


    # if `tgt` is specified, create a script with that name which unsets the
    # same names as in the tmp script above, and exports all vars from the
    # resulting env from above (thus storing the *results* of the pre_exec'ed
    # env, not the env and pre_exec directives themselves).
    #
    # FIXME: files could also be cached and re-used (copied or linked)
    if tgt:
        with open(tgt, 'w') as fout:

            fout.write('\n# unset\n')
            for k in rem:
                if k not in src:
                    fout.write('unset %s\n' % k)
            fout.write('\n')

            fout.write('# blacklist\n')
            for k in BLACKLIST:
                fout.write('unset %s\n' % k)
            fout.write('\n')

            fout.write('# export\n')
            for k, v in env.items():
                # FIXME: shell quoting for value
                fout.write("export %s='%s'\n" % (k, v))
            fout.write('\n')

    return env


# ------------------------------------------------------------------------------
#
def env_diff(env_1, env_2):
    '''
    This method serves debug purposes: it compares to environments and returns
    those elements which appear in only either one or the other env, and which
    changed from one env to another.
    '''

    only_1  = dict()
    only_2  = dict()
    changed = dict()

    keys_1 = sorted(env_1.keys())
    keys_2 = sorted(env_2.keys())

    for k in keys_1:
        v = env_1[k]
        if   k not in env_2: only_1[k]  = v
        elif v != env_2[k] : changed[k] = [v, env_2[k]]

    for k in keys_2:
        v = env_2[k]
        if   k not in env_1: only_2[k]  = v
        elif v != env_1[k] : changed[k] = [env_1[k], v]

    return only_1, only_2, changed


# ------------------------------------------------------------------------------

