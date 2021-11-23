# pylint: disable=protected-access

import re
import os
import queue
import hashlib
import tempfile

from typing import List, Dict, Tuple, Any, Optional

import multiprocessing as mp

from .misc  import as_list, rec_makedir, ru_open
from .shell import sh_callout


# we know that some env vars are not worth preserving.  We explicitly exclude
# those which are common to have complex syntax and need serious caution on
# shell escaping:
BLACKLIST  = ['PS1', 'LS_COLORS', '_', 'SHLVL', 'PROMPT_COMMAND']

# Identical task `pre_exec_cached` settings will result in the same environment
# settings, so we cache those environments here.  We rely on a hash to ensure
# `pre_exec_cached` identity.  Note that this assumes that settings do not
# depend on, say, the unit ID or similar, which needs very clear and prominent
# documentation.  Caching can be turned off by adding a unique noop string to
# the `pre_exec_cached` list - but we probably also add a config flag if that
# becomes a common issue.
_env_cache = dict()

# we use a regex to match snake_case words which we allow for variable names
# with the following conditions
#   - starts with a letter or underscore
#   - consists of letters, underscores and numbers
re_snake_case = re.compile(r'^[a-zA-Z_]\w*$', re.ASCII)

# regex to check if a variable refers to a bash shell function
re_bash_function = re.compile(r'^BASH_FUNC_([a-zA-Z_]\w+)(%%|\(\))$', re.ASCII)

# regex to detect the start of a function definition as written by `env_write()`
re_function = re.compile(r'^([a-zA-Z_]\w+)\(\) {$', re.ASCII)


# ------------------------------------------------------------------------------
#
def env_read(fname: str) -> Dict[str, str]:
    '''
    helper to parse environment from a file: this method parses the output of
    `env` and returns a dict with the found environment settings.
    '''

    with ru_open(fname, 'r') as fin:
        lines = fin.readlines()

    return env_read_lines(lines)


# ------------------------------------------------------------------------------
#
def env_write(script_path, env, unset=None, pre_exec=None):

    data = '\n'
    if unset:
        data += '# unset\n'
        for k in sorted(unset):
            if not re_snake_case.match(k):
                continue
            data += 'unset %s\n' % k
        data += '\n'

    if BLACKLIST:
        data += '# blacklist\n'
        for k in sorted(BLACKLIST):
            data += 'unset %s\n' % k
        data += '\n'

    funcs = list()
    data += '# export\n'
    for k in sorted(env.keys()):
        if k in BLACKLIST:
            continue
        if k.startswith('BASH_FUNC_') and \
                (k.endswith('%%') or k.endswith('()')):
            funcs.append(k)
            continue
        if not re_snake_case.match(k):
            continue
        data += "export %s=%s\n" % (k, _quote(env[k]))
    data += '\n'

    if funcs:
        data += '\n# functions\n'
        for func in funcs:
            fname = func[10:-2]
            v     = env[func]
            if v.startswith('() { '):
                v = v.replace('() { ', '() {\n', 1)
            data += '%s%s\n' % (fname, v.replace('\\n', '\n'))
            data += 'test -z "$BASH" || export -f %s\n\n' % fname
        data += '\n'

    if pre_exec:
        data += '# pre_exec (not cached)\n'

        # do not sort, order dependent
        for cmd in pre_exec:
            data += '%s\n' % cmd
        data += '\n'

    with ru_open(script_path, 'w') as fout:
        fout.write(data)


# ------------------------------------------------------------------------------
#
def env_read_lines(lines: List[str]) -> Dict[str, str]:
    '''
    read lines which are the result of an `env` shell call, and sort the
    resulting keys into and environment and a shell function dict, return both
    '''

    # POSIX definition of variable names
    env     = dict()
    key     = None
    val     = ''

    for line in lines:

        # remove newline
        line = line.rstrip('\n')

        if not line:
            continue

        # search for new key
        if '=' not in line:
            # no key present - append linebreak and line to value
            val += '\n'
            val += line
            continue

        elems    = line.split('=', 1)
        this_key = elems.pop(0)
        this_val = elems[0] if elems else ''

        if re_snake_case.match(this_key):
            # valid key - store previous key/val if we have any, and
            # initialize `key` and `val`
            if key and key not in BLACKLIST:
                env[key] = val

            key = this_key
            val = this_val.strip()

        elif re_bash_function.match(this_key):
            # function definitions
            # initialize `key` and `val`
            if key and key not in BLACKLIST:
                env[key] = val

            key = this_key
            val = this_val.strip()

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
def _quote(data: str) -> str:

    if "'" in data or '$' in data or '`' in data:
        # cannot use single quote, so use double quote and escale all other
        # double quotes in the data
        # NOTE: we only support these three types of shell directives
        data = data.replace('"', '\\"') \
                   .replace('$', '\\$')
        data = '"' + data + '"'

    else:
        # single quotes will do
        data = "'" + data +  "'"

    return data


# ------------------------------------------------------------------------------
#
def _unquote(data: str) -> str:

    if data.startswith("'") and data.endswith("'"):
        # just remove enclosing single quotes - no nesting
        data = data[1:-1]

    elif data.startswith('"') and data.endswith('"'):
        # remove enclosing double quotes, and replace all occurences of escaled
        # double quotes (`\"`) with an unescaled one (`"`).
        data = data[1:-1]
        data = data.replace('\\"', '"')

    return data


# ------------------------------------------------------------------------------
#
def env_eval(fname: str) -> Dict[str, str]:
    '''
    helper to create a dictionary with the env settings in the specified file
    which contains `unset` and `export` directives, or simple 'key=val' lines
    '''

    env = dict()
    with ru_open(fname, 'r') as fin:

        func_name = None
        func_data = None
        for line in fin.readlines():

            if func_name:
                # we capture a function definition right now - check if done
                if line == 'test -z "$BASH" || export -f %s' % func_name:
                    # done - convert into a bash env variables
                    env['BASH_FUNC_%s%%%%' % func_name] = '() %s' % func_data
                    # stop function parsing
                    func_name = None
                    func_data = None
                    continue
                else:
                    # still part of function data, replace the stripped newline
                    if func_data:
                        func_data += '\n'
                    func_data += line
                    continue

            line = line.strip()

            if not line:
                continue

            if line.startswith('#'):
                continue

            func_check = re_function.match(line)
            if func_check:
                # detected start of function
                assert(func_name is None)
                func_name = func_check[1]
                func_data = ''
                assert(func_name)
                continue


            if line.startswith('unset ') :
                _, spec = line.split(' ', 1)
                k = spec.strip()
                if k not in env:
                    continue
                del(env[k])

            elif line.startswith('export ') :
                _, spec = line.split(' ', 1)
                elems   = spec.split('=', 1)
                k = elems.pop(0)
                v = elems[0] if elems else ''
                env[k] = _unquote(v.strip())

            else:
                elems = line.split('=', 1)
                k = elems.pop(0)
                v = elems[0] if elems else ''
                env[k] = _unquote(v.strip())

    return env


# ------------------------------------------------------------------------------
#
def env_dump(environment: Optional[Dict[str,str]] = None,
             script_path: Optional[str]           = None) -> None:

    if not environment:
        environment = dict(os.environ)

    if script_path:
        with ru_open(script_path, 'w') as fout:
            for k in sorted(environment.keys()):
                fout.write('%s=%s\n' % (k, environment[k].replace('\n', '\\n')))
    else:
        for k in sorted(environment.keys()):
            print('%s=%s' % (k, environment[k].replace('\n', '\\n')))


# ------------------------------------------------------------------------------
#
def env_prep(environment    : Optional[Dict[str,str]] = None,
             unset          : Optional[List[str]]     = None,
             pre_exec       : Optional[List[str]]     = None,
             pre_exec_cached: Optional[List[str]]     = None,
             script_path    : Optional[str]           = None
            ) -> Dict[str, str]:
    '''
    Create a shell script which restores the environment specified in
    `environment` environment (dict).  While doing so, ensure that all env
    variables *not* defined in `environment` but defined in `unset` (list) are
    unset.  Also ensure that all commands provided in `pre_exec_cached` (list)
    are executed after these settings.

    Once the shell script is created, run it and dump the resulting env, then
    read it back via `env_read()` and return the resulting env dict - that
    can then be used for process fork/execve to run a process is the thus
    defined environment.

    The resulting environment will be cached: a subsequent call with the same
    set of parameters will simply return a previously cached environment if it
    exists.

    If `script_path` is given, a shell script will be created in the given
    location so that shell commands can source it and restore the specified
    environment.

    Any commands given in 'pre_exec' will be part of the cached script, and will
    thus *not* be executed when preparing the env, but *will* be executed
    whenever the prepared shell script is sources.  The returned env dictionary
    will thus *not* include the effects of those injected commands.
    '''

    # defaults
    if environment     is None: environment     = dict(os.environ)
    if unset           is None: unset           = list()
    if pre_exec        is None: pre_exec        = list()
    if pre_exec_cached is None: pre_exec_cached = list()

    if pre_exec and not script_path:
        raise ValueError('`pre_exec` must be used with `script_path`')

    # empty `pre_exec*` settings are ok - just ensure correct type
    pre_exec        = as_list(pre_exec       )
    pre_exec_cached = as_list(pre_exec_cached)

    # cache lookup
    cache_key = str(sorted(environment.items())) \
              + str(sorted(unset))               \
              + str(sorted(pre_exec_cached))
    cache_md5 = hashlib.md5(cache_key.encode('utf-8')).hexdigest()

    if cache_md5 in _env_cache:
        env = _env_cache[cache_md5]

    else:
        # cache miss

        # Write a temporary shell script which
        #
        #   - unsets all variables which are not defined in `environment`
        #     but are defined in the `unset` list;
        #   - unset all blacklisted vars;
        #   - sets all variables defined in the `environment` dict;
        #   - runs the `pre_exec_cached` commands given;
        #   - dumps the resulting env in a temporary file;
        #
        # Then run that script and read the resulting env back into a dict to
        # return.  If `script_path` is specified, then also create a file at the
        # given name and fill it with `unset` and `export` statements to
        # recreate that specific environment: any shell sourcing that
        # `script_path` file thus activates the environment we just prepared.
        tgt = os.getcwd() + '/env/'
        rec_makedir(tgt)

        if script_path: prefix = os.path.basename(script_path)
        else          : prefix = None

        _, tmp_name = tempfile.mkstemp(prefix=prefix, dir=tgt)

        env_write(tmp_name, environment, unset, pre_exec_cached)
        cmd = '/bin/bash -c ". %s && /usr/bin/env"' % tmp_name
        out, err, ret = sh_callout(cmd)

        if ret:
            raise RuntimeError('error running "%s": %s' % (cmd, err))

        env = env_read_lines(out.split('\n'))
      # os.unlink(tmp_name)

        _env_cache[cache_md5] = env


    # If `script_path` is specified, create a script with that name which unsets
    # the same names as in the tmp script above, and exports all vars from the
    # resulting env from above (thus storing the *results* of the
    # `pre_exec_cached` env, not the env and `pre_exec_cached` directives
    # themselves).
    #
    # FIXME: files could also be cached and re-used (copied or linked)
    if script_path:

        env_write(script_path, env=env, unset=unset, pre_exec=pre_exec)

    return env


# ------------------------------------------------------------------------------
#
def env_diff(env_1 : Dict[str,str],
             env_2 : Dict[str,str]
            ) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,str]]:
    '''
    This method serves debug purposes: it compares to environments and returns
    those elements which appear in only either one or the other env, and which
    changed from one env to another.

    It will ignore any keys in the `BLACKLIST` and will also ignore
    `BASH_FUNC_*` keys which point to bash function definitions.
    '''

    only_1  = dict()
    only_2  = dict()
    changed = dict()

    keys_1 = sorted(env_1.keys())
    keys_2 = sorted(env_2.keys())

    for k in keys_1:
        if k in BLACKLIST:
            continue
        if k.startswith('BASH_FUNC_'):
            continue
        v = env_1[k]
        if   k not in env_2: only_1[k]  = v
        elif v != env_2[k] : changed[k] = [v, env_2[k]]

    for k in keys_2:
        if k in BLACKLIST:
            continue
        if k.startswith('BASH_FUNC_'):
            continue
        v = env_2[k]
        if k not in env_1: only_2[k]  = v
        # else is checked in keys_1 loop above

    return only_1, only_2, changed


# ------------------------------------------------------------------------------
#
class EnvProcess(object):
    '''
    run a code segment in a different os.environ

        env = {'foo': 'buz'}
        with ru.EnvProcess(env=env) as p:
            if p:
                p.put(os.environ['foo'])

        print('-->', p.get())
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, env : Dict[str, str]) -> None:

        self._q     = mp.Queue()
        self._env   = env
        self._data  = [None, None]   # data, exception
        self._child = None


    # --------------------------------------------------------------------------
    #
    def __bool__(self) -> Optional[bool]:

        return self._child


    # --------------------------------------------------------------------------
    #
    def __enter__(self) -> 'EnvProcess':

        if os.fork():
            self._parent = True
            self._child  = False
        else:
            self._parent = False
            self._child  = True

        if self._child:

            for k in os.environ:
                del(os.environ[k])

            for k, v in self._env.items():
                os.environ[k] = v

            # refresh the python interpreter in that environment
            import site
            import importlib

            importlib.reload(site)
            importlib.invalidate_caches()

        return self


    # --------------------------------------------------------------------------
    #
    def __exit__(self, exc  : Optional[Exception],
                       value: Optional[Any],
                       tb   : Optional[Any]
                ) -> None:

        if exc and self._child:
            self._q.put([None, exc])
            self._q.close()
            self._q.join_thread()
            os._exit(0)


        if self._parent:

            while True:
                try:
                    self._data = self._q.get(timeout=1)
                    break
                except queue.Empty:
                    pass


    # --------------------------------------------------------------------------
    #
    def put(self, data: str) -> None:

        if self._child:
            self._q.put([data, None])
            self._q.close()
            self._q.join_thread()
            os._exit(0)


    # --------------------------------------------------------------------------
    #
    def get(self) -> Any:

        data, exc = self._data
        if exc:
            raise exc                         # pylint: disable=raising-bad-type

        return data


# ------------------------------------------------------------------------------

