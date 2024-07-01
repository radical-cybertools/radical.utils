
# The methods defined here reflect the capabilities to dump and prepare
# environments as implemented in `00_env_isolation.py`, but are, obviously, for
# use in shell scripts.  Specifically `05_env_isolation_wrapper.sh` uses these
# methods to temporarily escape the LM env before running `pre_exec_cmd`
# directives.
#
# example usage
# ------------------------------------------------------------------------------
# export FOO="foo\"bar\"buz"
# env_dump ed1.env
#
# export BAR="foo\"bar\"buz"
# env_dump ed2.env
#
# env_prep ed1.env ed2.env ed3.sh  "echo foo bar" "echo buz"
# ------------------------------------------------------------------------------


# do not export blacklisted env variables
BLACKLIST="PS1 LS_COLORS _"


# ------------------------------------------------------------------------------
#
env_grep(){

    # some env variables are known to have difficult to parse values and at the
    # same time don't need to be inherited - this method filters them out

    grep -v \
         -e '^LS_COLORS=' \
         -e '^PS1=' \
         -e '^_=' \
         -e '^SHLVL='
}


# ------------------------------------------------------------------------------
#
env_dump(){

    # The purpose of this method is to dump the environment of the current shell
    # in a format that can be parsed by other shell scripts (see `env_get`) and
    # from Python (or other languages).  That is a surprisingly difficult task:
    # environment variables can contain characters not allowed by POSIX (hello
    # bash functions), and values can contain unprintable characters, newlines,
    # quotes, assignments etc.  Consider this example:
    #
    #     foo="bar\nbuz=biz"
    #
    # `env` will result in
    #
    #     ...
    #     foo=bar
    #     buz=biz
    #     ...
    #
    # which a parser cannot reliably distinguish from two separate exports.
    # Alas, `env -0` (which outputs null-terminated strings) is not POSIX
    # compliant.
    #
    # We thus use `awk` to inspect the environment and to translate all value
    # line breaks into `\n`:
    #
    #     awk 'END {
    #       for (k in ENVIRON) {
    #         v=ENVIRON[k];
    #         gsub(/\n/,"\\n",v);
    #         print k"="v;
    #       }
    #     }' < /dev/null
    #
    # which, for our foo above, results into
    #
    #     ...
    #     foo=bar\nbuz=biz
    #     ...
    #
    # which can be cleanly parsed
    #

    local tgt
    local OPTIND OPTARG OPTION
    while getopts "t:" OPTION; do
        case $OPTION in
            t)  tgt="$OPTARG" ;;
            *)  echo "Unknown option: '$OPTION'='$OPTARG'"
                return 1;;
        esac
    done

    dump() {
        awk 'END {
          for (k in ENVIRON) {
            v=ENVIRON[k];
            gsub(/\n/,"\\n",v);
            print k"="v;
          }
        }' < /dev/null
    }

    if test -z "$tgt"; then
        dump | sort | env_grep
    else
        dump | sort | env_grep > "$tgt"
    fi
}


# ------------------------------------------------------------------------------
#
env_prep(){

    # Write a shell script to `tgt` (default: stdout) which
    #
    #   - unsets all variables which are not defined in `src` but are defined
    #     in the `rem` env;
    #   - unset all blacklisted vars;
    #   - sets all variables defined in the `src` env;
    #   - runs the `pre_exec_env` commands given;
    #
    # The resulting shell script can be sourced to activate the resulting
    # environment.  Note that, other than the Python counterpart, this method
    # does not return any representation of the resulting environment, but
    # simply creates the described shell script.
    #
    # Arguments:
    #
    #   -s <file>    : File containing the 'source' environment to re-create
    #   -r <file>    : File containing the 'remove' env to unset if needed
    #   -p cmd       : Command to run after all env settings (and unsettings)
    #                  This parameter can be specified multiple times.
    #   -t <file>    : File to write the target setting to -- sourcing that
    #                  file from within the 'remove' environment will re-create
    #                  the desired 'source' environment.
    #
    # Note that a side effect of this function is that the pre-exec commands
    # will be run once *immediately*, not repeatedly when the resulting target
    # script is sourced.
    #
    # FIXME: the latter is not yet true and needs fixing
    #
    local src tgt rem pre blk
    local OPTIND OPTION OPTARG
    while getopts "s:r:p:t:" OPTION; do
        case $OPTION in
            s) src="$OPTARG"       ;;
            t) tgt="$OPTARG"       ;;
            r) rem="$OPTARG"       ;;
            p) pre="$pre$OPTARG\n" ;;
            b) blk="$OPTARG"       ;;
            *)  echo "Unknown option: '$OPTION'='$OPTARG'"
                return 1;;
        esac
    done

    if test -z "$src"
    then
        echo "missing 'src' -- prepare env from current env"
        tmp=$(mktemp)
        env_dump -t "$tmp"
        src="$tmp"
    fi

    # get keys from `src` environment dump
    # NOTE: bash func exports can end in `()` or `%%`
    src_keys=$( cat "$src" \
               | grep -e '^[A-Za-z_][A-Za-z_0-9]*\(()\|\%\%\)\?=' \
               | cut -f1 -d= \
               | sort
               )

    if ! test -z "$rem"
    then
        # get keys from `rem` environment dump
        rem_keys=$( cat "$rem" \
                   | grep -e '^[A-Za-z_][A-Za-z_0-9]*\(()\|\%\%\)\?=' \
                   | cut -f1 -d= \
                   | sort
                   )
    fi

    _prep(){
        # unset all keys which are in `rem` but not in `src`
        if ! test -z "$rem_keys"
        then
            printf "\n# unset\n"
            for k in $rem_keys
            do
                grep -e "^$k=" $src >/dev/null || echo "unset '$k'"
            done
        fi

        # unset all blacklisted keys
        if ! test -z "$BLACKLIST"
        then
            printf "\n# blacklist\n"
            for k in $BLACKLIST
            do
                echo "unset '$k'"
            done
        fi


        # export all keys from `src` (but filter out bash function definitions)
        printf "\n# export\n"
        functions=''
        for k in $src_keys
        do
            # exclude blacklisted keys
            if expr "$BLACKLIST" : ".*\<$k\>.*" >/dev/null
            then
                continue
            fi

            # handle bash function definitions
            v=$(grep -e "^$k=" $src | cut -f 2- -d= | sed -e 's/{ /{\n/')
            func=$(expr "$k" : '^BASH_FUNC_\(.*\)()$' \
                     \| "$k" : '^BASH_FUNC_\(.*\)%%$')
            if ! test -z "$func" && ! test "$func" = 0
            then
                functions="$func $v\nexport -f $func\n\n$functions"
            else
                v=$(grep -e "^$k=" $src | cut -f 2- -d= | sed -e 's/"/\\"/g')
                echo "export $k=\"$v\""
            fi
        done
        printf "\n"

        # add functions if any were found
        if ! test -z "$functions"
        then
            echo "# functions" | sed -e 's/\\n/\n/g'
            echo "$functions"  | sed -e 's/\\n/\n/g'
        fi

        # run all remaining arguments as `pre_exec` commands
        if ! test -z "$pre"
        then
            printf "\n# pre_exec_env\n"
            for pe in $pre
            do
                printf "$pe"
            done
            printf "\n"
        fi
        printf "\n"

        printf "# end\n"
    }

    env=$(_prep)

    test -z "$tgt" && echo  "$env"
    test -z "$tgt" || echo  "$env" > $tgt
    test -z "$tmp" || rm -f "$tmp"
}


# ------------------------------------------------------------------------------
#
check(){
    # run a given command, log stdout/stderr if requested (otherwise
    # leave unaltered), and exit on errors.  Following flags are used
    #
    #   -o <file>  : redirect stdout to <file>
    #   -e <file>  : redirect stderr to <file>
    #   -f         : fail on error (`exit $retval`)
    #

    errfile=''
    outfile=''
    failerr=0

    while getopts o:e:f OPT
    do
        case $OPT in
            o)  outfile="$OPTARG";;
            e)  errfile="$OPTARG";;
            f)  failerr=1;;
            -)  break;;
        esac
    done
    shift $(($OPTIND - 1))
    cmd="$*"

    test -z "$outfile" && outfile=1
    test -z "$errfile" && errfile=2

    if   test -z "$outfile" -a -z "$errfile"; then ($cmd)
    elif test -z "$outfile"                 ; then ($cmd) 2>> "$errfile"
    elif test -z "$errfile"                 ; then ($cmd) 1>> "$outfile"
    else                                           ($cmd) 1>> "$outfile" \
                                                          2>> "$errfile"
    fi

    ret=$?

    if   test "$ret"     = 0; then return $ret
    elif test "$failerr" = 1; then exit   $ret
    else                           return $ret
    fi

}


# ------------------------------------------------------------------------------
#
sync_n(){

    # sync 'n' (see `-n`) instances by waiting for 'n' 'READY' signals
    # (presumably one from each instance), and then issuing one 'GO' signal to
    # whoever is interested.  The instances are not required to live in the same
    # OS image, and we thus cannot rely on actual UNIX signals.  Instead, we use
    # the (presumably shared) file system: the 'READY' signal appends a line to
    # the 'ready' file (see `-f`), the 'GO' signal creates removes the `ready`
    # file.

    #   -n <int>   : number of instances to sync
    #   -f <file>  : file used for signalling

    f=
    n=
    while getopts n:f: OPT
    do
        case $OPT in
            n)  n="$OPTARG";;
            f)  f="$OPTARG";;
        esac
    done

    test -z "$n" && echo "ERROR: missing option -n"
    test -z "$n" && return 1
    test -z "$f" && echo "ERROR: missing option -f"
    test -z "$f" && return 1

    while true
    do
        test -f "$f" -a $(wc -l "$f") = "$n" && break
        sleep 0.1   # assume we can sleep for <float> seconds
    done

    rm -f "$f"

}


# ------------------------------------------------------------------------------
#
env_deactivate(){
    # provide a generic deactivate which attempts to move the current shell out
    # of any activated virtualenv or conda env

    # deactivate active conda
    conda=$(which conda 2>/dev/null)
    if ! test -z "$conda"
    then
        eval "$(conda shell.posix hook)"
        while test "$CONDA_SHLVL" -gt "0"
        do
            conda deactivate
        done
    fi

    # old style conda
    has_deactivate=$(which deactivate 2>/dev/null)
    if ! test -z "$has_deactivate"
    then
        . deactivate
    fi

    # deactivate active virtualenv
    has_deactivate=$(set | grep -e '^deactivate\s*()')
    if ! test -z "$has_deactivate"
    then
        deactivate
    fi

    # as a fallback, check if `$VIRTUAL_ENV` is set and dactivate manually
    if ! test -z "$VIRTUAL_ENV"
    then
        # remove the `$PATH` entry if it exists
        REMOVE="$VIRTUAL_ENV/bin"
        PATH=$(echo $PATH | tr ":" "\n" | grep -v "$REMOVE" | tr "\n" ":")
        export PATH
        unset  VIRTUAL_ENV
    fi

    # conda env fallback
    if ! test -z "$CONDA_PREFIX"
    then
        # remove the `$PATH` entry if it exists
        REMOVE="$CONDA_PREFIX/bin"
        PATH=$(echo $PATH | tr ":" "\n" | grep -v "$REMOVE" | tr "\n" ":")
        export PATH
        unset  CONDA_PREFIX
    fi

    # check for other conda levels
    prefixes=$(env | cut -f 1 -d '=' | grep CONDA_PREFIX_)
    for prefix in $prefixes
    do
        # remove the `$PATH` entry if it exists
        REMOVE="$prefix/bin"
        export PATH=$(echo $PATH | tr ":" "\n" | grep -v "$REMOVE" | tr "\n" ":")
        unset  $prefix
    done

    # clean out CONDA env vars
    unset  CONDA_DEFAULT_ENV
    unset  CONDA_PROMPT_MODIFIER
}


# ------------------------------------------------------------------------------
# env_prep -s ed1.env -d ed2.env -t ed3.sh  "echo foo bar" "echo buz"

