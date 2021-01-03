

# The methods defined here reflect the capabilities to dump and prepare
# environments as implemented in `00_env_isolation.py`, but are, obviously, for
# use in shell scripts.  Specifically `05_env_isolation_wrapper.sh` uses these
# methods to temporarily escape the LM env before running `pre_exec_cmd`
# directives.

# do not export blacklisted env variables
BLACKLIST="PS1 LS_COLORS _"


# ------------------------------------------------------------------------------
#
env_dump(){

    # Note that this capture will result in an unquoted dump where the values
    # can contain spaces, quotes (double and single), non-printable characters
    # etc.  The guarantees we have are:
    #
    #   - variable names begin with a letter, and contain letters, numbers and
    #     underscores.  From POSIX:
    #
    #       "Environment variable names used by the utilities in the Shell and
    #       Utilities volume of IEEE Std 1003.1-2001 consist solely of uppercase
    #       letters, digits, and the '_' (underscore) from the characters
    #       defined in Portable Character Set and do not begin with a digit."
    #
    #     Note that implementations usually also support lowercase letters, so
    #     we'll have to support that, too (event though it is rarely used for
    #     exported system variables).
    #
    #   - variable values can have any character.  Again POSIX:
    #
    #       "For values to be portable across systems conforming to IEEE Std
    #       1003.1-2001, the value shall be composed of characters from the
    #       portable character set (except NUL [...])."
    #
    # So the rules for names are strict, for values they are, unfortunately,
    # loose.  Specifically, values can contain unprintable characters and also
    # newlines.  While the Python equivalent of `env_prep` handles that case
    # well, the shell implementation below will simply ignore any lines which do
    # not start with a valid key.

    tgt=''
    while ! test -z "$1"; do
        case "$1" in
            -t) tgt="u$2"; shift 1 ;;
            * ) echo "invalid option $1"; return 1
        esac
    done

    test -z "$tgt" && env | sort
    test -z "$tgt" && env | sort > $tgt
}


# ------------------------------------------------------------------------------
#
env_prep(){

    src=''
    tgt=''
    rem=''
    pre=''
    while ! test -z "$1"; do
        case "$1" in
            -s) src="$2"      ; shift 2 ;;
            -t) tgt="$2"      ; shift 2 ;;
            -r) rem="$2"      ; shift 2 ;;
            * ) pre="$pre$1\n"; shift 1 ;;
        esac
    done

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


    if test -z "$src"
    then
        echo "missing 'src' -- prepare env from current env"
        tmp=$(mktemp)
        env > "$tmp"
        src="$tmp"
    fi

    # get keys from `src` environment dump
    src_keys=$( cat "$src" \
               | sort \
               | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
               | cut -f1 -d=
               )

    if ! test -z "$rem"
    then
        # get keys from `rem` environment dump
        rem_keys=$( cat "$rem" \
                  | sort \
                  | grep -e '^[A-Za-z_][A-Za-z_0-9]*=' \
                  | cut -f1 -d=
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


        # export all keys from `src`
        printf "\n# export\n"
        for k in $src_keys
        do
            # exclude blacklisted keys
            if ! expr "$BLACKLIST" : ".*\<$k\>.*" >/dev/null
            then
                bv=$(grep -e "^$k=" $src | cut -f 2- -d= | sed -e 's/"/\\"/g')
                echo "export $k=\"$bv\""
            fi
        done
        printf "\n"

        # run all remaining arguments as `pre_exec` commands
        if ! test -z "$pre"
        then
            printf "\n# pre_exec_env\n"
            for pe in "$pre"
            do
                printf "$pe"
            done
            printf "\n"
        fi
        printf "\n"
    }

    env=$(_prep)

    test -z "$tgt" && echo  "$env"
    test -z "$tgt" || echo  "$env" > $tgt

    test -z "$tmp" || rm -f "$tmp"
}


# ------------------------------------------------------------------------------

# # example usage
# export FOO="foo\"bar\"buz"
# env_dump ed1.env
#
# export BAR="foo\"bar\"buz"
# env_dump ed2.env
#
# env_prep -s ed1.env -d ed2.env -t ed3.sh  "echo foo bar" "echo buz"

