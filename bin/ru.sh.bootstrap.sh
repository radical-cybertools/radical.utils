#!/bin/sh

# The purpose of this script is to expose a shell over a network port.  This is
# intented to be used by the RU.SH bootstrapping routine: it will create such an
# endpoint, and then forward the tcp port over an ssh tunnel.
#
#
# Security Considerations
# -----------------------
#
# Opening a shell over a network port is *obviously* a bad idea.  We apply two
# measures to mitigate the security risks:
#
#   - the service endpoint will only listen on the local network interface, and
#     will thus not be directly reachable from the public internet.
#   - we don't interface directly to a shell, but rather to a wrapper.  That
#     wrapper will expect a secret as first string on all commands, and will
#     immediately terminate itself if an incorrect secret is provided.  That
#     secret will never live on the disk (but can be spotted via `/proc`.
#
# TODO: A  better security measures will be needed for production use:
#       use named pipe in `$TMP` and apply unix FS auth mechanisms.
#
#
# Implementation
# --------------
#
# This script is being staged to the target resource, and is then executed when
# an ssh tunnel is created.  It performs the following actions:
#
#   - find required tools and configure for common alternatives
#     - netcat / ncat / nc
#     - netstat / ss
#     - /bin/sh as sh, bash, dash
#   - check if a nc_sh_service is already active on this host (for this user,
#     from the originating host).  If so, check if the nc shell is still alive.
#     If so, don't start a new shell, but reconnect to it, and return the EP.
#     FIXME: can that lead to duplicated connections?
#   - check if secure shell wrapper script exists in $RUSH_HOME
#     (sec_shell_wrapper.sh).  Create it if not.
#   - create a unique named fifo in $RUSH_HOME (fifo_$$)
#   - find an open local port and connect that port to the fifo, via netcat
#     cat $FIFO | /bin/sh $WRAPPER -i 2>&1 | nc -l 127.0.0.1 $P > $FIFO
#   - check if that pipeline remains alive healthy (for one sec)
#   - echo the chosen port on stdout
#
# The client side of the tunnel will use that port information to open another
# tunnel to that port, thus essentially obtaining a secured, persistent, private
# and managed shell endpoint.  This can be used to further bootstrap the Python
# and ZMQ layer for async command execution.
#
# FIXME: we should also check if that ZMQ layer already exists andis running,
#        because we can skip most of the above in that case and then return
#        the ZMQ endpoint.
#
# FIXME: Do we need to lock against concurrent incarnations of this script?
#

SECRET="$1"
ORIGIN="$2"


# ------------------------------------------------------------------------------
#
# make sure we have all env settings we need
#
test -z "$UID"       && UID=$(id -u)
test -z "$TMP"       && TMP="/tmp/"
test -z "$RUSH_TMP"  && RUSH_TMP="$TMP/.radical/ru.sh/"
test -z "$RUSH_HOME" && RUSH_HOME="$HOME/ru.sh.$UID/"

mkdir -p "$RUSH_TMP"
mkdir -p "$RUSH_HOME"

RUSH_BS1="$RUSH_HOME/rush.bs.1.sh"


# ------------------------------------------------------------------------------
#
# create a shell script which executes commands on the netcat'ed port.  As
# motivated above, we don't invoke a shell directly, to (a) add some security,
# and (b) avoid the need for prompt parsing.
# This script is created no matter what, even if it already exists.  This allows
# for seemless upgrade of this bootstrapper.  We use a `mv` command to
# atomically replace any existing script.
#
cat >  "$RUSH_BS1.$$" \
    << EOT
#!/bin/sh
MD5_SECRET="$1"
PROMPT="rush.bs.1> "
if test -z "\$MD5_SECRET"
then
    echo "missing secret (md5)"
    exit 1
fi
echo "\$PROMPT"
while read -r SECRET CMD
do
    MD5="\$(echo -n "\$SECRET" | md5sum | cut -f 1 -d ' ')"
    if ! test "\$MD5" = "\$MD5_SECRET"
    then
        echo "secret mismatch - abort"
        exit 1
    fi
    # FIXME: add logging, tracing
    RET=\$(\$CMD)
    echo "\$RET"
    echo "\$PROMPT"
done
EOT
chmod 0700 "$RUSH_BS1.$$"
mv "$RUSH_BS1.$$" "$RUSH_BS1"


# ------------------------------------------------------------------------------
#
# find a free port and start a netcat'ed shell on it
#
PMIN=10000
PMAX=11000
P=$((PMIN-1))

# get list of ports which are used - we don't need to try those (ignore races)
used=$(netstat -lnt | awk '{print $4}' | awk -F : '{printf " %s ", $2}')

# create a fifo to put the shell behind
mkdir -p "$RUSH_HOME"
FIFO="$RUSH_HOME/ru_${UID}_$$.fifo"
rm -f $FIFO; mkfifo $FIFO

# loop over the portrange (excl. used ports) until we find a usable one
while test "$P" -lt "$PMAX"
do
    # increase to next port number, but skip if we know (knew) it's being used
    P=$((P+1))
    echo " $used " | grep " $P " 2>&1 > /dev/null
    if test "$?" = 0
    then
        echo "skip  $P [used]"
        continue
    fi

    # port could be free - try to start our netcat'ed shell
    echo "check $P"
    cat "$FIFO"                      \
        | "$RUSH_BS1" "$SECRET" 2>&1 \
        | nc -l 127.0.0.1 $P         \
        >  "$FIFO"                   \
        2> "$FIFO.err" &
    pid=$!

    # let it bootstrap, and check if it's still alive (send signal 0)
    # NOTE: we could sleep shorter - but POSIX says 'sleep arg is INT'
    sleep 1
    kill -0 "$pid"
    if test "$?" = 0
    then
        # the shell seems to be alive - we are done here
        break
    fi
done

if ! test "$P" -lt "$PMAX"
then
    # we either did not manage to find an open port in the given range, or the
    # shell did not come up for some other reason.  Either way, we bail out at
    # this point.
    echo "NC_SH_PORT:-"
else
    # wohoo!
    echo "NC_SH_PORT:$P"
fi


# ------------------------------------------------------------------------------

