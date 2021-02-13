
import os
import csv
import time
from types import FrameType

from .ids     import get_radical_base
from .misc    import as_string, as_list
from .misc    import get_env_ns      as ru_get_env_ns
from .misc    import get_hostname    as ru_get_hostname
from .misc    import get_hostip      as ru_get_hostip
from .threads import get_thread_name as ru_get_thread_name
from .config  import DefaultConfig
from .atfork  import atfork


# ------------------------------------------------------------------------------
#
# We store profiles in CSV formatted files.
# The CSV field names are defined here:
#
TIME         = 0  # time of event (float, seconds since epoch)  mandatory
EVENT        = 1  # event ID (string)                           mandatory
COMP         = 2  # component which recorded the event          mandatory
TID          = 3  # uid of thread involved                      optional
UID          = 4  # uid of entity involved                      optional
STATE        = 5  # state of entity involved                    optional
MSG          = 6  # message describing the event                optional
ENTITY       = 7  # type of entity involved                     optional
PROF_KEY_MAX = 8  # iteration helper: `for _ in range(PROF_KEY_MAX):`

# Note that `ENTITY` is not written to the profile, but rather derived from the
# UID when reading the profiles.

# A previous incarnation of this class stored CSVs with the following columns:
#
# TIME       = 0  # time of event (float, seconds since epoch)  mandatory
# COMP       = 2  # component which recorded the event          mandatory
# TID        = 3  # uid of thread involved                      optional
# UID        = 4  # uid of entity involved                      optional
# STATE      = 5  # state of entity involved                    optional
# EVENT      = 1  # event ID (string)                           mandatory
# MSG        = 6  # message describing the event                optional

# ------------------------------------------------------------------------------
#
# when recombining profiles, we will get one NTP sync offset per profile, and
# thus potentially multiple such offsets per host.  If those differ more than
# a certain value (float, in seconds) from each other, we print a warning:
#
NTP_DIFF_WARN_LIMIT = 1.0

# syncing with the NTP host is expensive, so we only do it once in a while and
# cache the result.  We use a disk cache which is valid for 1 minute
NTP_CACHE_TIMEOUT = 60  # disk cache is valid for 60 seconds

# maximum field size allowed by the csv parser.  The larger the number of
# entities in the profile, the larger the size of the filed required by the
# csv parser. We assume a 64bit C long.
CSV_FIELD_SIZE_LIMIT = 9223372036854775807


_t_prev = time.time()
def tdiff(msg):
    global _t_prev
    now = time.time()
    print('RU %10.2f  :  %s' % (now - _t_prev, msg))
    _t_prev = now


# ------------------------------------------------------------------------------
#
def _sync_ntp():

    # read from disk cache
    try:
        with open('%s/ntp.cache' % get_radical_base('utils'), 'r') as fin:
            data  = as_string(fin.read()).split()
            t_sys = float(data[0])
            t_ntp = float(data[1])
            t_now = time.time()

    except:
        t_sys = None
        t_ntp = None


    # if disc cache is empty or old
    if t_sys is None or t_now - t_sys > NTP_CACHE_TIMEOUT:

        # refresh data
        import ntplib                                    # pylint: disable=E0401

        ntp_host = os.environ.get('RADICAL_UTILS_NTPHOST','0.pool.ntp.org')

        t_one = time.time()
        response = ntplib.NTPClient().request(ntp_host, timeout=1)
        t_two = time.time()

        t_sys = (t_one + t_two) / 2.0
        t_ntp = response.tx_time

        with open('%s/ntp.cache' % get_radical_base('utils'), 'w') as fout:
            fout.write('%f\n%f\n' % (t_sys, t_ntp))

    # correct both time stamps by current time
    t_cor  = time.time() - t_sys
    t_sys += t_cor
    t_ntp += t_cor

    return t_sys, t_ntp


# ------------------------------------------------------------------------------
#
# the profiler is not using threads and is generally threadsafe (all write ops
# should be atomic) - but alas Python threadlocks I/O streams, and those locks
# can still deadlock after fork:
#
#   - https://bugs.python.org/issue6721
#   - https://bugs.python.org/issue40399
#
# We thus have to close/reopen the prof file handle after fork.  This creates
# a bit of a mess as we now have to maintain a global list of profiler instances
# to clean up after fork... :-/
#
_profilers = list()


def _atfork_prepare():
    pass


def _atfork_parent():
    pass


def _atfork_child():
    global _profilers
    for prof, fname in _profilers:
        prof._handle = open(fname, 'a', buffering=1024)


atfork(_atfork_prepare, _atfork_parent, _atfork_child)


# ------------------------------------------------------------------------------
#
class Profiler(object):
    '''
    This class is really just a persistent file handle with a convenience call
    (prof()) to write lines timestamped events.  Any profiling intelligence must
    be applied when reading and evaluating the created profiles.  the following
    fields are defined for each event:

        time : mandatory, float,  time in seconds since epoch
        event: mandatory, string, short, unique name of event to be recorded
        comp : optional,  string, name of component where the event originates
        tid  : optional,  string, current thread id (name)
        uid  : optional,  string, ID of entity involved (when available)
        state: optional,  string, state of entity involved, if applicable
        msg  : optional,  string, free for message describing the event

    Strings MUST NOT contain commas.  Otherwise they are encouraged to be formed
    as `[a-z][0-9a-z_.]*'. `msg` are free-form, but the inhibition of comma
    holds.  We propose to limit the sum of strings to about 256 characters -
    this will guarantee atomic writes on most OS's, w/o additional locking
    overheads.  Less than 100 charcters makes the profiles almost
    human-readable.

    The profile is enabled by setting environment variables.  For a profiler
    named `radical.utils`, the following env variables will be evaluated:

        RADICAL_UTILS_PROFILE
        RADICAL_PROFILE

    If either is present in the environemnt, the profile is enabled (the value
    of the setting is ignored).
    '''

    fields  = ['time', 'event', 'comp', 'thread', 'uid', 'state', 'msg']

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, ns=None, path=None):
        '''
        Open the file handle, sync the clock, and write timestam_zero
        '''

        ru_def = DefaultConfig()

        if not ns:
            ns = name

        # check if this profile is enabled via an env variable
        self._enabled = ru_get_env_ns('profile', ns)

        if  self._enabled is None:
            self._enabled = ru_def.get('profile')

        if self._enabled is None:
            self._enabled = 'False'

        if self._enabled.lower() in ['0', 'false', 'off']:
            self._enabled = False
            return

        # profiler is enabled - set properties, sync time, open handle
        self._enabled = True
        self._path    = path
        self._name    = name

        if not self._path:
            self._path = ru_def['profile_dir']

        self._ts_zero, self._ts_abs, self._ts_mode = self._timestamp_init()

        try:
            os.makedirs(self._path)
        except OSError:
            pass  # already exists

        # we set `buffering` to `1` to force line buffering.  That is not idea
        # performance wise - but will not do an `fsync()` after writes, so OS
        # level buffering should still apply.  This is supposed to shield
        # against incomplete profiles.
        fname = '%s/%s.prof' % (self._path, self._name)
        self._handle = open(fname, 'a', buffering=1024)

        # register for cleanup after fork
        global _profilers
        _profilers.append([self, fname])


        # write header and time normalization info
        if self._handle:
            self._handle.write('%.7f,%s,%s,%s,%s,%s,%s\n' %
                           (self.timestamp(), 'sync_abs', self._name,
                            ru_get_thread_name(), '', '',
                            '%s:%s:%s:%s:%s' % (ru_get_hostname(),
                                                ru_get_hostip(),
                                                self._ts_zero,
                                                self._ts_abs,
                                                self._ts_mode)))


    # --------------------------------------------------------------------------
    #
    def __del__(self):

      # self.close()
        pass


    # --------------------------------------------------------------------------
    #
    @property
    def enabled(self):

        return self._enabled


    @property
    def path(self):

        return self._path


    # --------------------------------------------------------------------------
    #
    def enable(self):  self._enabled = True
    def disable(self): self._enabled = False


    # --------------------------------------------------------------------------
    #
    def close(self):

        try:
            if not self._enabled:
                return

            if self._enabled and self._handle:
                self.prof('END')
                self.flush()
                self._handle.close()
                self._handle = None

        except:
            pass


    # --------------------------------------------------------------------------
    #
    def flush(self, verbose=False):

        if not self._enabled: return
        if not self._handle : return

        if verbose:
            self.prof('flush')

        # see https://docs.python.org/2/library/stdtypes.html#file.flush
        self._handle.flush()
        os.fsync(self._handle.fileno())


    # --------------------------------------------------------------------------
    #
    # FIXME: reorder args to reflect tupleorder (breaks API)
    #
    def prof(self, event, uid=None, state=None, msg=None, ts=None, comp=None,
                   tid=None):

        if not self._enabled: return
        if not self._handle : return

        if ts    is None: ts    = self.timestamp()
        if comp  is None: comp  = self._name
        if tid   is None: tid   = ru_get_thread_name()
        if uid   is None: uid   = ''
        if state is None: state = ''
        if msg   is None: msg   = ''

        # if uid is a list, then recursively call self.prof for each uid given
        for _uid in as_list(uid):

            data = '%.7f,%s,%s,%s,%s,%s,%s\n' \
                    % (ts, event, comp, tid, _uid, state, msg)
            self._handle.write(data)
            self._handle.flush()


    # --------------------------------------------------------------------------
    #
    def _timestamp_init(self):
        '''
        return a tuple of [system time, absolute time]
        '''

        # retrieve absolute timestamp from an external source
        #
        # We first try to contact a network time service for a timestamp, if
        # that fails we use the current system time.
        try:
            ts_sys, ts_ntp = _sync_ntp()
            return [ts_sys, ts_ntp, 'ntp']

        except:
            pass

        # on any errors, we fall back to system time
        t = time.time()
        return [t,t, 'sys']


    # --------------------------------------------------------------------------
    #
    def timestamp(self):

        return time.time()


# --------------------------------------------------------------------------
#
def timestamp():

    return time.time()


# ------------------------------------------------------------------------------
#
def read_profiles(profiles, sid=None):
    '''
    Use `datatables.fread` to read the CVS profiles into data tables.
    The `time` column is auto-converted to float.  The resulting tables
    are converted into pandas data frames.
    '''

    tdiff('read reset')
    from datatable import dt, fread

    dt.options.progress.enabled = True

  # columns = ['time'    , 'event' , 'comp', 'thread',    'uid',  'state',    'msg']
    columns = [dt.float64, dt.str32,   None,     None, dt.str32, dt.str32, dt.str32]
    names   = {'C0' : 'time',
               'C1' : 'event',
             # 'C2' : 'comp',
             # 'C3' : 'thread',
               'C2' : 'uid',
               'C3' : 'state',
               'C4' : 'msg'}

    ret = dict()
    for pname in profiles:
        tdiff('read  %s' % pname)
        table = fread(pname, columns=columns, na_strings=[''], fill=True,)

        if not table.nrows:
            continue

        table.names = names
        table['entity'] = str()
        tdiff('table read %s' % pname)

      # for i in range(table.nrows):
      #     import sys
      #     sys.stdout.write('.')
      #     sys.stdout.flush()
      #     uid = table[i, 2]
      #     if uid:
      #         if '.' in uid:
      #             etype, _ = uid.split('.', 1)
      #         else:
      #             etype = uid
      #     else:
      #         etype = 'session'
      #
      #     table[i, 5] = etype

      # if table.nrows > 3:
      #     print(table)

        tdiff('table fix %s' % pname)
        ret[pname] = table
        tdiff('frame %s' % pname)

    tdiff('read done')

    return ret


# ------------------------------------------------------------------------------
#
def combine_profiles(profs):
    '''
    We merge all profiles and sort by time.

    This routine expects all profiles to have a synchronization time stamp
    (`sync_abs`) We expect one such absolute timestamp to be available per host
    (the first profile entry will contain host information).  All timestamps
    from the same host will be corrected by the respectively determined NTP
    offset.  We define an 'accuracy' measure which is the maximum difference of
    clock correction offsets across all hosts.  All time stamps are then
    0-aligned, so that the smalles timestamp in the returned data frame is 0.0.

    The method returnes the combined profiles (a single pandas data frame)
    and accuracy, as tuple.
    '''

    from datatable import dt

    tdiff('combine start')
    syncs    = dict()  # profiles which have relative time refs
    t_host   = dict()  # time offset per host
    t_min    = None    # absolute starting point of profiled session
    accuracy = 0       # max uncorrected clock deviation

    # we expect exactly one `sync_abs` event per profile - all additional ones are
    # ignored.  This implies that all events in that profile originate on the
    # same host.  Not that `sync_abs` is always the first event in a profile, we
    # can thus use it to determine t_min
  # for pname, prof in profs.items():
  #
  #     tdiff('sync start %s' % pname)
  #     # add an `entity` column to each profile table
  #     prof['entity'] = str()
  #
  #     t_off = 0.0
  #
  #     if not prof.nrows:
  #       # print('WARNING: skip empty profile %s' % pname)
  #         syncs[pname] = t_off
  #         continue
  #
  #     sync = prof.iloc[0]
  #   # print(sync)
  #   # print('==', sync[EVENT])
  #   # print()
  #
  #     if t_min is None: t_min =     sync[TIME]
  #     else            : t_min = min(sync[TIME], t_min)
  #
  #     if sync[EVENT] != 'sync_abs':
  #       # print('WARNING: unsynced   profile %s' % pname)
  #         pass
  #
  #     else:
  #         host, ip, t_sys, t_ntp, t_mode = sync[MSG].split(':')
  #
  #         host_id = '%s:%s' % (host, ip)
  #         t_off   = float(t_sys) - float(t_ntp)
  #
  #         syncs[pname] = t_off
  #
  #         if t_mode == 'sys':
  #           # print('sys synced profile (%s)' % t_mode)
  #             pass
  #
  #         else:
  #             # determine the correction for the given host
  #             if host_id not in t_host:
  #                 t_host[host_id] = t_off
  #
  #             else:
  #                 diff = t_off - t_host[host_id]
  #                 accuracy = max(accuracy, diff)
  #
  #                 # allow *some* amount of inconsistency before warning
  #                 if diff > NTP_DIFF_WARN_LIMIT:
  #                     print('conflicting time sync for %-45s (%15s): '
  #                           '%10.2f - %10.2f = %5.2f'
  #                        % (pname, host_id, t_off, t_host[host_id], diff))
  #                     continue
  #     tdiff('sync stop  %s' % pname)
  #
  # # apply t_min and t_off correction to all events
  # for pname, prof in profs.items():
  #
  #     if not prof.nrows:
  #         continue
  #
  #     t_fix = syncs[pname] - t_min
  #     prof.time += t_fix
  #
  #     end = prof.loc[prof['event'] == 'END']
  #     if not end.nrows:
  #       # print('WARNING: profile "%s" not correctly closed.' % pname)
  #         pass

    # combine, sort by time and return
    # FIXME

    tdiff('concat start')
    p_glob = dt.rbind(*(list(profs.values())))
  # p_glob = pandas.concat(profs.values())
    tdiff('concat')
  # p_glob.drop_duplicates(inplace=True)
  # tdiff('dedup')
    p_glob.sort('time')
    tdiff('sort')

    return p_glob, accuracy


# ------------------------------------------------------------------------------
#
def clean_profile(profile, sid, state_final=None, state_canceled=None):
    '''
    This method will prepare a profile for consumption in radical.analytics.
    It performs the following actions:

      - makes sure all events have a `ename` entry
      - remove all state transitions to `CANCELLED` if a different final state
        is encountered for the same uid
      - assignes the session uid to all events without uid
      - makes sure that state transitions have an `ename` set to `state`
    '''

    import numpy as np

    tdiff('clean start')
    state_final = as_list(state_final)

    # replace all NaN with ''
    profile['uid'].replace(np.nan, '')
    tdiff('replace nan')

    # 'advance' events are renamed to `state`
    profile['event'].replace('advance', 'state')
    tdiff('replace advance')

    # use sid as uid if that is not defined
    profile['uid'].replace('', sid)
    tdiff('replace ""')

    # extract entity type from uid
    etype = profile['uid'].str.split('.', n=1, expand=True)
    profile['entity'] = etype[0]
    tdiff('add entity')

    # `etype` for session id is `session` (not `rp.session`)
    profile['entity'].replace('rp', 'session', inplace=True)
    tdiff('replace rp')

    # FIXME: duplicated final events are ignored right now

    # convert to list of tuples
    # FIXME: this should be DFs all the way down...
    ret = list(profile.to_records(index=False))
    tdiff('as list')

    return ret


# ------------------------------------------------------------------------------
#
def event_to_label(event):

    if event[EVENT] == 'state':
        return event[STATE]
    else:
        return event[EVENT]


# ------------------------------------------------------------------------------

