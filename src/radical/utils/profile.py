
import os
import csv
import time

from .ids     import get_radical_base
from .misc    import as_string, as_list
from .misc    import get_env_ns      as ru_get_env_ns
from .misc    import get_hostname    as ru_get_hostname
from .misc    import get_hostip      as ru_get_hostip
from .config  import DefaultConfig
from .atfork  import atfork


# ------------------------------------------------------------------------------
#
# We store profiles in CSV formatted files.
# The CSV field names are defined here:
#
TIME         = 0  # time of event (float, seconds since epoch)  mandatory
EVENT        = 1  # event ID (string)                           mandatory
ENTITY       = 2  # type of entity involved                     optional
UID          = 3  # uid of entity involved                      optional
MSG          = 4  # message describing the event                optional


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


# ------------------------------------------------------------------------------
#
def _sync_ntp():

    # read from disk cache
    t_now = time.time()
    try:
        with open('%s/ntp.cache' % get_radical_base('utils'), 'r') as fin:
            data  = as_string(fin.read()).split()
            t_sys = float(data[0])
            t_ntp = float(data[1])

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

        time  : mandatory, float,  time in seconds since epoch
        event : mandatory, string, short, unique name of event to be recorded
        entity: optional,  string, type of entity involved (when available)
        uid   : optional,  string, ID   of entity involved (when available)
        msg   : optional,  string, message describing the event (optional)

    Strings MUST NOT contain commas.  Otherwise they are encouraged to be formed
    as `[a-z][0-9a-z_.]*'. `msg` are free-form, but the inhibition of comma
    holds.  We propose to limit the sum of strings to about 256 characters -
    this will guarantee atomic writes on most OS's, w/o additional locking
    overheads.

    The profile is enabled by setting environment variables.  For a profiler
    named `radical.utils`, the following env variables will be evaluated:

        RADICAL_UTILS_PROFILE
        RADICAL_PROFILE

    If either is present in the environemnt, the profile is enabled (the value
    of the setting is ignored).
    '''

    fields  = ['time', 'event', 'entity', 'uid', 'msg']

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, ns=None, path=None):
        '''
        Open the file handle, sync the clock, and write timestam_zero
        '''

        # list of registered events
        self._registry = list()

        ru_def = DefaultConfig()

        if not ns:
            ns = name

        # check if this profile is enabled via an env variable
        self._enabled  = ru_get_env_ns('profile', ns)

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


        # write time normalization info
        self.prof(event='sync',
                  msg='%s:%s:%s:%s:%s' % (ru_get_hostname(),
                                          ru_get_hostip(),
                                          self._ts_zero,
                                          self._ts_abs,
                                          self._ts_mode))


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()
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
    def register(self, events):

        self._registry.extend(as_list(events))


    # --------------------------------------------------------------------------
    #
    def close(self):

        try:
            if not self._enabled:
                return

            if not self._handle:
                self._enabled = False
                return

            if self._enabled and self._handle:
                self.prof('END')
                self.flush()
                self._handle.close()
                self._handle = None

                self._enabled = False

        except:
            pass


    # --------------------------------------------------------------------------
    #
    def flush(self, verbose=False):

        if not self._enabled:
            return

        # see https://docs.python.org/2/library/stdtypes.html#file.flush
        self._handle.flush()
        os.fsync(self._handle.fileno())


    # --------------------------------------------------------------------------
    #
    # FIXME: reorder args to reflect tupleorder (breaks API)
    #
    def prof(self, event, entity='', uid='', msg='', ts=None):

        if not self._enabled:
            return

        # do nothing for events which are not registered (optional)
        if self._registry and event not in self._registry:
            print('warn: drop %s' % event)
            return

        if ts is None:
            ts = self.timestamp()

        self._handle.write('%.7f,%s,%s,%s,%s\n' % (ts, event, entity, uid, msg))


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
            # on any errors, we fall back to system time
            t_sys = time.time()
            return [t_sys, t_sys, 'sys']


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
def read_profiles(profiles, sid=None, efilter=None):
    '''
    We read all profiles as CSV files and parse them.  For each profile,
    we back-calculate global time (epoch) from the synch timestamps.

    The caller can provide a filter of the following structure

        filter = {ru.EVENT: ['event 1', 'event 2', ...],
                  ru.MSG  : ['msg 1',   'msg 2',   ...],
                  ...
                 }

    Filters apply on *substring* matches!
    '''

  # import resource
  # print('max RSS       : %20d MB' % (resource.getrusage(1)[2]/(1024)))

    # FIXME: we correct one pesky profile entry, which is exactly 1.000 in an
    #        otherwise ntp-aligned profile - see [1].  In this case we use the
    #        previous timestamp (if available)
    #
    #    [1] https://github.com/radical-cybertools/radical.pilot/issues/1117

    if not efilter:
        efilter = dict()

    ret     = dict()
    last    = list()
    skipped = 0

    for prof in profiles:

        with open(prof, 'r') as csvfile:

            ret[prof] = list()
            reader    = csv.reader(csvfile)

            try:
                for raw in reader:

                    # we keep the raw data around for error checks
                    row       = list(raw)
                    row[TIME] = float(row[TIME])

                    if None in row:
                        print('row invalid [%s]: %s' % (prof, raw))
                        continue
                      # raise ValueError('row invalid [%s]: %s' % (prof, row))

                    # apply the filter.  We do that after adding the entity
                    # field above, as the filter might also apply to that.
                    skip = False
                    for field, pats in efilter.items():
                        for pattern in pats:
                            if row[field] in pattern:
                                skip = True
                                break
                        if skip:
                            continue

                    # fix rp issue 1117 (see FIXME above)
                    if row[TIME] == 1.0 and last:
                        row[TIME] = last[TIME]

                    if not skip:
                        ret[prof].append(row)

                    last = row

                  # print(' --- %-30s -- %-30s ' % (row[STATE], row[MSG]))
                  # if 'bootstrap_1' in row:
                  #     print(row)
                  #     print()
                  #     print('TIME    : %s' % row[TIME  ])
                  #     print('EVENT   : %s' % row[EVENT ])
                  #     print('COMP    : %s' % row[COMP  ])
                  #     print('TID     : %s' % row[TID   ])
                  #     print('UID     : %s' % row[UID   ])
                  #     print('STATE   : %s' % row[STATE ])
                  #     print('ENTITY  : %s' % row[ENTITY])
                  #     print('MSG     : %s' % row[MSG   ])

            except:
                raise
              # print('skip remainder of %s' % prof)
              # continue

    return ret


# ------------------------------------------------------------------------------
#
def combine_profiles(profs):
    '''
    We merge all profiles and sort by time.

    Time syncing is done based on 'sync' timestamps.  We expect one such
    absolute timestamp to be available per host (the first profile entry will
    contain host information).  All timestamps from the same host will be
    corrected by the respectively determined NTP offset.  We define an
    'accuracy' measure which is the maximum difference of clock correction
    offsets across all hosts.

    The method returnes the combined profile and accuracy, as tuple.
    '''

    syncs    = dict()  # profiles which have relative time refs
    t_host   = dict()  # time offset per host
    p_glob   = list()  # global profile
    t_min    = None    # absolute starting point of profiled session
    c_end    = 0       # counter for profile closing tag
    accuracy = 0.0     # max uncorrected clock deviation

    if len(profs) == 1:
        return list(profs.values())[0], accuracy

    # first get all absolute and relative timestamp sync from the profiles,
    # for all hosts
    for pname, prof in profs.items():

        syncs[pname] = list()

        if not len(prof):
            continue

        for entry in prof:
            if entry[EVENT] == 'sync': syncs[pname].append(entry)

  # for pname, prof in profs.items():
  #     if prof:
  #         print('check        %-100s: %s' % (pname, prof[0][TIME:EVENT]))

    for pname, prof in profs.items():

        if not len(prof):
          # print('empty        %s' % pname)
            continue

    # all profiles are rel-synced here.  Now we look at `sync` values to align
    # across hosts and to determine accuracy.
    for pname in syncs:

        for sync in syncs[pname]:

            # https://github.com/radical-cybertools/radical.analytics/issues/20
            if not sync[MSG] or ':' not in sync[MSG]:
              # print('unsynced profile %s [%s]' % (pname, sync))
                continue

            t_prof = sync[TIME]

            host, ip, t_sys, t_ntp, t_mode = sync[MSG].split(':')
            host_id = '%s:%s' % (host, ip)

            if t_min: t_min = min(t_min, t_prof)
            else    : t_min = t_prof

            if t_mode == 'sys':
              # print('sys synced profile (%s)' % t_mode)
                continue

            # determine the correction for the given host
            t_sys = float(t_sys)
            t_ntp = float(t_ntp)
            t_off = t_sys - t_ntp

            if  host_id in t_host and \
                t_host[host_id] != t_off:

                diff = t_off - t_host[host_id]
                accuracy = max(accuracy, diff)

                # we allow for *some* amount of inconsistency before warning
                if diff > NTP_DIFF_WARN_LIMIT:
                    print('conflicting time sync for %-45s (%15s): '
                          '%10.2f - %10.2f = %5.2f'
                        % (pname.split('/')[-1], host_id, t_off,
                           t_host[host_id], diff))
                    continue

            t_host[host_id] = t_off


    unsynced = set()
    # now that we can align clocks for all hosts, apply that correction to all
    # profiles
    for pname, prof in profs.items():

        if not len(prof):
          # print('empty prof: %s' % pname)
            continue

        if not syncs[pname]:
            print('unsynced %s' % pname)
            continue

        sync = syncs[pname][0]

        host, ip, _, _, _ = sync[MSG].split(':')
        host_id = '%s:%s' % (host, ip)
        if host_id in t_host:
            t_off = t_host[host_id]
        else:
            unsynced.add(host_id)
            t_off = 0.0

        t_0 = sync[TIME]
        t_0 -= t_min

        # correct profile timestamps
        for row in prof:

            row[TIME] -= t_min
            row[TIME] -= t_off

          # print(row[EVENT],)
            # count closing entries
            if row[EVENT] == 'END':
                c_end += 1

        # add profile to global one
        p_glob += prof

      # if prof:
      #     print('check        %-100s: %s' % (pname, prof[0][TIME:EVENT]))

        # Check for proper closure of profiling files
        if c_end == 0:
            print('WARNING: profile "%s" not correctly closed.' % pname)
      # elif c_end > 1:
      #     print('WARNING: profile "%s" closed %d times.' % (pname, c_end))

    # sort by time and return
    p_glob = sorted(p_glob[:], key=lambda k: k[TIME])

  # print('check        %-100s: %s' % ('t_min', p_glob[0][TIME]))
  # print('check        %-100s: %s' % ('t_max', p_glob[-1][TIME]))
    return p_glob, accuracy


# ------------------------------------------------------------------------------
#
def clean_profile(profile, sid, state_final=None, state_canceled=None):
    '''
    This method will prepare a profile for consumption in radical.analytics.
    It performs the following actions:

      - assignes the session uid to all events without uid
      - sort by time
    '''

    ret = list()
    for event in profile:

        if not event[UID]   : event[UID   ] = sid
        if not event[ENTITY]: event[ENTITY] = 'session'

        ret.append(event)

    return sorted(ret[:], key=lambda k: k[TIME])


# ------------------------------------------------------------------------------

