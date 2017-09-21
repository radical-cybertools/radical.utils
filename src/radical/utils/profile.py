
import os
import csv
import sys
import glob
import time
import threading

from   .misc      import get_size        as ru_get_size
from   .misc      import name2env        as ru_name2env
from   .misc      import get_hostname    as ru_get_hostname
from   .misc      import get_hostip      as ru_get_hostip
from   .threads   import get_thread_name as ru_get_thread_name
from   .read_json import read_json       as ru_read_json


# ------------------------------------------------------------------------------
#
# We store profiles in CSV formatted files.  The CSV field names are defined
# here:
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

# ------------------------------------------------------------------------------
#
# when recombining profiles, we will get one NTP sync offset per profile, and
# thus potentially multiple such offsets per host.  If those differ more than
# a certain value (float, in seconds) from each other, we print a warning:
#
NTP_DIFF_WARN_LIMIT = 1.0


# ------------------------------------------------------------------------------
#
class Profiler(object):
    """
    This class is really just a persistent file handle with a convenience call
    (prof()) to write lines with timestamp and events.
    Any profiling intelligence is applied when reading and evaluating the
    created profiles.
    """

    fields  = ['time', 'comp', 'uid', 'state', 'event', 'msg']

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, env_name=None, path=None):
        """
        Open the file handle, sync the clock, and write timestam_zero
        """

        # use the profiler name as basis for the env check
        if not env_name:
            env_name = '%s' % ru_name2env(name)

        if not path:
            path = os.getcwd()

        self._path    = path
        self._name    = name
        self._enabled = False


        # example: for RADICAL_PILOT_COMPONENT, we check
        # RADICAL_PILOT_COMPONENT_PROFILE
        # RADICAL_PILOT_PROFILE
        # RADICAL_PROFILE
        # if any of those is set in env, the profiler is enabled
        env_elems = env_name.split('_')
        if env_elems[-1] == 'PROFILE':
            env_elems = env_elems[:-1]

        env_check = ''
        for elem in env_elems:
            env_check += '%s_' % elem
            if '%sPROFILE' % env_check in os.environ:
                self._enabled = True
                break

        # FIXME
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            self._enabled = True

        if not self._enabled:
            return

        # profiler is enabled - sync time and open handle
        self._ts_zero, self._ts_abs, self._ts_mode = self._timestamp_init()

        try:
            os.makedirs(self._path)
        except OSError:
            pass  # already exists

        self._handle = open("%s/%s.prof" % (self._path, self._name), 'a')

        # write header and time normalization info
        if self._handle:
            self._handle.write("#%s\n" % (','.join(Profiler.fields)))
            self._handle.write("%.4f,%s,%s,%s,%s,%s,%s\n" %
                           (self.timestamp(), 'sync_abs', self._name,
                            ru_get_thread_name(), '', '',
                            "%s:%s:%s:%s:%s" % (ru_get_hostname(),
                                                ru_get_hostip(),
                                                self._ts_zero,
                                                self._ts_abs,
                                                self._ts_mode)))


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    @property
    def enabled(self):

        return self._enabled


    # --------------------------------------------------------------------------
    #
    def close(self):

        if not self._enabled:
            return

        if self._enabled and self._handle:
            self.prof("END")
            self.flush(verbose=False)
            self._handle.close()
            self._handle = None


    # --------------------------------------------------------------------------
    #
    def flush(self, verbose=True):

        if not self._enabled:
            return

        if self._enabled:

            if verbose:
                self.prof("flush")

            # see https://docs.python.org/2/library/stdtypes.html#file.flush
            self._handle.flush()
            os.fsync(self._handle.fileno())


    # --------------------------------------------------------------------------
    #
    def prof(self, event, uid=None, state=None, msg=None, timestamp=None,
             comp=None):

        if not self._enabled: return

        if uid       is None: uid       = ''
        if state     is None: state     = ''
        if msg       is None: msg       = ''
        if timestamp is None: timestamp = self.timestamp()
        if comp      is None: comp      = self._name

        # if uid is a list, then recursively call self.prof for each uid given
        if isinstance(uid, list):
            for _uid in uid:
                self.prof(event=event, uid=_uid, state=state, msg=msg,
                          timestamp=timestamp, comp=comp)
            return

        tid = threading.current_thread().name

        try:
            if self._handle:
            #   if event == 'sync_abs':
            # TIME   = 0  # time of event (float, seconds since epoch)  mandatory
            # EVENT  = 1  # event ID (string)                           mandatory
            # COMP   = 2  # component which recorded the event          mandatory
            # TID    = 3  # uid of thread involved                      optional
            # UID    = 4  # uid of entity involved                      optional
            # STATE  = 5  # state of entity involved                    optional
            # MSG    = 6  # message describing the event                optional
            # ENTITY = 7  # type of entity involved                     optional
                self._handle.write("%.4f,%s,%s,%s,%s,%s,%s\n"
                        % (timestamp, event, comp, tid, uid, state, msg))
        except Exception as e:
            sys.stderr.write('profile write error: %s' % repr(e))
            sys.stderr.flush()


    # --------------------------------------------------------------------------
    #
    def _timestamp_init(self):
        """
        return a tuple of [system time, absolute time]
        """

        # retrieve absolute timestamp from an external source
        #
        # We first try to contact a network time service for a timestamp, if that
        # fails we use the current system time.
        try:
            import ntplib

            ntphost = os.environ.get('RADICAL_UTILS_NTPHOST', '0.pool.ntp.org')

            t_one = time.time()
            response = ntplib.NTPClient().request(ntphost, timeout=1)
            t_two = time.time()

            ts_ntp = response.tx_time
            ts_sys = (t_one + t_two) / 2.0
            return [ts_sys, ts_ntp, 'ntp']

        except Exception:
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
def read_profiles(profiles, sid=None, efilter=None):
    """
    We read all profiles as CSV files and parse them.  For each profile,
    we back-calculate global time (epoch) from the synch timestamps.

    The caller can provide a filter of the following structure

        filter = {ru.EVENT: ['event 1', 'event 2', ...],
                  ru.MSG  : ['msg 1',   'msg 2',   ...],
                  ...
                 }

    Filters apply on *substring* matches!
    """

  # import resource
  # print 'max RSS       : %20d MB' % (resource.getrusage(1)[2]/(1024))

    if not efilter:
        efilter = dict()

    ret     = dict()
    skipped = 0

    for prof in profiles:

        with open(prof, 'r') as csvfile:

            ret[prof] = list()
            reader    = csv.reader(csvfile)

            for raw in reader:

                # we keep the raw data around for error checks
                row = list(raw)

              # if 'sync_abs' in row:
              #     print
              #     print row
              #     print
              #     print 'TIME    : %s' % row[TIME ]
              #     print 'EVENT   : %s' % row[EVENT]
              #     print 'COMP    : %s' % row[COMP ]
              #     print 'TID     : %s' % row[TID  ]
              #     print 'UID     : %s' % row[UID  ]
              #     print 'STATE   : %s' % row[STATE]
              #     print 'MSG     : %s' % row[MSG  ]
              #     raise 'oops'

                try:

                    # skip header
                    if row[TIME].startswith('#'):
                        skipped += 1
                        continue

                    # make room in the row for entity type etc.
                    row.extend([None] * (PROF_KEY_MAX - len(row)))

                    row[TIME] = float(row[TIME])

                    # we derive entity type from the uid -- but funnel
                    # some cases into 'session' as a catch-all type
                    uid = row[UID]
                    if uid:
                        row[ENTITY] = uid.split('.',1)[0]
                    else:
                        row[ENTITY] = 'session'
                        row[UID]    = sid

                    # we should have no unset (ie. None) fields left - otherwise
                    # the profile was likely not correctly closed.
                    if None in row:
                        raise ValueError('row invalid [%s]: %s' % (prof, raw))

                    # apply the filter.  We do that after adding the entity
                    # field above, as the filter might also apply to that.
                    skip = False
                    for field, pats in efilter.iteritems():
                        for pattern in pats:
                            if pattern in row[field]:
                                skip = True
                                continue
                        if skip:
                            continue

                    if not skip:
                        ret[prof].append(row)

                  # print ' --- %-30s -- %-30s ' % (row[STATE], row[MSG])

                except Exception as e:
                    raise

    return ret


# ------------------------------------------------------------------------------
#
def combine_profiles(profs):
    """
    We merge all profiles and sort by time.

    This routine expects all profiles to have a synchronization time stamp.
    Two kinds of sync timestamps are supported: absolute (`sync_abs`) and
    relative (`sync_rel`).

    Time syncing is done based on 'sync_abs' timestamps.  We expect one such
    absolute timestamp to be available per host (the first profile entry will
    contain host information).  All timestamps from the same host will be
    corrected by the respectively determined NTP offset.  We define an
    'accuracy' measure which is the maximum difference of clock correction
    offsets across all hosts.

    The method returnes the combined profile and accuracy, as tuple.
    """

    pd_rel   = dict()  # profiles which have relative time refs
    t_host   = dict()  # time offset per host
    p_glob   = list()  # global profile
    t_min    = None    # absolute starting point of profiled session
    c_end    = 0       # counter for profile closing tag
    accuracy = 0       # max uncorrected clock deviation

    # first get all absolute timestamp sync from the profiles, for all hosts
    for pname, prof in profs.iteritems():

        if not len(prof):
            print 'empty profile %s' % pname
            continue

        if not prof[0][MSG] or ':' not in prof[0][MSG]:
            # FIXME:
            # https://github.com/radical-cybertools/radical.analytics/issues/20
          # print 'unsynced profile %s [%s]' % (pname, prof[0])
            continue

        t_prof = prof[0][TIME]

        host, ip, t_sys, t_ntp, t_mode = prof[0][MSG].split(':')
        host_id = '%s:%s' % (host, ip)

        if t_min: t_min = min(t_min, t_prof)
        else    : t_min = t_prof

        if t_mode == 'sys':
          # print 'sys synced profile (%s)' % t_mode
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
                print 'conflicting time sync for %-45s (%15s): ' \
                    + '%10.2f - %10.2f = %5.2f' % \
                      (pname.split('/')[-1], host_id, t_off, 
                       t_host[host_id], diff)
                continue

        t_host[host_id] = t_off


    unsynced = set()
    last = None
    # now that we can align clocks for all hosts, apply that correction to all
    # profiles
    for pname, prof in profs.iteritems():

        if not len(prof):
          # print 'empty prof: %s' % pname
            continue

        if not prof[0][MSG]:
          # print 'no sync msg: %s' % prof[0]
            continue

      # print MSG
      # print prof[0]
      # print prof[0][MSG]
      # print prof[0][MSG].split(':')
        host, ip, _, _, _ = prof[0][MSG].split(':')
        host_id = '%s:%s' % (host, ip)
        if host_id in t_host:
            t_off = t_host[host_id]
        else:
            unsynced.add(host_id)
            t_off = 0.0

        t_0 = prof[0][TIME]
        t_0 -= t_min

        # correct profile timestamps
        for row in prof:

            t_orig = row[TIME]

            row[TIME] -= t_min
            row[TIME] -= t_off

          # print row[EVENT],
            # count closing entries
            if row[EVENT] == 'END':
                c_end += 1

            last = row

        # add profile to global one
        p_glob += prof

        # Check for proper closure of profiling files
        if c_end == 0:
            print 'WARNING: profile "%s" not correctly closed.' % pname
      # elif c_end > 1:
      #     print 'WARNING: profile "%s" closed %d times.' % (pname, c_end)

  # if last:
  #     print 'TIME   : %s' % last[TIME  ]
  #     print 'EVENT  : %s' % last[EVENT ]   
  #     print 'COMP   : %s' % last[COMP  ]
  #     print 'UID    : %s' % last[UID   ]
  #     print 'STATE  : %s' % last[STATE ]
  #     print 'MSG    : %s' % last[MSG   ]
  #     print 'ENTITY : %s' % last[ENTITY]


    # sort by time and return
    p_glob = sorted(p_glob[:], key=lambda k: k[TIME])

    return p_glob, accuracy


# ------------------------------------------------------------------------------
#
def clean_profile(profile, sid, state_final, state_canceled):
    """
    This method will prepare a profile for consumption in radical.analytics.  It
    performs the following actions:

      - makes sure all events have a `ename` entry
      - remove all state transitions to `CANCELLED` if a different final state
        is encountered for the same uid
      - assignes the session uid to all events without uid
      - makes sure that state transitions have an `ename` set to `state`
    """

    entities = dict()  # things which have a uid

    if not isinstance(state_final, list):
        state_final = [state_final]

    for event in profile:

        uid   = event[UID  ]
        state = event[STATE]
        time  = event[TIME ]
        name  = event[EVENT]

        # we derive entity_type from the uid -- but funnel
        # some cases into the session
        if uid:
            event[ENTITY] = uid.split('.',1)[0]
        else:
            event[ENTITY] = 'session'
            event[UID]    = sid
            uid = sid

        if uid not in entities:
            entities[uid] = dict()
            entities[uid]['states'] = dict()
            entities[uid]['events'] = list()

        if name == 'advance':

            # this is a state progression
            assert(state), 'cannot advance w/o state'
            assert(uid),   'cannot advance w/o uid'

            # this is a state transition event
            event[EVENT] = 'state'  

            skip = False
            if state in state_final and state != state_canceled:

                # a final state other than CANCELED will cancel any previous
                # CANCELED state.
                if state_canceled in entities[uid]['states']:
                    del(entities[uid]['states'][state_canceled])

                # vice-versa, we will not add CANCELED if a final
                # state already exists:
                if state == state_canceled:
                    if any([s in entities[uid]['states']
                              for s in state_final]):
                        skip = True
                        continue

            if state in entities[uid]['states']:
                # ignore duplicated recordings of state transitions
                skip = True
                continue
              # raise ValueError('double state (%s) for %s' % (state, uid))

            if not skip:
                entities[uid]['states'][state] = event

        entities[uid]['events'].append(event)


    # we have evaluated, cleaned and sorted all events -- now we recreate
    # a clean profile out of them
    ret = list()
    for uid,entity in entities.iteritems():

        ret += entity['events']
        for state,event in entity['states'].iteritems():
            ret.append(event)

    # sort by time and return
    ret = sorted(ret[:], key=lambda k: k[TIME])

    return ret


# ------------------------------------------------------------------------------

