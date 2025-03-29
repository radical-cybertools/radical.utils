
import math
import time
import shlex
import queue

import threading as mt

from rc.process  import Process
from functools   import partial
from collections import defaultdict
from typing      import List, Dict, Any

from .misc       import as_list
from .which      import which
from .ids        import generate_id, ID_SIMPLE
from .logger     import Logger
from .modules    import import_module


try:
    _flux     = import_module('flux')
    _flux_job = import_module('flux.job')
    _flux_exc = None

except Exception as e:
    _flux     = None
    _flux_job = None
    _flux_exc = e


# --------------------------------------------------------------------------
#
def spec_from_command(cmd: str) -> 'flux.job.JobspecV1':

    spec = _flux_job.JobspecV1.from_command(shlex.split(cmd))
    spec.attributes['user']['uid'] = generate_id(ID_SIMPLE)

    return spec


# --------------------------------------------------------------------------
#
def spec_from_dict(td: dict) -> 'flux.job.JobspecV1':

    version = 1
    user    = {'uid'     : td.get('uid', generate_id(ID_SIMPLE))}
    system  = {'duration': td.get('duration', 0.0)}
    tasks   = [{'command': [td['executable']] + td.get('arguments', []),
                'slot'   : 'task',
                'count'  : {'per_slot': 1}}]

    if 'environment' in td: system['environment'] = td['environment']
    if 'sandbox'     in td: system['cwd']         = td['sandbox']
    if 'shell'       in td: system['shell']       = td['shell']
    if 'stdin'       in td: system['stdin']       = td['stdin']
    if 'stdout'      in td: system['stdout']      = td['stdout']
    if 'stderr'      in td: system['stderr']      = td['stderr']

    attributes = {'system' : system,
                  'user'   : user}
    resources  = [{'count': td.get('ranks', 1),
                   'type' : 'slot',
                   'label': 'task',
                   'with' : [{
                       'count': int(td.get('cores_per_rank', 1)),
                       'type' : 'core'}]}]
                 #     'count': int(td.get('gpus_per_rank', 0)) or None,
                 #     'type' : 'gpu'

    gpr = td.get('gpus_per_rank', 0)
    if gpr:
        resources[0]['with'].append({'count': math.ceil(gpr),  # flux needs int
                                     'type' : 'gpu'})

    spec = _flux_job.JobspecV1(resources=resources,
                               attributes=attributes,
                               tasks=tasks,
                               version=version)
    return spec


# ------------------------------------------------------------------------------
#
class FluxService(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid     : str    = None,
                       log     : Logger = None,
                       launcher: str    = None
                       ) -> None:

        self._uid      = uid      or generate_id('ru.flux')
        self._log      = log      or Logger('radical.utils.flux')
        self._launcher = launcher or ''

        self._fexe  = which('flux')
        self._uri   = None
        self._proc  = None
        self._ready = mt.Event()

        if not _flux:
            raise RuntimeError('flux module not found') from self._exception

        if not _flux_job:
            raise RuntimeError('flux.job module not found') from self._exception

        if not self._fexe:
            raise RuntimeError('flux executable not found')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:
        return self._uid

    @property
    def uri(self) -> str:
        return self._uri


    # --------------------------------------------------------------------------
    #
    def _proc_line_cb(self, prefix: str,
                            proc  : Process,
                            lines : List[str]
                     ) -> None:

        for line in lines:
            if line.startswith('FLUX_URI:'):
                self._uri = line.strip().split(':', 1)[1]
                self._log.info('%s: found flux uri: %s', self._uid, self.uri)
                self._ready.set()


    # --------------------------------------------------------------------------
    #
    def _proc_state_cb(self, proc: Process, state: str) -> None:

        self._log.info('flux instance state update: %s', state)
        if state in Process.FINAL:

            self._log.info('flux instance stopped: %s', state)
            self.stop()


    # --------------------------------------------------------------------------
    #
    def start(self, timeout: float = None) -> None:

        fcmd = 'echo FLUX_URI:\\$FLUX_URI && sleep inf'
        cmd  = '%s start bash -c "%s"' % (self._fexe, fcmd)

        if self._launcher:
            cmd = '%s %s' % (self._launcher, cmd)

        self._log.info('%s: start flux instance: %s', self._uid, cmd)

        p = Process(cmd)
        p.register_cb(p.CB_OUT_LINE, partial(self._proc_line_cb, 'out'))
        p.register_cb(p.CB_ERR_LINE, partial(self._proc_line_cb, 'err'))
        p.register_cb(p.CB_STATE, self._proc_state_cb)
        p.polldelay = 0.1
        p.start()

        self._proc  = p
        self._ptime = time.time()

        return self.ready(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def ready(self, timeout: float = None) -> None:

        if timeout is not None:
            if timeout < 0: self._ready.wait()
            else          : self._ready.wait(timeout)

        return self._ready.is_set()


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        if not self._proc:
            return

        self._proc.cancel()
        self._proc.wait()

        self.uri   = None
        self._proc = None

        self._log.info('%s: found flux uri: %s', self._uid, self.uri)


# ------------------------------------------------------------------------------
#
class FluxHelper(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str,
                       log : Logger = None) -> None:

        self._t0 = time.time()

        self._uri      = uri
        self._log      = log  or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')
        self._handle   = _flux.Flux(self._uri)

        # journal watcher
        self._jthread  = None

        # event handle thread
        self._ethread  = None
        self._equeue   = queue.Queue()

        # submit thread
        self._sthread  = None
        self._squeue   = queue.Queue()
        self._sevent   = mt.Event()

        self._idlock   = mt.Lock()          # lock ID dicts
        self._elock    = mt.Lock()          # lock event dict
        self._task_ids = dict()             # flux ID -> task ID
        self._flux_ids = dict()             # task ID -> flux ID
        self._events   = defaultdict(list)  # flux ID -> event list
        self._cbacks   = list()             # list of callbacks

        if not _flux:
            raise RuntimeError('flux module not found') from _flux_exc

        if not _flux_job:
            raise RuntimeError('flux.job module not found') from _flux_exc


    # --------------------------------------------------------------------------
    #
    def start(self, launcher: str   = None) -> None:

        if self._jthread is not None:
            return

        self._jthread = mt.Thread(target=self._jwatcher)
        self._jthread.daemon = True
        self._jthread.start()

        self._ethread = mt.Thread(target=self._ewatcher)
        self._ethread.daemon = True
        self._ethread.start()

        self._sthread = mt.Thread(target=self._swatcher)
        self._sthread.daemon = True
        self._sthread.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        if self._handle is None:
            self._jterm.set()
            self._jthread.join()

            # FIXME: shutdown flux instance
            self._flux_service = None
            self._uri          = None
            self._handle       = None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:
        return self._uid

    @property
    def uri(self) -> str:
        return self._uri


    # --------------------------------------------------------------------------
    #
    def _jwatcher(self):

        # NOTE: *never* used self._handle in this thread, as it is not thread
        #      safe.  Instead, use the private handle created here
        fh = _flux.Flux(self._uri)

        # start watching the event journal
        journal = _flux_job.JournalConsumer(fh)
        journal.start()

        while True:

            try:
                event = journal.poll(timeout=1.0)
                self._handle_events(fh, event.jobid, event)

            except TimeoutError:
                pass


    # --------------------------------------------------------------------------
    #
    def _swatcher(self):

        # if we get new specs, submit them, return IDs to iqueue, and also
        # forward ID to ewatcher
        fh = _flux.Flux(self._uri)
        while True:

            try:
                specs = self._squeue.get(block=True, timeout=1.0)

            except queue.Empty:
                continue

            except Exception:
                self._log.exception("exception")
                raise

            with self._idlock:
                try:

                    futs = list()
                    for spec in specs:
                        tid = spec.attributes['user']['uid']
                        fut = _flux_job.submit_async(fh, spec, waitable=True)

                        futs.append([fut, tid])

                    for fut, tid in futs:
                        fid = fut.get_id()
                        self._task_ids[fid] = tid
                        self._flux_ids[tid] = fid

                        # trigger an event check
                        self._equeue.put(fid)


                except Exception:
                    self._log.exception("exception")
                    raise

                finally:
                    # trigger submit completion
                    self._sevent.set()


    # --------------------------------------------------------------------------
    #
    def _ewatcher(self):

        # if we get a new job ID, check if we have events for it

        fh = _flux.Flux(self._uri)
        while True:

            try:
                fid = self._equeue.get(timeout=1.0)
                self._handle_events(fh, fid)

            except queue.Empty:
                continue


    # --------------------------------------------------------------------------
    #
    def register_cb(self, cb: callable) -> None:

        with self._elock:
            self._log.debug('==== register cb %s', cb)
            self._cbacks.append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_cb(self, cb: callable) -> None:

        with self._elock:
            self._cbacks.remove(cb)


    # --------------------------------------------------------------------------
    #
    def _handle_events(self, fh   : 'flux.Flux',
                             fid  : 'flux.job.JobID',
                             event: 'flux.job.journal.JournalEvent' = None
                      ) -> None:

        with self._elock:

            self._log.debug_9('==== event %s: %s', fid, event)

            # if triggered by submit, check if we have anything to do
            if not event:
                if fid not in self._events:
                    return

            # check if we can handle the event - otherwise store it
            if not self._cbacks:
                self._log.debug('==== no cb %s: %s', fid, event)
                self._events[fid].append(event)
                return

            # check if application knows the task - otherwise store the event
            if fid not in self._task_ids:
                self._log.debug('==== no id %s: %s', fid, event)
                self._events[fid].append(event)
                return

            # task is known, flush stored events
            for ev in self._events[fid]:
                self._log.debug('==== play  %s: %s - %s', fid, event, self._cbacks)
                for cb in self._cbacks:
                    cb(self._task_ids[fid], ev)
                self._events[fid] = []

            # process the current event
            if event:
                self._log.debug('==== relay %s: %s - %s', fid, event, self._cbacks)
                for cb in self._cbacks:
                    cb(self._task_ids[fid], event)


    # --------------------------------------------------------------------------
    #
    def submit(self, specs: List['flux.job.JobspecV1']) -> List[str]:

        if not self._handle:
            raise RuntimeError('flux instance not started')

        self._log.debug('submit %d specs', len(specs))
        tids = list()
        for spec in specs:
            tid = spec.attributes['user'].get('uid')
            if not tid:
                tid = generate_id(ID_SIMPLE)
                if 'user' not in spec.attributes:
                    spec.attributes['user'] = dict()
                spec.attributes['user']['uid'] = tid
            tids.append(tid)

        self._sevent.clear()
        self._squeue.put(specs)
        self._sevent.wait()  # FIXME: timeout?

        return tids


    # --------------------------------------------------------------------------
    #
    def cancel(self, tids: [str|List[str]]) -> None:

        if not self._handle:
            raise RuntimeError('flux instance not started')

        with self._idlock:
            for tid in as_list(tids):
                fid = self._flux_ids[tid]
                _flux_job.cancel_async(self._handle, fid, reason='user cancel')


    # --------------------------------------------------------------------------
    #
    def wait(self, tids: [str|List[str]]) -> None:

        if not self._handle:
            raise RuntimeError('flux instance not started')

        tids = as_list(tids)
        with self._idlock:
            fids = [self._flux_ids[tid] for tid in tids]

        for fid in fids:
            _flux_job.wait(self._handle, fid)


# ------------------------------------------------------------------------------

