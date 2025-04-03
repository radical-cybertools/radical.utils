
import os
import sys
import math
import time
import shlex
import queue

import threading as mt

from rc.process  import Process
from functools   import partial
from collections import defaultdict
from typing      import List, Dict, Any

from .url        import Url
from .misc       import as_list
from .which      import which
from .ids        import generate_id, ID_SIMPLE
from .logger     import Logger
from .modules    import import_module
from .shell      import sh_callout


# ------------------------------------------------------------------------------
#
def import_flux():
    '''
    import the flux module, if available

    returns: flux     : loaded python module (`None` if not available)
             flux.job : loaded python module (`None` if not available)
             exception: exception raised during import (`None` if no error)
             version  : what ru.FluxHelper version to use (`0` or `1`)
    '''

    flux     = None
    flux_job = None
    flux_exc = None
    flux_v   = None

    try:
        flux     = import_module('flux')
        flux_job = import_module('flux.job')
        if 'JournalConsumer' in dir(flux_job):
            flux_v = 1
        else:
            flux_v = 0

    except Exception as e:
        flux_exc = e


    # on failure, try to derive module path from flux executable
    if flux is None or flux_job is None:

        to_pop = None
        try:
            cmd = 'flux python -c "import flux; print(flux.__file__)"'
            out, err, ret = sh_callout(cmd)

            if not ret:
                flux_path = os.path.dirname(out.strip())
                mod_path  = os.path.dirname(flux_path)
                sys.path.append(mod_path)
                to_pop = mod_path

                flux     = import_module('flux')
                flux_job = import_module('flux.job')
                if 'JournalConsumer' in dir(flux_job):
                    flux_v = 1
                else:
                    flux_v = 0

        except Exception as e:
            flux_exc = e

        if to_pop:
            sys.path.remove(to_pop)

    return flux, flux_job, flux_exc, flux_v


_flux, _flux_job, _flux_exc, _flux_v = import_flux()


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
    user    = {'uid'     : td.get('uid', generate_id('ru_flux', ID_SIMPLE))}
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
        self._r_uri = None
        self._host  = None
        self._proc  = None
        self._ready = mt.Event()

        if not _flux:
            raise RuntimeError('flux module not found') from _flux_exc

        if not _flux_job:
            raise RuntimeError('flux.job module not found') from _flux_exc

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


    @property
    def r_uri(self) -> str:
        return self._r_uri


    # --------------------------------------------------------------------------
    #
    def _proc_line_cb(self, prefix: str,
                            proc  : Process,
                            lines : List[str]
                     ) -> None:

        try:
            for line in lines:
                self._log.info('=== line: %s', line)
                if line.startswith('FLUX_URI='):
                    parts = line.strip().split(' ', 1)
                    self._log.info('%s: found flux info: %s', self._uid, parts)

                    self._uri   = parts[0].split('=', 1)[1]
                    self._host  = parts[1].split('=', 1)[1]

                    url         = Url(self._uri)
                    url.host    = self._host
                    url.schema  = 'ssh'
                    self._r_uri = str(url)

                    self._log.info('%s: flux uri: %s', self._uid, self._uri)
                    self._log.info('%s:    r uri: %s', self._uid, self._r_uri)
                    self._ready.set()
        except:
            self._log.exception('line processing failed')


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

        fcmd = 'echo FLUX_URI=\\$FLUX_URI FLUX_HOST=\\$(hostname) && sleep inf'
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
class FluxHelperV0(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str,
                       log : Logger = None) -> None:

        self._t0 = time.time()

        self._uri      = uri
        self._log      = log or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')
        self._handle   = _flux.Flux(self._uri)
        self._api_lock = mt.Lock()
        self._exe      = _flux_job.executor.FluxExecutor(
                                               handle_kwargs={'url': self._uri})

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

        pass


    # --------------------------------------------------------------------------
    #
    def stop(self):

        with self._api_lock:

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
    def register_cb(self, cb: callable) -> None:

        with self._api_lock, self._elock:
            self._log.debug('register cb %s', cb)
            self._cbacks.append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_cb(self, cb: callable) -> None:

        with self._api_lock, self._elock:
            self._cbacks.remove(cb)


    # --------------------------------------------------------------------------
    #
    def _handle_events(self, fh   : 'flux.Flux',
                             fid  : 'flux.job.JobID',
                             event: 'flux.job.journal.JournalEvent' = None
                      ) -> None:

        self._log.debug('event for %s: %s', fid, event)
      # print('event %s: %s' % (fid, event))

        # if triggered by submit, check if we have anything to do
        if not event:
            if fid not in self._events:
              # print('no event')
                return

        # check if we can handle the event - otherwise store it
        if not self._cbacks:
          # print('no cbacks')
            self._events[fid].append(event)
            return

        # task is known, flush stored events
        for ev in self._events[fid]:
          # print('flush stored events')
            for cb in self._cbacks:
                try   : cb(fid, ev)
                except: self._log.exception('cb failed: %s')
            self._events[fid] = []

        # process the current event
        if event:
          # print('process current event')
            for cb in self._cbacks:
                try   : cb(fid, event)
                except: self._log.exception('cb failed: %s')


    # --------------------------------------------------------------------------
    #
    def submit(self, specs: List['flux.job.JobspecV1']) -> List[str]:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            self._log.debug('== submit %d specs', len(specs))

            events = ['submit', 'depend', 'alloc', 'start', # 'cleanup',
                      'finish', 'release', 'free', 'clean', 'priority', 'exception']

            def event_cb(fid, fut, event):
                self._handle_events(self._handle, fid, event)

            futures = list()
            def id_cb(fut):
                flux_id = fut.jobid()
                idx     = fut.ru_idx
                for ev in events:
                    tmp_cb = partial(event_cb, flux_id)
                    fut.add_event_callback(ev, tmp_cb)
                futures.append([flux_id, idx, fut])
                self._log.debug('got flux id: %s: %s', idx, flux_id)

            for idx, spec in enumerate(specs):
                fut      = self._exe.submit(spec, waitable=True)
                fut.ru_idx = idx
                self._log.debug('%s: submitted  : %s', self._uid, idx)
                fut.add_jobid_callback(id_cb)

            # wait until we saw all jobid callbacks (assume 10 tasks/sec)
            timeout = len(specs)
            timeout = max(100, timeout)
            start   = time.time()
            self._log.debug('%s: wait %.2fsec for %d flux IDs',
                            self._uid, timeout, len(specs))
            while len(futures) < len(specs):
                time.sleep(0.1)
                self._log.debug('%s: wait %s / %s', self._uid,
                                                     len(futures), len(specs))
                if time.time() - start > timeout:
                    raise RuntimeError('%s: timeout on submission', self._uid)
            self._log.info('got %d flux IDs', len(futures))

            # get flux_ids sorted by submission order (idx)
            flux_ids = [fut[0] for fut in sorted(futures, key=lambda x: x[1])]

            self._log.debug('%s: submitted: %s', self._uid, flux_ids)
            return flux_ids


    # --------------------------------------------------------------------------
    #
    def cancel(self, fids: [str|List[str]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            with self._idlock:
                for fid in as_list(fids):
                    _flux_job.cancel_async(self._handle, fid, reason='user cancel')


    # --------------------------------------------------------------------------
    #
    def wait(self, fids: [str|List[str]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            for fid in fids:
                self._log.debug('wait  for %s', fid)
                _flux_job.wait(self._handle, fid)


# ------------------------------------------------------------------------------
#
class FluxHelperV1(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str,
                       log : Logger = None) -> None:

        self._t0 = time.time()

        self._uri      = uri
        self._log      = log or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')
        self._handle   = _flux.Flux(self._uri)
        self._api_lock = mt.Lock()

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

        with self._api_lock:

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

        with self._api_lock:

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

            except:
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

        with self._api_lock, self._elock:
            self._log.debug('register cb %s', cb)
            self._cbacks.append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_cb(self, cb: callable) -> None:

        with self._api_lock, self._elock:
            self._cbacks.remove(cb)


    # --------------------------------------------------------------------------
    #
    def _handle_events(self, fh   : 'flux.Flux',
                             fid  : 'flux.job.JobID',
                             event: 'flux.job.journal.JournalEvent' = None
                      ) -> None:

        with self._elock:

          # self._log.debug_9('event %s: %s', fid, event)

            # if triggered by submit, check if we have anything to do
            if not event:
                if fid not in self._events:
                    return

            # check if we can handle the event - otherwise store it
            if not self._cbacks:
                self._events[fid].append(event)
                return

            # check if application knows the task - otherwise store the event
            if fid not in self._task_ids:
                self._events[fid].append(event)
                return

            tid = self._task_ids[fid]

            # task is known, flush stored events
            for ev in self._events[fid]:
                for cb in self._cbacks:
                    try   : cb(tid, ev)
                    except: self._log.exception('cb failed: %s')
                self._events[fid] = []

            # process the current event
            if event:
                for cb in self._cbacks:
                    try   : cb(tid, event)
                    except: self._log.exception('cb failed: %s')


    # --------------------------------------------------------------------------
    #
    def submit(self, specs: List['flux.job.JobspecV1']) -> List[str]:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            self._log.debug('== submit %d specs', len(specs))
            tids = [spec.attributes['user']['uid'] for spec in specs]

            self._sevent.clear()
            self._squeue.put(specs)
            self._sevent.wait()  # FIXME: timeout?

            return tids


    # --------------------------------------------------------------------------
    #
    def cancel(self, tids: [str|List[str]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            with self._idlock:
                for tid in as_list(tids):
                    fid = self._flux_ids[tid]
                    _flux_job.cancel_async(self._handle, fid, reason='user cancel')


    # --------------------------------------------------------------------------
    #
    def wait(self, tids: [str|List[str]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            tids = as_list(tids)
            with self._idlock:
                fids = [self._flux_ids[tid] for tid in tids]

            for fid in fids:
                _flux_job.wait(self._handle, fid)


# ------------------------------------------------------------------------------
#
if _flux_v == 1: FluxHelper = FluxHelperV1
else           : FluxHelper = FluxHelperV0


# ------------------------------------------------------------------------------

