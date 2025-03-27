
import time
import shlex

import threading as mt

from rc.process  import Process
from functools   import partial
from collections import defaultdict
from typing      import List, Dict, Any

from .misc       import as_list
from .which      import which
from .ids        import generate_id
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


# ------------------------------------------------------------------------------
#
class _FluxService(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid : str,
                       log : Logger) -> None:

        self._uid  = uid
        self._log  = log

        self._fexe = which('flux')
        self._uri  = None
        self._tout = 60

        if not _flux:
            raise RuntimeError('flux module not found') from self._exception

        if not _flux_job:
            raise RuntimeError('flux.job module not found') from self._exception

        if not self._fexe:
            raise RuntimeError('flux executable not found')

        self._start()


    # --------------------------------------------------------------------------
    #
    @property
    def uri(self) -> str:
        return self._uri

    @property
    def timeout(self) -> int:
        return self._tout

    @timeout.setter
    def timeout(self, tout) -> None:
        self._tout = tout


    # --------------------------------------------------------------------------
    #
    def _proc_line_cb(self, prefix: str,
                            proc  : Process,
                            lines : List[str]
                     ) -> None:

        for line in lines:
            if line.startswith('FLUX_URI:'):
                self._uri = line.strip().split(':', 1)[1]

    # --------------------------------------------------------------------------
    #
    def _proc_state_cb(self, proc: Process, state: str) -> None:
        self._log.info('flux instance state update: %s', state)


    # --------------------------------------------------------------------------
    #
    def _start(self) -> None:

        fcmd = 'echo FLUX_URI:\\$FLUX_URI && sleep inf'
        cmd  = '%s start bash -c "%s"' % (self._fexe, fcmd)

        self._log.info('%s: start flux instance: %s', self._uid, cmd)

        p = Process(cmd)
        p.register_cb(p.CB_OUT_LINE, partial(self._proc_line_cb, 'out'))
        p.register_cb(p.CB_ERR_LINE, partial(self._proc_line_cb, 'err'))
        p.register_cb(p.CB_STATE, self._proc_state_cb)
        p.polldelay = 0.1
        p.start()

        start = time.time()
        while time.time() - start < self._tout:
            time.sleep(0.1)
            if self._uri is not None:
                break

        if self.uri is None:
            self._log.error('%s: flux instance did not start', self._uid)
            raise RuntimeError('%s: flux instance did not start', self._uid)

        self._log.info('%s: found flux uri: %s', self._uid, self.uri)


# ------------------------------------------------------------------------------
#
class FluxHelper(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str      = None,
                       log : Logger   = None) -> None:

        self._uri      = uri
        self._log      = log  or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')
        self._handle   = None
        self._journal  = None
        self._service  = None
        self._jthread  = None
        self._started  = False

        self._tasks    = dict()  # task ID -> task
        self._task_ids = dict()  # flux ID -> task ID
        self._flux_ids = dict()  # task ID -> flux ID

        self._elock    = mt.Lock()          # lock event dict
        self._events   = defaultdict(list)  # flux ID -> event list
        self._cbacks   = list()             # list of callbacks

        if not _flux:
            raise RuntimeError('flux module not found') from _flux_exc

        if not _flux_job:
            raise RuntimeError('flux.job module not found') from _flux_exc


    # --------------------------------------------------------------------------
    #
    def start(self) -> None:

        if self._started:
            return

        if not self._uri:
            self._flux_service = _FluxService(uid=self._uid, log=self._log)
            self._uri = self._flux_service.uri

        self._handle = _flux.Flux(self._uri)

        self._jthread = mt.Thread(target=self._watcher)
        self._jthread.daemon = True
        self._jthread.start()

        self._started = True


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self) -> str:
        return self._uid


    # --------------------------------------------------------------------------
    #
    def _watcher(self):

        # NOTE: *never* used self._handle in this thread, as it is not thread
        #      safe.  Instead, use the private handle created here
        handle = _flux.Flux(self._uri)

        # start watching the event journal
        self._journal = _flux_job.JournalConsumer(handle)
        self._journal.start()

        while True:

            try:
                event = self._journal.poll(timeout=1.0)
                self._handle_events(event.jobid, event)

            except TimeoutError:
                pass


    # --------------------------------------------------------------------------
    #
    def register_cb(self, cb: callable) -> None:

        with self._elock:
            self._cbacks.append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_cb(self, cb: callable) -> None:

        with self._elock:
            self._cbacks.remove(cb)


    # --------------------------------------------------------------------------
    #
    def _handle_events(self, flux_id: 'flux.job.JobID',
                             event  : 'flux.job.journal.JournalEvent' = None
                      ) -> None:

        with self._elock:

            # if triggered by submit, check if we have anything to do
            if not event:
                if flux_id not in self._events:
                    return

            # check if we can handle the event - otherwise store it
            if not self._cbacks:
                self._events[flux_id].append(event)
                return

            # check if we already know the task - otherwise store the event
            if flux_id not in self._task_ids:
                self._events[flux_id].append(event)
                return

            # task is known, process stored events
            for ev in self._events[flux_id]:
                for cb in self._cbacks:
                    cb(self._task_ids[flux_id], ev)

            # process the current event
            if event:
                for cb in self._cbacks:
                    cb(self._task_ids[flux_id], event)


    # --------------------------------------------------------------------------
    #
    def spec_from_command(self, cmd: str) -> 'flux.job.JobspecV1':

        return _flux_job.JobspecV1.from_command(shlex.split(cmd))


    # --------------------------------------------------------------------------
    #
    def spec_from_dict(self, td: dict) -> 'flux.job.JobspecV1':

        version    = 1
        tasks      = [{'command': [td['executable']] + td.get('arguments', []),
                       'slot'   : 'task',
                       'count'  : {'per_slot': 1}}]

        system = {'duration': td.get('duration', 0.0)}

        if 'environment' in td: system['environment'] = td['environment']
        if 'sandbox'     in td: system['cwd']         = td['sandbox']
        if 'shell'       in td: system['shell']       = td['shell']
        if 'stdin'       in td: system['stdin']       = td['stdin']
        if 'stdout'      in td: system['stdout']      = td['stdout']
        if 'stderr'      in td: system['stderr']      = td['stderr']
        if 'uid'         in td: system['job']         = {'name': td['uid']}

        attributes = {'system' : system}
        resources  = [{'count': td.get('ranks', 1),
                       'type' : 'slot',
                       'label': 'task',
                       'with' : [{
                           'count': int(td.get('cores_per_rank', 1)),
                           'type' : 'core'}]}]
                     #     'count': int(td.get('gpus_per_rank', 0)) or None,
                     #     'type' : 'gpu'

        if 'gpus_per_rank' in td:
            resources[0]['with'].append({
                    # flux likes integer GPU counts
                    'count': math.ceil(td['gpus_per_rank']),
                    'type' : 'gpu'})

      # import json
      # return json.dumps({
      #     'version'   : version,
      #     'resources' : resources,
      #     'attributes': attributes,
      #     'tasks'     : tasks})

        spec = _flux_job.JobspecV1(resources=resources,
                                   attributes=attributes,
                                   tasks=tasks,
                                   version=version)

        return spec


    # --------------------------------------------------------------------------
    #
    def submit(self, descriptions: List[Dict[str, Any]]) -> List[str]:

        jobs  = list()

        def _submit_cb(tid: str, f: _flux.future.Future) -> None:
            # care must be taken on the order of some of the operations: the
            # `journal_cb` will check if a flux_id is known, and if so will
            # assume that the task id is known also and callbacks can be issued.
            # So *first* register the task ID, *then* the flux ID, to avoid the
            # need for additional locking.

            flux_id = _flux_job.submit_get_id(f)
            self._flux_ids[tid]     = flux_id
            self._task_ids[flux_id] = tid
            jobs.append(flux_id)

            # if we already got events we'll invoke the callbacks now
            self._handle_events(flux_id)

        try:
            # asynchronously submit jobspec files from a directory
            for i, descr in enumerate(descriptions):
                cb   = partial(_submit_cb, 'task.%04d' % i)
                spec = descr
                fut  = _flux_job.submit_async(self._handle, spec, waitable=True)
                fut.then(cb)

            # make sure we get jobid events
            if self._handle.reactor_run() < 0:
                self._log.error("reactor run failed")
                self._handle.fatal_error("reactor start failed")

            # wait for all jobs to get IDs
            # FIXME: Do we need a timeout?  Also, the jobid cb can signal on
            #        completion, so we could wait for that instead of polling.
            while len(jobs) < len(descriptions):
                time.sleep(0.001)

            return jobs


        except Exception:
            self._log.exception("exception")
            raise


    # --------------------------------------------------------------------------
    #
    def cancel(self, flux_ids: [str|List[str]]) -> None:

        for flux_id in as_list(flux_ids):
            _flux_job.cancel_async(self._handle, flux_id, reason='user cancel')


    # --------------------------------------------------------------------------
    #
    def wait(self, flux_ids: [str|List[str]]) -> None:

        for flux_id in as_list(flux_ids):
            _flux_job.wait(self._handle, flux_id)


# ------------------------------------------------------------------------------

