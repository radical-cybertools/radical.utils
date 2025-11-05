
import time
import queue

import threading as mt

from functools   import partial
from collections import defaultdict
from typing      import List, Union

from ..misc       import as_list
from ..ids        import generate_id
from ..logger     import Logger

from .flux_module import FluxModule


# ------------------------------------------------------------------------------
#
class FluxHelperV0(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str,
                       log : Logger = None) -> None:

      # print('=== v0 flux helper ===')

        self._uri      = uri
        self._log      = log or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')

        self._fm       = FluxModule()
        self._handle   = self._fm.core.Flux(self._uri)
        self._api_lock = mt.Lock()
        self._exe      = self._fm.job.executor.FluxExecutor(
                                               handle_kwargs={'url': self._uri})

        self._idlock   = mt.Lock()          # lock ID dicts
        self._elock    = mt.Lock()          # lock event dict
        self._task_ids = dict()             # flux ID -> task ID
        self._flux_ids = dict()             # task ID -> flux ID
        self._events   = defaultdict(list)  # flux ID -> event list
        self._cbacks   = list()             # list of callbacks

        self._fm.verify()
        for line in (self._fm.version or '').splitlines():
            self._log.info('flux version: %s', line)


    # --------------------------------------------------------------------------
    #
    def start(self, launcher: str   = None) -> None:

        pass


    # --------------------------------------------------------------------------
    #
    def stop(self):

        with self._api_lock:

            # FIXME: shutdown flux instance
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
                except: self._log.exception('cb failed')
            self._events[fid] = []

        # process the current event
        if event:
          # print('process current event')
            for cb in self._cbacks:
                try   : cb(fid, event)
                except: self._log.exception('cb failed')


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
                    raise RuntimeError('%s: timeout on submission' % self._uid)
            self._log.info('got %d flux IDs', len(futures))

            # get flux_ids sorted by submission order (idx)
            flux_ids = [fut[0] for fut in sorted(futures, key=lambda x: x[1])]

            self._log.debug('%s: submitted: %s', self._uid, flux_ids)
            return flux_ids


    # --------------------------------------------------------------------------
    #
    def cancel(self, fids: [Union[str, List[str]]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            with self._idlock:
                for fid in as_list(fids):
                    self._fm.job.cancel_async(self._handle, fid, reason='user cancel')


    # --------------------------------------------------------------------------
    #
    def wait(self, fids: [Union[str, List[str]]]) -> None:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            for fid in fids:
                self._log.debug('wait  for %s', fid)
                self._fm.job.wait(self._handle, fid)


# ------------------------------------------------------------------------------

