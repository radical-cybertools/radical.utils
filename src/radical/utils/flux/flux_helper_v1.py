
import time
import queue

import threading as mt

from collections import defaultdict
from typing      import List

from ..misc       import as_list
from ..ids        import generate_id
from ..logger     import Logger

from .flux_module import FluxModule


# ------------------------------------------------------------------------------
#
class FluxHelperV1(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uri : str,
                       log : Logger = None) -> None:

        print('=== v1 flux helper ===')

        self._uri      = uri
        self._log      = log or Logger('radical.utils.flux')
        self._uid      = generate_id('ru.flux')

        self._fm       = FluxModule()
        self._handle   = self._fm.core.Flux(self._uri)
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

        self._fm.verify()


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
        fh = self._fm.core.Flux(self._uri)

        # start watching the event journal
        journal = self._fm.job.JournalConsumer(fh)
        journal.start()

        while True:

            try:
                event = journal.poll(timeout=1.0)
                if event:
                    # FIXME: How can that ever *not* be a journal event?
                    #        But it has happened...
                    self._handle_events(fh, event.jobid, event)

            except TimeoutError:
                pass


    # --------------------------------------------------------------------------
    #
    def _swatcher(self):

        self._log.debug('=== swatcher started')

        # if we get new specs, submit them, return IDs to iqueue, and also
        # forward ID to ewatcher
        fh = self._fm.core.Flux(self._uri)
        while True:

            try:
                specs = self._squeue.get(block=True, timeout=1.0)
                self._log.debug('=== got %d specs', len(specs))

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
                        fut = self._fm.job.submit_async(fh, spec, waitable=True)
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
                    self._log.debug('=== submit done')
                    self._sevent.set()


    # --------------------------------------------------------------------------
    #
    def _ewatcher(self):

        # if we get a new job ID, check if we have events for it

        fh = self._fm.core.Flux(self._uri)
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
                    except: self._log.exception('cb failed')
                self._events[fid] = []

            # process the current event
            if event:
                for cb in self._cbacks:
                    try   : cb(tid, event)
                    except: self._log.exception('cb failed')


    # --------------------------------------------------------------------------
    #
    def submit(self, specs: List['flux.job.JobspecV1']) -> List[str]:

        with self._api_lock:

            if not self._handle:
                raise RuntimeError('flux instance not started')

            self._log.debug('== submit %d specs start', len(specs))
            tids = [spec.attributes['user']['uid'] for spec in specs]

            self._sevent.clear()
            self._squeue.put(specs)
            self._sevent.wait()  # FIXME: timeout?
            self._log.debug('== submit %d specs done', len(specs))

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
                    self._fm.job.cancel_async(self._handle, fid, reason='user cancel')


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
                self._fm.job.wait(self._handle, fid)

            # FIXME: remove tasks which have been waited for.


# ------------------------------------------------------------------------------

