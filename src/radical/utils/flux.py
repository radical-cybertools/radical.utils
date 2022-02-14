
# pylint: disable=cell-var-from-loop

import os
import time
import json
import errno
import queue

from typing import Optional, List, Dict, Any, Callable

import threading       as mt
import subprocess      as sp


from .url     import Url
from .ids     import generate_id, ID_CUSTOM
from .shell   import sh_callout
from .logger  import Logger
from .profile import Profiler
from .modules import import_module
from .misc    import as_list, ru_open
from .host    import get_hostname
from .debug   import get_stacktrace


# --------------------------------------------------------------------------
#
class _FluxService(object):
    '''
    Helper class to handle a private Flux instance, including configuration,
    start, monitoring and termination.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid  : str,
                       log  : Logger,
                       prof : Profiler) -> None:

        self._uid  = uid
        self._log  = log
        self._prof = prof

        self._lock = mt.RLock()
        self._term = mt.Event()

        self._uri       = None
        self._env       = None
        self._proc      = None
        self._watcher   = None
        self._listener  = None
        self._callbacks = list()

        try:
            self._flux     = import_module('flux')
            self._flux_job = import_module('flux.job')

        except Exception:
            self._log.exception('flux import failed')
            raise



    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    @property
    def uri(self):
        return self._uri


    @property
    def env(self):
        return self._env


    # --------------------------------------------------------------------------
    #
    def _watch(self) -> None:

        # FIXME: this thread will change `os.environ` for this *process* because
        #        we want to call `flux ping` via `sh_callout`.  We should
        #        instead use the Flux Python API to run the pings and pass the
        #        URI explicitly.
        self._log.info('starting flux watcher')

        if self._env:
            for k,v in self._env.items():
                os.environ[k] = v

        while not self._term.is_set():

            time.sleep(1)

            _, err, ret = sh_callout('flux ping -c 1 kvs')
            if ret:
                self._log.error('flux watcher err: %s', err)
                break

        # we only get here when the ping failed - set the event
        self._term.set()
        self._log.warn('flux stopped')


    # --------------------------------------------------------------------------
    #
    def start_service(self,
                      env: Optional[Dict[str,str]] = None
                     ) -> Optional[str]:

        with self._lock:

            if self._proc is not None:
                raise RuntimeError('already started Flux: %s' % self._uri)

            self._term.clear()

            return self._locked_start_service(env)


    # --------------------------------------------------------------------------
    #
    def _locked_start_service(self,
                              env: Optional[Dict[str,str]] = None
                             ) -> Optional[str]:

        check = 'flux env; echo "OK"; while true; do echo "ok"; sleep 1; done'
        start = 'flux start -o,-v,-S,log-filename=%s.log' % self._uid
        cmd   = '/bin/bash -c "echo \\\"%s\\\" | %s"' % (check, start)

        penv  = None
        if env:
            penv = {k:v for k,v in os.environ.items()}
            for k,v in env.items():
                penv[k] = v

        flux_env  = dict()
        flux_proc = sp.Popen(cmd, shell=True, env=penv,
                             stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

        while flux_proc.poll() is None:

            try:
                line = flux_proc.stdout.readline()
                line = bytes.decode(line, 'utf-8').strip()

            except Exception as e:
                self._log.exception('flux service failed to start')
                raise RuntimeError('could not start flux') from e

            if not line:
                continue

            self._log.debug('%s', line)

            if line.startswith('export '):
                k, v = line.split(' ', 1)[1].strip().split('=', 1)
                flux_env[k] = v.strip('"')
                self._log.debug('%s = %s' % (k, v.strip('"')))

            elif line == 'OK':
                break

        if flux_proc.poll() is not None:
            raise RuntimeError('could not execute `flux start`')

        assert('FLUX_URI' in flux_env), 'no FLUX_URI in env'

        # make sure that the flux url can be reched from other hosts
        # FIXME: this also routes local access via ssh which may slow comm
        flux_url             = Url(flux_env['FLUX_URI'])
        flux_url.host        = get_hostname()
        flux_url.schema      = 'ssh'
        flux_uri             = str(flux_url)
        flux_env['FLUX_URI'] = flux_uri

        self._uri       = flux_uri
        self._env       = flux_env
        self._proc      = flux_proc
        self._handles   = list()
        self._executors = list()

        self._prof.prof('flux_started', msg=self._uid)

        # start watcher thread to monitor the instance
        self._watcher = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()

        self._log.info("flux startup successful: [%s]", flux_env['FLUX_URI'])

        return self._uri


    # --------------------------------------------------------------------------
    #
    def check_service(self) -> Optional[str]:

        with self._lock:

            if not self._proc:
                raise RuntimeError('flux service was not yet started')

            if self._term.is_set():
                raise RuntimeError('flux service was terminated')

            return self._uri


    # --------------------------------------------------------------------------
    #
    def close_service(self) -> None:

        with self._lock:

            self.check_service()

            if not self._proc:
                raise RuntimeError('cannot kill flux from this process')

            # terminate watcher and listener
            self._term.set()

            if self._listener: self._listener.join()
            if self._watcher:  self._watcher.join()

            # terminate the service process
            # FIXME: send termination signal to flux for cleanup
            self._proc.kill()
            time.sleep(0.1)
            self._proc.terminate()
            self._proc.wait()

            self._uri = None
            self._env = None


# ------------------------------------------------------------------------------
#
class FluxHelper(object):

    '''
    Helper CLASS to programnatically handle flux instances and to obtain state
    update events for flux jobs known in that instance.
    '''

    # list of reported flux events
    _event_list = [
                   'NEW',
                   'DEPEND',
                   'SCHED',
                   'RUN',
                   'CLEANUP',
                   'INACTIVE',
                  ]



    # --------------------------------------------------------------------------
    #
    def __init__(self) -> None:
        '''
        The Flux Helper c'tor takes no arguments and will initially not be
        connected to a Flux instance.  After construction, the application can
        call either one of the following methods:

            FluxHelper.connect_flux(uri=None)
            FluxHelper.start_flux()

        The first will attempt to connect to the Flux instance referenced by
        that URI - a `ValueError` exception will be raised if that instance
        cannot be reached.  If no URI is provided, the environment variable
        `FLUX_URI` will be used.

        The second method will instantiate a new flux instance in the current
        process environment.

        In both cases, the properties

            FluxHelper.uri
            FluxHelper.env

        will provide information about the connected Flux instance.  The `uri`
        is provided as a string, the `env` as a dictionary of environment
        settings (including `FLUX_URI` again).

        The method

            FluxHelper.reset()

        will disconnect from the Flux instance, and in the case where
        `start_flux` created a private instance, that instance will be killed.
        The `uri` and `env` properties will be reset to `None`.


        While connected to a Flux instance, the following methods can be used to
        interact with the instance:

            FluxHelper.get_executor() - return a flux.job.Executor instance
            FluxHelper.get_handle()   - return a flux.job.Flux     instance

        All provided executors and handles will be invalidated upon `reset()`.
        '''

        self._service : Optional[_FluxService] = None

        self._uri       = None
        self._env       = None

        self._uid       = generate_id('flux.%(item_counter)04d', ID_CUSTOM)
        self._log       = Logger(self._uid,   ns='radical.utils')
        self._prof      = Profiler(self._uid, ns='radical.utils')

        self._lock      = mt.RLock()
        self._term      = mt.Event()
        self._listener  = None
        self._callbacks = list()
        self._queue     = queue.Queue()

        self._exe       = None
        self._handles   = list()  # TODO
        self._executors = list()  # TODO

        try:
            self._flux     = import_module('flux')
            self._flux_job = import_module('flux.job')

        except Exception:
            self._log.exception('flux import failed')
            raise


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        # FIXME: are handles / executors correctly garbage collected?
        self.reset()


    # --------------------------------------------------------------------------
    #
    def reset(self):
        '''
        Close the connection to the FLux instance (if it exists), and terminate
        the Flux service if it was started by this instance.  All handles and
        executors created for this service will be invalidated.
        '''

        with self._lock:

            if self._listener:
                self._term.set()

            for handle in self._handles:
                del(handle)

            for exe in self._executors:
                del(exe)

            self._exe    = None
            self._handle = None

            if self._uri:
                try:
                    self._service.close_service()
                except:
                    pass
                self._uri = None
                self._env = None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        '''
        unique ID for this FluxHelper instance
        '''

        with self._lock:
            return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def uri(self):
        '''
        uri for the connected Flux instance.  Returns `None` if no instance is
        connected.
        '''

        with self._lock:
            return self._uri


    # --------------------------------------------------------------------------
    #
    @property
    def env(self):
        '''
        environment dict for the connected Flux instance.  Returns `None` if no
        instance is connected.
        '''

        with self._lock:
            return self._env


    # --------------------------------------------------------------------------
    #
    def start_flux(self) -> None:
        '''
        Start a private Flux instance

        FIXME: forward env
        '''

        with self._lock:

            if self._uri:
                raise RuntimeError('service already connected: %s' % self._uri)

            with ru_open(self._uid + '.dump', 'a') as fout:
                fout.write('starting ' + str(os.getpid()) + '\n')
                for l in get_stacktrace():
                    fout.write(l + '\n')

            self._service = _FluxService(self._uid, self._log, self._prof)
            self._service.start_service()


            self._uri = self._service.check_service()
            self._env = self._service.env

            self._setup()


    # --------------------------------------------------------------------------
    #
    def connect_flux(self, uri : Optional[str] = None) -> None:
        '''
        Connect to an existing Flux instance
        '''

        with self._lock:

            with ru_open(self._uid + '.dump', 'a') as fout:
                fout.write('connecting ' + str(os.getpid()) + '\n')
                for l in get_stacktrace():
                    fout.write(l + '\n')


            if self._uri:
                raise RuntimeError('service already connected: %s' % self._uri)

            if not uri:
                uri = os.environ.get('FLUX_URI')

            if not uri:
                raise RuntimeError('no Flux instance found via FLUX_URI')

            self._uri = uri
            self._env = {'FLUX_URI': uri}

            # FIXME: run a ping test to ensure the service is up

            self._setup()


    # ----------------------------------------------------------------------
    #
    def _setup(self):
        '''
        Once a service is connected, create one handle and start a listener
        thread on it to serve any registered callback
        '''

        with self._lock:

            assert(self._uri), 'not initialized'

            # start a listener thread so that we can serve callbacks
            self._term.clear()
            self._listener = mt.Thread(target=self._listen)
            self._listener.daemon = True
            self._listener.start()

            # create a executor and handle for job management
            self._exe    = self.get_executor()
            self._handle = self.get_handle()


    # --------------------------------------------------------------------------
    #
    def register_callback(self,
                          cb : Callable[[str, str, float, dict], None]
                         ) -> None:
        '''
        Register a callable to be fired when a flux event is collected.  The
        callable MUST have the following signature :

            def cb(job_id     : str,      # job which triggered event
                   event_name : str,      # name of event (usually job state)
                   ts         : float,    # event creation timestamp
                   context    : dict)     # event meta data

        '''

        with self._lock:

            self._log.debug('register cb %s', cb)
            self._callbacks.append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self,
                            cb : Callable[[str, str, float, dict], None]
                           ) -> None:
        '''
        unregister a callback which previously was added via `register_callback`
        '''

        with self._lock:

            self._log.debug('unregister cb %s', cb)
            self._callbacks.remove(cb)



    # ----------------------------------------------------------------------
    #
    def _listen(self) -> None:
        '''
        collect events from the connected Flux instance, and invoke any
        registered callbacks for each event.

        NOTE: we handle `INACTIVE` separately: we will wait for the respective
              job to finish to ensure cleanup and stdio flush
        '''

        self._log.debug('listen for events')
        handle = None
        try:
            handle = self.get_handle()
            handle.event_subscribe('job-state')

            while not self._term.is_set():

                # FIXME: how can recv be timed out or interrupted after work
                #        completed?
                event = handle.event_recv()

                if 'transitions' not in event.payload:
                    self._log.warn('unexpected flux event: %s' %
                                    event.payload)
                    continue

                transitions = as_list(event.payload['transitions'])

                for event in transitions:

                    self._log.debug('event: %s', event)
                    job_id, event_name, ts = event

                    if event_name not in self._event_list:
                        # we are not interested in this event
                        continue

                    with self._lock:
                        try:
                            for cb in self._callbacks:
                                context = dict()

                                if event_name == 'INACTIVE':
                                    context = self._flux_job.event_wait(
                                            handle, job_id, "finish").context
                                cb(job_id, event_name, ts, context)
                        except:
                            self._log.exception('cb error')


        except OSError as e:

            if e.errno == errno.EIO:
                # flux terminated
                self._log.info('connection lost, stop listening')
                handle = None

            else:
                self._log.exception('Error in listener loop')


        except Exception:

            self._log.exception('Error in listener loop')


        finally:

            # disconnect from the Flux instance on any event collection errors
            if handle:
                handle.event_unsubscribe('job-state')
                del(handle)

            self.reset()


    # --------------------------------------------------------------------------
    #
    def submit_jobs(self,
               specs: List[Dict[str, Any]],
               cb   : Optional[Callable[[str, str, float, dict], None]] = None
              ) -> Any:

        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            assert(self._exe), 'no executor'

            def jid_cb(fut, evt):
                try:
                    jid = fut.jobid(timeout=0.1)
                    self._queue.put(jid)
                except:
                    self._log.exception('flux cb failed')
                    self._queue.put(None)


            for spec in specs:
                jobspec = json.dumps(spec)
                fut     = self._exe.submit(jobspec, waitable=False)
                self._log.debug('submit: %s', fut)
                fut.add_event_callback('submit', jid_cb)

                if cb:
                    def app_cb(fut, evt):
                        try:
                            jid = fut.jobid()
                            cb(jid, evt.name, evt.timestamp, evt.context)
                        except:
                            self._log.exception('app cb failed')

                    for ev in [
                               'submit',
                             # 'alloc',
                               'start',
                               'finish',
                               'release',
                             # 'free',
                               'clean',
                               'exception',
                              ]:
                        fut.add_event_callback(ev, app_cb)

            ids = list()
            for spec in specs:
                ids.append(self._queue.get())

            self._log.debug('submitted: %s', ids)
            return ids


    # --------------------------------------------------------------------------
    #
    def attach_jobs(self,
                    ids: List[int],
                    cb : Optional[Callable[[str, str, float, dict], None]] = None
                   ) -> Any:

        states = list()
        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            assert(self._exe), 'no executor'

            for flux_id in ids:

                fut = self._exe.attach(flux_id)
                states.append(fut.state())
                self._log.debug('attach %s : %s', flux_id, fut)

                if cb:
                    def app_cb(fut, evt):
                        try:
                            cb(str(flux_id), evt.name, evt.timestamp, evt.context)
                        except:
                            self._log.exception('app cb failed')

                    for ev in [
                               'submit',
                             # 'alloc',
                               'start',
                               'finish',
                               'release',
                             # 'free',
                               'clean',
                               'exception',
                              ]:
                        fut.add_event_callback(ev, app_cb)

            return states


    # --------------------------------------------------------------------------
    #
    def cancel_jobs(self, flux_ids: List[int]) -> None:

        with self._lock:

            assert(self._exe), 'no executor'

            for flux_id in flux_ids:
                fut = self._exe.attach(flux_id)
                self._log.debug('cancel %s : %s', flux_id, fut)
                fut.cancel()


    # --------------------------------------------------------------------------
    #
    def get_handle(self) -> Any:

        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            try:
                handle = self._flux.Flux(url=self._uri)
                assert(handle), 'no handle'

            except Exception as e:
                raise RuntimeError('failed to connect at %s' % self._uri) from e

            self._handles.append(handle)

            return handle


    # --------------------------------------------------------------------------
    #
    def get_executor(self) -> Any:

        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            try:
                args = {'url': self._uri}
                exe  = self._flux_job.executor.FluxExecutor(handle_kwargs=args)
                assert(exe), 'no executor'

            except Exception as e:
                raise RuntimeError('failed to connect at %s' % self._uri) from e

            self._executors.append(exe)

            return exe


# ------------------------------------------------------------------------------

