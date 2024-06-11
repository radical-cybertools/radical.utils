
# pylint: disable=cell-var-from-loop

import os
import sys
import time
import json
import shlex

from typing import Optional, List, Dict, Any, Callable

import threading       as mt
import subprocess      as sp


from .url     import Url
from .ids     import generate_id, ID_CUSTOM
from .shell   import sh_callout
from .logger  import Logger
from .profile import Profiler
from .modules import import_module


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

        self._uid     = uid
        self._log     = log
        self._prof    = prof

        self._lock    = mt.RLock()
        self._term    = mt.Event()

        self._uri     = None
        self._env     = None
        self._proc    = None
        self._watcher = None

        try:
            cmd = 'flux python -c "import flux; print(flux.__file__)"'
            out, err, ret = sh_callout(cmd)

            if ret:
                raise RuntimeError('flux not found: %s' % err)

            flux_path = os.path.dirname(out.strip())
            mod_path  = os.path.dirname(flux_path)
            sys.path.append(mod_path)

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

        out, err, ret = sh_callout('flux resource list')
        self._log.info('flux resources [ %d %s]:\n%s', ret, err, out)

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
                      launcher: Optional[str]           = None,
                      env     : Optional[Dict[str,str]] = None
                     ) -> Optional[str]:

        with self._lock:

            if self._proc is not None:
                raise RuntimeError('already started Flux: %s' % self._uri)

            self._term.clear()

            return self._locked_start_service(launcher, env)


    # --------------------------------------------------------------------------
    #
    def _locked_start_service(self,
                              launcher: Optional[str]           = None,
                              env     : Optional[Dict[str,str]] = None
                             ) -> Optional[str]:

        cmd  = list()

        if launcher:
            cmd += shlex.split(launcher)

        cmd += ['flux', 'start', 'bash', '-c',
                'echo "HOST:$(hostname) URI:$FLUX_URI" && sleep inf']

        self._log.debug('flux command: %s', ' '.join(cmd))

        flux_proc = sp.Popen(cmd, encoding="utf-8",
                             stdin=sp.DEVNULL, stdout=sp.PIPE, stderr=sp.PIPE)

        flux_env = dict()
        while flux_proc.poll() is None:

            try:
                line = flux_proc.stdout.readline()

            except Exception as e:
                self._log.exception('flux service failed to start')
                raise RuntimeError('could not start flux') from e

            if not line:
                continue

            self._log.debug('flux output: %s', line)

            if line.startswith('HOST:'):

                flux_host, flux_uri = line.split(' ', 1)

                flux_host = flux_host.split(':', 1)[1].strip()
                flux_uri  = flux_uri.split(':', 1)[1].strip()

                flux_env['FLUX_HOST'] = flux_host
                flux_env['FLUX_URI']  = flux_uri
                break

        if flux_proc.poll() is not None:
            raise RuntimeError('could not execute `flux start`')

      # fr  = self._flux.uri.uri.FluxURIResolver()
      # ret = fr.resolve('pid:%d' % flux_proc.pid)
      # flux_env = {'FLUX_URI': ret}

        assert 'FLUX_URI' in flux_env, 'no FLUX_URI in env'

        # make sure that the flux url can be reached from other hosts
        # FIXME: this also routes local access via ssh which may slow comm
        flux_url             = Url(flux_env['FLUX_URI'])
        flux_url.host        = flux_env['FLUX_HOST']
        flux_url.schema      = 'ssh'
        flux_uri             = str(flux_url)
        flux_env['FLUX_URI'] = flux_uri

        self._uri       = flux_uri
        self._env       = flux_env
        self._proc      = flux_proc

        self._log.debug('flux uri: %s', flux_uri)

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

            if self._watcher:
                self._watcher.join()

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

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid:str = None) -> None:
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

        if uid: self._uid = uid
        else  : self._uid = generate_id('flux.%(item_counter)04d', ID_CUSTOM)

        self._log       = Logger(self._uid,   ns='radical.utils')
        self._prof      = Profiler(self._uid, ns='radical.utils')

        self._lock      = mt.RLock()

        self._uri       = None
        self._env       = None
        self._exe       = None
        self._handle    = None
        self._handles   = list()  # TODO
        self._executors = list()  # TODO

        try:
            cmd = 'flux python -c "import flux; print(flux.__file__)"'
            out, err, ret = sh_callout(cmd)

            if ret:
                raise RuntimeError('flux not found: %s' % err)

            flux_path = os.path.dirname(out.strip())
            mod_path  = os.path.dirname(flux_path)
            sys.path.append(mod_path)

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

            for idx in range(len(self._handles)):
                del self._handles[idx]

            for idx in range(len(self._executors)):
                del self._executors[idx]

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
    def start_flux(self, launcher: Optional[str] = None) -> None:
        '''
        Start a private Flux instance

        FIXME: forward env
        '''

        with self._lock:

            if self._uri:
                raise RuntimeError('service already connected: %s' % self._uri)

            self._service = _FluxService(self._uid, self._log, self._prof)
            self._service.start_service(launcher=launcher)

            self._uri = self._service.check_service()
            self._env = self._service.env

          # with ru_open(self._uid + '.dump', 'a') as fout:
          #     fout.write('start flux pid %d: %s\n' % (os.getpid(), self._uri))
          #     for l in get_stacktrace()[:-1]:
          #         fout.write(l)

            self._setup()


    # --------------------------------------------------------------------------
    #
    def connect_flux(self, uri : Optional[str] = None) -> None:
        '''
        Connect to an existing Flux instance
        '''

        with self._lock:

          # with ru_open(self._uid + '.dump', 'a') as fout:
          #     fout.write('connect flux %d: %s\n' % (os.getpid(),  uri))
          #     for l in get_stacktrace():
          #         fout.write(l + '\n')

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
        Once a service is connected, create a handle and executor
        '''

        with self._lock:

            assert self._uri, 'not initialized'

            # create a executor and handle for job management
            self._exe    = self.get_executor()
            self._handle = self.get_handle()


    # --------------------------------------------------------------------------
    #
    def submit_jobs(self,
               specs: List[Dict[str, Any]],
               cb   : Optional[Callable[[str, Any], None]] = None
              ) -> Any:

        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            assert self._exe, 'no executor'

            futures = list()
            for spec in specs:
                jobspec = json.dumps(spec)
                fut     = self._flux_job.submit_async(self._handle, jobspec)
                futures.append(fut)

            ids = list()
            for fut in futures:
                flux_id = fut.get_id()
                ids.append(flux_id)
                self._log.debug('submit: %s', flux_id)

                if cb:
                    def app_cb(fut, event):
                        try:
                            cb(flux_id, event)
                        except:
                            self._log.exception('app cb failed')

                    for ev in [
                               'submit',
                               'alloc',
                               'start',
                               'finish',
                               'release',
                             # 'free',
                             # 'clean',
                               'exception',
                              ]:
                        fut.add_event_callback(ev, app_cb)

            self._log.debug('submitted: %s', ids)
            return ids


    # --------------------------------------------------------------------------
    #
    def attach_jobs(self,
                    ids: List[int],
                    cb : Optional[Callable[[int, Any], None]] = None
                   ) -> Any:

        with self._lock:

            if not self._uri:
                raise RuntimeError('FluxHelper is not connected')

            assert self._exe, 'no executor'

            for flux_id in ids:

                fut = self._exe.attach(flux_id)
                self._log.debug('attach %s : %s', flux_id, fut)

                if cb:
                    def app_cb(fut, event):
                        try:
                            cb(flux_id, event)
                        except:
                            self._log.exception('app cb failed')

                    for ev in [
                               'submit',
                               'alloc',
                               'start',
                               'finish',
                               'release',
                             # 'free',
                             # 'clean',
                               'exception',
                              ]:
                        fut.add_event_callback(ev, app_cb)


    # --------------------------------------------------------------------------
    #
    def cancel_jobs(self, flux_ids: List[int]) -> None:

        with self._lock:

            assert self._exe, 'no executor'

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
                assert handle, 'no handle'

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
                assert exe, 'no executor'

            except Exception as e:
                raise RuntimeError('failed to connect at %s' % self._uri) from e

            self._executors.append(exe)

            return exe


# ------------------------------------------------------------------------------

