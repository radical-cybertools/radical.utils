
import os
import time

from typing import Optional, Dict, Any, Callable

import threading       as mt
import subprocess      as sp

from .url     import Url
from .ids     import generate_id
from .shell   import sh_callout
from .logger  import Logger
from .profile import Profiler
from .modules import import_module
from .json_io import read_json, write_json
from .misc    import as_list, get_hostname, rec_makedir


# ------------------------------------------------------------------------------
#
class FluxHelper(object):

    # helper to programnatically handle flux instances and to obtain state
    # update events for flux jobs runknown in that instance.
    #
    # TODO: At the moment, that `FluxHelper` can only be used within a single
    #       process - later iterations my allow to start the service in one
    #       process and to register for callbacks etc. from another process.

    # list of allowed (reported) flux events
    event_list = {
                  'NEW',
                  'DEPEND',
                  'SCHED',
                  'RUN',
                  'CLEANUP',
                  'INACTIVE'
                 }


    # --------------------------------------------------------------------------
    #
    def __init__(self, name : str = None) -> None:

        if name: self._name = name
        else   : self._name = 'flux_helper'

        self._log  = Logger('radical.utils.%s' % self._name)
        self._prof = Profiler('radical.utils.%s' % self._name)
        self._lock = mt.RLock()

        try:
            self._mod = import_module('flux')

        except Exception:
            self._log.exception('flux import failed')
            raise

        self._flux_info   = dict()  # serializable data
        self._local_state = dict()  # non-serializable state

        self._base = '%s/%s' % (os.getcwd(), self._name)
        rec_makedir(self._base)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):

        return self._name


    # --------------------------------------------------------------------------
    #
    def start_service(self,
                      env: Optional[Dict[str,str]] = None
                     ) -> Dict[str, Any]:

        with self._lock:

            return self._locked_start_service(env)


    # --------------------------------------------------------------------------
    #
    def _locked_start_service(self,
                              env: Optional[Dict[str,str]] = None
                             ) -> Dict[str, Any]:

        flux_uid = generate_id('flux')

        check = 'flux env; echo "OK"; while true; do echo "ok"; sleep 1; done'
        start = 'flux start -o,-v,-S,log-filename=%s.log' % flux_uid
        cmd   = '/bin/bash -c "echo \\\"%s\\\" | %s"' % (check, start)

        penv  = None
        if not env:
            penv  = {k:v for k,v in os.environ.items()}
            for k,v in env.items():
                penv[k] = v

        flux_env  = dict()
        flux_term = mt.Event()
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

        assert('FLUX_URI' in flux_env)

        # make sure that the flux url can be reched from other hosts
        # FIXME: this also routes local access via ssh which may slow comm
        flux_url             = Url(flux_env['FLUX_URI'])
        flux_url.host        = get_hostname()
        flux_url.schema      = 'ssh'
        flux_uri             = str(flux_url)
        flux_env['FLUX_URI'] = flux_uri

        self._prof.prof('flux_started', msg=flux_uid)

        # ----------------------------------------------------------------------
        def _watch_flux(flux_env : Dict[str,str],
                        flux_term: mt.Event
                       ) -> None:

            self._log.info('starting flux watcher')

            for k,v in flux_env.items():
                os.environ[k] = v

            while not flux_term.is_set():

                time.sleep(1)

                _, err, ret = sh_callout('flux ping -c 1 kvs')
                if ret:
                    self._log.error('flux watcher err: %s', err)
                    break

            # we only get here when the ping failed - set the event
            flux_term.set()
            self._log.warn('flux stopped')
        # ----------------------------------------------------------------------

        flux_watcher = mt.Thread(target=_watch_flux, args=[flux_env, flux_term])
        flux_watcher.daemon = True
        flux_watcher.start()

        self._log.info("flux startup successful: [%s]", flux_env['FLUX_URI'])


        self._flux_info[flux_uid]   = {'uid'      : flux_uid,
                                       'uri'      : flux_uri,
                                       'env'      : flux_env}

        self._local_state[flux_uid] = {'proc'     : flux_proc,
                                       'term'     : flux_term,
                                       'watcher'  : flux_watcher,
                                       'listeners': list(),
                                       'handles'  : list(),
                                       'executors': list(),
                                       'callbacks': list()}

        write_json('%s/%s.json' % (self._base, flux_uid),
                   self._flux_info[flux_uid])

        return self._flux_info[flux_uid]


    # --------------------------------------------------------------------------
    #
    def check_service(self, uid : str) -> Dict[str, Any]:

        with self._lock:

            if uid not in self._flux_info:
                try:
                    fname = '%s/%s.json' % (self._base, uid)
                    self._flux_info[uid] = read_json(fname)

                except Exception as e:
                    # no (valid) info file found - that service does not exist
                    raise LookupError('flux service id %s unknown' % uid) from e

            if uid not in self._local_state:
                # the service actually exists, but we don't have local state.
                # This implies that the servide was started by a different
                # process than this one.  We create a dummy local state, but
                # otherwise consider that service as valid.
                self._local_state[uid] = {'proc'     : None,
                                          'term'     : None,
                                          'watcher'  : None,
                                          'listener' : None,
                                          'handles'  : list(),
                                          'executors': list(),
                                          'callbacks': list()}

            else:
                if self._local_state[uid]['term'].is_set():
                    # the service is gone already
                    raise RuntimeError('flux service %s was terminated' % uid)


            return self._flux_info[uid]


    # --------------------------------------------------------------------------
    #
    def get_handle(self, uid : str) -> Any:

        with self._lock:

            flux_info = self.check_service(uid)
            assert(uid in self._local_state)

            try:
                handle = self._mod.Flux(url=flux_info['uri'])
                assert(handle)

            except Exception as e:
                self._log.exception('failed to create flux handle for %s' % uid)
                raise RuntimeError('failed to create flux handle') from e

            self._local_state[uid]['handles'].append(handle)

            return handle


    # --------------------------------------------------------------------------
    #
    def get_executor(self, uid : str) -> Any:

        with self._lock:

            flux_info = self.check_service(uid)
            assert(uid in self._local_state)

            try:
                args = {'url':flux_info['uri']}
                jex  = self._mod.job.executor.FluxExecutor(handle_kwargs=args)
                assert(jex)

            except Exception as e:
                self._log.exception('failed to create flux executor for %s' % uid)
                raise RuntimeError('failed to create flux executor') from e

            self._local_state[uid]['executors'].append(jex)

            return jex


    # ----------------------------------------------------------------------
    def _listen(self,
               flux_uid : str,
               flux_uri : str,
               flux_term: mt.Event,
               cb       : Callable[[str, str, float, dict], None]
              ) -> None:

        # NOTE: for services spawned in a different process, `flux_term` will be
        # `None` and `self._listen` will not be able to terminate with the
        # service.  In that case we run this thread forever - it is a daemon
        # thread and will dy with its parent process.

        handle = None
        self._log.debug('====> register for events: %s', cb)
        try:
            handle = self._mod.Flux(url=flux_uri)
            self._log.debug('=== event handle: %s', handle)
            handle.event_subscribe('job-state')
            self._log.debug('=== subscribe event')

            while flux_term is None or not flux_term.is_set():

                self._log.debug('=== recv event')

                # FIXME: how can recv be timed out or interrupted after work
                #        completed?
                event = handle.event_recv()

                if 'transitions' not in event.payload:
                    self._log.warn('unexpected flux event: %s' %
                                    event.payload)
                    continue

                transitions = as_list(event.payload['transitions'])

                for event in transitions:
                    self._log.debug('====> event: %s', event)
                    job_id, job_state, ts = event
                    if job_state not in self.event_list:
                        # we are not interested in this event
                        continue

                    with self._lock:
                        if flux_uid not in self._flux_info:
                            # service is gone
                            flux_term.set()
                            break

                        # FIXME: can we dig out exit code?  Other meta data?
                        try:
                            for cb in self._local_state[flux_uid]['callbacks']:
                                context = dict()
                                if job_state == 'INACTIVE':
                                    context = self._mod.job.event_wait(
                                            handle, job_id, "finish").context
                                cb(job_id, job_state, ts, context)
                        except:
                            self._log.exception('cb error')


        except Exception:
            self._log.exception('Error in listener loop')
            if handle:
                handle.event_unsubscribe('job-state')
                del(handle)

        finally:
            flux_term.set()

    # --------------------------------------------------------------------------
    #
    def register_callback(self,
                          uid: str,
                          cb : Callable[[str, str, float, dict], None]
                         ) -> None:

        with self._lock:

            self._log.debug('===> register cb %s', cb)

            self.check_service(uid)
            assert(uid in self._local_state)

            self._local_state[uid]['callbacks'].append(cb)

            flux_term = self._local_state[uid]['term']
            flux_uri  = self._flux_info[uid]['uri']

            flux_listener = mt.Thread(target=self._listen,
                                      args=[uid, flux_uri, flux_term, cb])
            flux_listener.daemon = True
            flux_listener.start()

            self._local_state[uid]['listeners'].append(flux_listener)




    # --------------------------------------------------------------------------
    #
    def unregister_callback(self,
                            uid: str,
                            cb : Callable[[Any], None]
                           ) -> None:

        with self._lock:

            self.check_service(uid)
            assert(uid in self._local_state)

            self._local_state[uid]['callbacks'].remove(cb)


    # --------------------------------------------------------------------------
    #
    def close_service(self, uid : str) -> None:

        with self._lock:

            self.check_service(uid)

            if self._local_state[uid]['proc'] is None:
                raise RuntimeError('cannot kill flux from this process')

            # terminate watcher and listener
            self._local_state[uid]['term'].set()

            # delete all created handles
            for handle in self._local_state[uid]['handles']:
                del(handle)

            # delete all created executors
            for jex in self._local_state[uid]['executors']:
                del(jex)

            # terminate the service process
            # FIXME: send termination signal to flux for cleanup
            self._local_state[uid]['proc'].kill()
            time.sleep(0.1)
            self._local_state[uid]['proc'].terminate()
            self._local_state[uid]['proc'].wait()

            # remove service entries
            del(self._flux_info[uid])
            del(self._local_state[uid])

            try:
                os.unlink('%s/%s.json' % (self._base, uid))
            except Exception:
                pass


# ------------------------------------------------------------------------------

