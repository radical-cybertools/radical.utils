
import os
import time

from typing import Optional, Dict, Any, Callable

import subprocess as sp
import threading  as mt

from .url     import Url
from .ids     import generate_id
from .shell   import sh_callout
from .logger  import Logger
from .profile import Profiler
from .modules import import_module
from .misc    import as_list, get_hostname


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
    def __init__(self) -> None:

        self._log  = Logger('radical.utils.flux')
        self._prof = Profiler('radical.utils.flux')
        self._lock = mt.Lock()

        try:
            self._mod = import_module('flux')

        except Exception:
            self._log.exception('flux import failed')
            raise

        self._services = dict()


    # --------------------------------------------------------------------------
    #
    def start_service(self,
                      env: Optional[Dict[str,str]] = None
                     ) -> Dict[str, Any]:

        flux_uid = generate_id('flux')

        check = 'flux env; echo "OK"; while true; do echo "ok"; sleep 1; done'
        start = 'flux start -o,-v,-S,log-filename=out'
        cmd   = '/bin/bash -c "echo \\\"%s\\\" | %s"' % (check, start)

        penv  = os.environ
        if env:
            penv  = {k:v for k,v in os.environ.items()}
            for k,v in env.items():
                penv[k] = v

        flux_env  = dict()
        flux_term = mt.Event()
        flux_proc = sp.Popen(cmd, shell=True, env=penv,
                             stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

        while True:

            try:
                line = flux_proc.stdout.readline()
                line = bytes.decode(line, 'utf-8').strip()

            except Exception as e:
                self._log.exception('flux service failed to start')
                raise RuntimeError('could not start flux') from e

            if not line:
                continue

            self._log.debug('=== %s', line)

            if line.startswith('export '):
                k, v = line.split(' ', 1)[1].strip().split('=', 1)
                flux_env[k] = v.strip('"')
                self._log.debug('%s = %s' % (k, v.strip('"')))

            elif line == 'OK':
                break


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

            ret = None
            while not flux_term.is_set():

                time.sleep(1)

                _, err, ret = sh_callout('flux ping -c 1 all')
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

        # ----------------------------------------------------------------------
        def listen(flux_uid : str,
                   flux_uri : str,
                   flux_term: mt.Event
                  ) -> None:

            handle = None
            try:
                handle = self._mod.Flux(url=flux_uri)
                handle.event_subscribe('job-state')

                while not flux_term.is_set():

                    # FIXME: how can recv be timed out or interrupted after work
                    #        completed?
                    event = handle.event_recv()

                    if 'transitions' not in event.payload:
                        self._log.warn('unexpected flux event: %s' %
                                        event.payload)
                        continue

                    transitions = as_list(event.payload['transitions'])

                    for event in transitions:
                        job_id, job_state = event
                        if job_state not in self.event_list:
                            # we are not interested in this event
                            continue

                        with self._lock:
                            if flux_uid not in self._services:
                                # service is gone
                                flux_term.set()
                                break

                            # FIXME: can we dig out exit code?  Other meta data?
                            for cb in self._services[flux_uid]['callbacks']:
                                cb(job_id, job_state)


            except Exception:
                self._log.exception('Error in listener loop')
                if handle:
                    handle.event_unsubscribe('job-state')
                    del(handle)

            finally:
                flux_term.set()
        # ----------------------------------------------------------------------

        flux_listener = mt.Thread(target=listen,
                                  args=[flux_uid, flux_uri, flux_term])
        flux_listener.daemon = True
        flux_listener.start()


        self._log.info("flux startup successful: [%s]", flux_env['FLUX_URI'])

        with self._lock:
            self._services[flux_uid] = {'uid'      : flux_uid,
                                        'uri'      : flux_uri,
                                        'env'      : flux_env,
                                        'proc'     : flux_proc,
                                        'term'     : flux_term,
                                        'watcher'  : flux_watcher,
                                        'listener' : flux_listener,
                                        'handles'  : list(),
                                        'callbacks': list(),
                                       }

        return self.check_service(flux_uid)


    # --------------------------------------------------------------------------
    #
    def check_service(self, uid : str) -> Dict[str, Any]:

        with self._lock:

            if uid not in self._services:
                raise LookupError('flux service id %s unknown' % uid)

            if self._services[uid]['term'].is_set():
                raise RuntimeError('flux service id %s unknown' % uid)

            return self._services[uid]


    # --------------------------------------------------------------------------
    #
    def get_handle(self, uid : str) -> Any:

        with self._lock:

            if uid not in self._services:
                raise LookupError('flux service id %s unknown' % uid)

            try:
                handle = self._mod.Flux(url=self._services[uid]['uri'])
                if not handle:
                    raise RuntimeError('handle invalid')

            except Exception as e:
                self._log.exception('failed to create flux handle for %s' % uid)
                raise RuntimeError('failed to create flux handle') from e

            self._services[uid]['handles'].append(handle)

        return handle


    # --------------------------------------------------------------------------
    #
    def register_callback(self,
                          uid: str,
                          cb : Callable[[Any], None]
                         ) -> None:

        with self._lock:

            if uid not in self._services:
                raise LookupError('flux service id %s unknown' % uid)

            self._services[uid]['callbacks'].append(cb)


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self,
                            uid: str,
                            cb : Callable[[Any], None]
                           ) -> None:

        with self._lock:

            if uid not in self._services:
                raise LookupError('flux service id %s unknown' % uid)

            self._services[uid]['callbacks'].remove(cb)


    # --------------------------------------------------------------------------
    #
    def close_service(self, uid : str) -> None:

        with self._lock:

            if uid not in self._services:
                raise LookupError('flux service id %s unknown' % uid)

            # terminate watcher and listener
            self._services[uid]['term'].set()

            # delete all created handles
            for handle in self._services[uid]['handles']:
                del(handle)

            # terminate the service process
            # FIXME: send termination signal to flux for cleanup
            self._services[uid]['proc'].kill()
            time.sleep(0.1)
            self._services[uid]['proc'].terminate()
            self._services[uid]['proc'].wait()

            # remove service entry
            del(self._services[uid])


# ------------------------------------------------------------------------------

