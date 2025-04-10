
import os
import sys
import math
import shlex

from typing       import Any

from ..which      import which
from ..ids        import generate_id, ID_SIMPLE
from ..modules    import import_module
from ..shell      import sh_callout


# ------------------------------------------------------------------------------
#
class FluxModule(object):

    _flux_core = None
    _flux_job  = None
    _flux_exc  = None
    _flux_v    = None


    # --------------------------------------------------------------------------
    #
    def __init__(self):
        '''
        import the flux module, if available
        '''

        if self._flux_core or self._flux_job or self._flux_exc:
            return

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

                assert not ret, [cmd, err]

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

        self._flux_core = flux
        self._flux_job  = flux_job
        self._flux_exc  = flux_exc
        self._flux_v    = flux_v
        self._flux_exe  = which('flux')


    # --------------------------------------------------------------------------
    #
    def verify(self) -> None:
        '''
        verify that flux modules are available
        '''

        if self._flux_core is None:
            raise RuntimeError('flux core module not found') from self._flux_exc

        if self._flux_job is None:
            raise RuntimeError('flux.job module not found') from self._flux_exc

        if self._flux_exe is None:
            raise RuntimeError('flux executable not found') from self._flux_exc


    # --------------------------------------------------------------------------
    #
    @property
    def version(self) -> int:
        return self._flux_v

    @property
    def core(self) -> Any:
        return self._flux_core

    @property
    def job(self) -> Any:
        return self._flux_job

    @property
    def exc(self) -> Any:
        return self._flux_exc

    @property
    def exe(self) -> str:
        return self._flux_exe


# ------------------------------------------------------------------------------
#
def spec_from_command(cmd: str) -> 'flux.job.JobspecV1':

    fm = FluxModule()

    spec = fm.job.JobspecV1.from_command(shlex.split(cmd))
    spec.attributes['user']['uid'] = generate_id(ID_SIMPLE)

    return spec


# ------------------------------------------------------------------------------
#
def spec_from_dict(td: dict) -> 'flux.job.JobspecV1':

    fm = FluxModule()

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

    spec = fm.job.JobspecV1(resources=resources,
                            attributes=attributes,
                            tasks=tasks,
                            version=version)
    return spec


# ------------------------------------------------------------------------------

