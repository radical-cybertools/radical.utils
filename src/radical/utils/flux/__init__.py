
from .flux_service   import FluxService
from .flux_helper    import FluxHelper
from .flux_module    import FluxModule, spec_from_command, spec_from_dict

_fm = FluxModule()
if _fm.mode == 1: FluxHelper = _FluxHelperV1
else            : FluxHelper = _FluxHelperV0

