
from .flux_service   import FluxService
from .flux_helper_v0 import FluxHelperV0 as _FluxHelperV0
from .flux_helper_v1 import FluxHelperV1 as _FluxHelperV1
from .flux_module    import FluxModule, spec_from_command, spec_from_dict

_fm = FluxModule()
if _fm.version == 1: FluxHelper = _FluxHelperV1
else               : FluxHelper = _FluxHelperV0

