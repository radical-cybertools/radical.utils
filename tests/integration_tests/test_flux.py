#!/usr/bin/env python3

import os
import time
import pytest

import radical.utils as ru


if not bool(ru.which('flux')):
    pytest.skip('flux not installed', allow_module_level=True)


yaml   = pytest.importorskip('yaml')
flux   = pytest.importorskip('flux')
events = dict()
spec   = ru.flux.spec_from_command(cmd='/bin/date')


# ------------------------------------------------------------------------------
#
def test_flux():

    global events

    njobs    = 10
    events   = dict()

    def cb1(job_id, state):

        if job_id not in events:
            events[job_id] = [state]
        else:
            events[job_id].append(state)


    fs = ru.FluxService()
    fs.start(timeout=10)

    assert fs.uri

    fh = ru.FluxHelper(uri=fs.uri)
    fh.register_cb(cb1)

    specs = [spec] * njobs
    ids   = fh.submit(specs)
    assert len(ids) == njobs, len(ids)

    time.sleep(5)

    assert len(events) == njobs, len(events)
    for jid in events:
        # we expect at least 4 events per job:
        # 'submit', 'start', 'finish', 'clean',
        assert len(events[jid]) >= 4, [jid, events[jid]]

    fh.stop()
    assert fh.uri is None

    fs.stop()
    assert fs.uri is None


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_flux()


# ------------------------------------------------------------------------------

