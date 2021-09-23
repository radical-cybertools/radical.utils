#!/usr/bin/env python3

import os
import time
import pytest

import radical.utils as ru


yaml   = pytest.importorskip('yaml')
flux   = pytest.importorskip('flux')
events = dict()
spec   = {
             "tasks": [{
                 "slot": "task",
                 "count": {
                     "per_slot": 1
                 },
                 "command": [
                     "/bin/date"
                 ]
             }],
             "attributes": {
                 "system": {
                     "duration": 10000
                 }
             },
             "version": 1,
             "resources": [{
                 "count": 1,
                 "type" : "slot",
                 "label": "task",
                 "with": [{
                     "count": 1,
                     "type": "core"
                 }]
             }]
         }


# ------------------------------------------------------------------------------
#
def test_flux_startup():

    global events

    njobs    = 10
    events   = dict()

    def cb1(job_id, state, ts, context):

      # print([job_id, state, ts, context])
        if job_id not in events:
            events[job_id] = [ts, state]
        else:
            events[job_id].append([ts, state])


    fh = ru.FluxHelper()
    fh.start_flux()

    assert(fh.uri)
    assert('FLUX_URI' in fh.env)

    specs = [spec] * njobs
    ids   = fh.submit_jobs(specs, cb=cb1)
    assert(len(ids) == njobs), len(ids)

    time.sleep(5)

    assert(len(events) == njobs), len(events)
    for jid in events:
        # we expect at least 4 events per job:
        # 'submit', 'start', 'finish', 'clean',
        assert(len(events[jid]) >= 4), [jid, events[jid]]

    fh.reset()
    assert(fh.uri is None)


# ------------------------------------------------------------------------------
#
def test_flux_pickup():

    global events

    njobs    = 10
    events   = dict()
    outer_fh = None

    if 'FLUX_URI' not in os.environ:
        outer_fh = ru.FluxHelper()
        outer_fh.start_flux()

        for k,v in outer_fh.env.items():
            os.environ[k] = v

    def cb1(job_id, state, ts, context):

      # print([job_id, state, ts, context])
        if job_id not in events:
            events[job_id] = [ts, state]
        else:
            events[job_id].append([ts, state])

    fh = ru.FluxHelper()
    fh.start_flux()

    assert(fh.uri)
    assert('FLUX_URI' in fh.env)

    specs = [spec] * njobs
    ids   = fh.submit_jobs(specs, cb=cb1)
    assert(len(ids) == njobs), len(ids)

    time.sleep(5)

    assert(len(events) == njobs), len(events)
    for jid in events:
        # we expect at least 4 events per job:
        # 'submit', 'start', 'finish', 'clean',
        assert(len(events[jid]) >= 4), [jid, events[jid]]

    fh.reset()
    assert(fh.uri is None)

    if outer_fh:
        outer_fh.reset()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_flux_startup()
    test_flux_pickup()


# ------------------------------------------------------------------------------

