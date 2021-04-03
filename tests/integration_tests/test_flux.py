#!/usr/bin/env python3

import json
import time
import pprint

import radical.utils as ru


import pytest
yaml = pytest.importorskip('yaml')
flux = pytest.importorskip('flux')


# ------------------------------------------------------------------------------
#
def test_flux():

    def cb1(job_id, state, ts, context):
        print('=== event ===', end='')
        pprint.pprint([job_id, state, ts, context])

    fh   = None
    uid  = None
    info = None

    try:
        fh   = ru.FluxHelper()
        info = fh.start_service()
        uid  = info['uid']

        fh.register_callback(uid, cb1)

        print('\n--- start', uid)
        pprint.pprint(info)

        print('\n--- check', uid)
        pprint.pprint(fh.check_service(info['uid']))

        print('\n--- handle', uid)
        pprint.pprint(fh.get_handle(info['uid']))

        print('\n--- executor', uid)
        jex = fh.get_executor(info['uid'])
        pprint.pprint(jex)
        jobspec = json.dumps(ru.read_json('/tmp/spec.json'))
        fut   = jex.submit(jobspec, waitable=True)
        jobid = fut.jobid()
        print('\n--- job', uid, jobid)
        time.sleep(3)

        print('\n--- check', uid)
        pprint.pprint(fh.check_service(info['uid']))

        print('\n--- close', uid)
        pprint.pprint(fh.close_service(info['uid']))

    finally:

        if uid:
            try:
                print('\n--- final', uid)
                pprint.pprint(fh.check_service(info['uid']))
            except:
                print('\n--- ok')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_flux()


# ------------------------------------------------------------------------------

