#!/usr/bin/env python3

import pprint

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_flux():

    def cb(job, info):
        pprint.pprint(['cb', job.uid, info])


    fh   = None
    uid  = None
    info = None

    try:
        fh   = ru.FluxHelper()
        info = fh.start_service()
        uid  = info['uid']

        print('\n--- start', uid)
        pprint.pprint(info)

        print('\n--- check', uid)
        pprint.pprint(fh.check_service(info['uid']))

        print('\n--- handle', uid)
        pprint.pprint(fh.get_handle(info['uid']))

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

