#!/usr/bin/env python3

# noqa: E201


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_metric_expand():

    d_in  = {'total'     : [{'ru.EVENT': 'bootstrap_0_start'},
                            {'ru.EVENT': 'bootstrap_0_stop' }],
             'boot'      : [{'ru.EVENT': 'bootstrap_0_start'},
                            {'ru.EVENT': 'sync_rel'         }],
             'setup_1'   : [{'ru.EVENT': 'sync_rel'         },
                            {'ru.STATE': 'rp.PMGR_ACTIVE'   }],
             'ignore'    : [{'ru.STATE': 'rp.PMGR_ACTIVE'   },
                            {'ru.EVENT': 'cmd'              ,
                             'ru.MSG  ': 'cancel_pilot'     }],
             'term'      : [{'ru.EVENT': 'cmd'              ,
                             'ru.MSG  ': 'cancel_pilot'     },
                            {'ru.EVENT': 'bootstrap_0_stop' }]}

    d_out = {'total'     : [{1         : 'bootstrap_0_start'},
                            {1         : 'bootstrap_0_stop' }],
             'boot'      : [{1         : 'bootstrap_0_start'},
                            {1         : 'sync_rel'         }],
             'setup_1'   : [{1         : 'sync_rel'         },
                            {5         : 'PMGR_ACTIVE'      }],
             'ignore'    : [{5         : 'PMGR_ACTIVE'      },
                            {1         : 'cmd'              ,
                             6         : 'cancel_pilot'     }],
             'term'      : [{1         : 'cmd'              ,
                             6         : 'cancel_pilot'     },
                            {1         : 'bootstrap_0_stop' }]}

    assert(ru.metric_expand(d_in) == d_out)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_metric_expand()


# ------------------------------------------------------------------------------

