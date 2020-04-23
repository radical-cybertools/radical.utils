
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"

import os
os.environ['RADICAL_BASE_DIR'] = '/tmp'

import time
from uuid import UUID

import radical.utils as ru

ru_base_dir = ru.get_radical_base('utils')


# ------------------------------------------------------------------------------
#
def test_ids():
    '''
    Test ID generation
    '''

    id_a_1 = ru.generate_id('a')  # default mode is ID_SIMPLE
    id_a_2 = ru.generate_id('a', mode=ru.ID_SIMPLE)
    assert(id_a_1 == 'a.0000'), 'id_a_1 != a.0000'
    assert(id_a_2 == 'a.0001'), 'id_a_2 != a.0001'
    assert(id_a_1 != id_a_2),   'id_a_1 == id_a_2'

    _ = ru.generate_id('b', mode=ru.ID_UNIQUE)
    id_b_2 = ru.generate_id('b', mode=ru.ID_UNIQUE)
    assert(id_b_2.endswith('.0001')), 'id_b_2 does not end with ".0001"'
    os_pid_str = '%06d' % os.getpid()
    assert(os_pid_str in id_b_2), 'id_b_2 does not contain pid:%s' % os_pid_str

    id_c_1 = ru.generate_id('c', mode=ru.ID_UUID)
    assert(UUID(id_c_1.split('c.')[1])), 'id_c_1 does not have UUID'

    # ids that use file for counters (day_counter, item_counter)

    n_days = int(time.time() / (60 * 60 * 24))
    try:
        import getpass
        user = getpass.getuser()
    except:
        user = 'nobody'

    id_d_1 = ru.generate_id('d', mode=ru.ID_PRIVATE)
    assert(user in id_d_1), 'id_d_1 does not contain user:%s' % user

    # file format: /tmp/.radical/utils/ru_<user>_<n_days>.cnt
    fname = os.path.join(ru_base_dir, 'ru_%s_%s.cnt' % (user, n_days))
    assert(os.path.isfile(fname)), 'file with <day_counter> does not exist'

    with open(fname) as fd:
        day_counter = int(fd.readline())
    id_d_2 = ru.generate_id('d.%(day_counter)06d', mode=ru.ID_CUSTOM)
    assert(id_d_2 == 'd.%06d' % day_counter), 'id_d_2 != d.%06d' % day_counter

    ns = 'id_e'
    _ = ru.generate_id('e.%(item_counter)04d', mode=ru.ID_CUSTOM, ns=ns)
    # file format: /tmp/.radical/utils/id_e/ru_<user>_<cleaned_prefix>.cnt
    fname = os.path.join(ru_base_dir, ns, 'ru_%s_e.item_counter.cnt' % user)
    assert (os.path.isfile(fname)), 'file with <item_counter> does not exist'

    try                  : ru.generate_id(None)
    except TypeError     : pass
    except Exception as e: assert(False), 'TypeError != %s' % type(e)

    try                  : ru.generate_id(1)
    except TypeError     : pass
    except Exception as e: assert(False), 'TypeError != %s' % type(e)

    try                  : ru.generate_id('a.', mode='RANDOM')
    except ValueError    : pass
    except Exception as e: assert(False), 'ValueError != %s' % type(e)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_ids()


# ------------------------------------------------------------------------------

