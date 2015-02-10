
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_ids () :
    """
    Test ID generation
    """

    id_a_1 = ru.generate_id ('a')
    id_a_2 = ru.generate_id ('a')
    id_a_3 = ru.generate_id ('a', mode=ru.ID_SIMPLE)
    id_b_1 = ru.generate_id ('b', mode=ru.ID_SIMPLE)
    id_b_2 = ru.generate_id ('b', mode=ru.ID_SIMPLE)
    id_c_1 = ru.generate_id ('c', mode=ru.ID_UNIQUE)
    id_c_2 = ru.generate_id ('c', mode=ru.ID_UNIQUE)
    id_d_1 = ru.generate_id ('d.%(day_counter)03d', mode=ru.ID_CUSTOM)

    assert (id_a_1 == 'a.0000'       )
    assert (id_a_2 == 'a.0001'       )
    assert (id_a_3 == 'a.0002'       )
    assert (id_b_1 == 'b.0000'       )
    assert (id_b_2 == 'b.0001'       )
    assert (id_c_1.startswith  ('c.'))
    assert (id_c_2.startswith  ('c.'))
    assert (id_c_1.endswith ('.0000'))
    assert (id_c_2.endswith ('.0001'))
    assert (id_d_1.startswith  ('d.'))

    try                   : id_x = ru.generate_id (None)
    except TypeError      : pass
    except Exception as e : assert (False), "TypeError  != %s" % type(e)

    try                   : id_x = ru.generate_id (1)
    except TypeError      : pass
    except Exception as e : assert (False), "TypeError  != %s" % type(e)

    try                   : id_x = ru.generate_id ('a.', mode='RANDOM')
    except ValueError     : pass
    except Exception as e : assert (False), "ValueError != %s" % type(e)


# ------------------------------------------------------------------------------

