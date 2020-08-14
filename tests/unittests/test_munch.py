#!/usr/bin/env python3

import pytest
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_munch():
    # note that test_description.py also tests large parts of the Munch class in
    # a non-hierarchical setup.  This test is focused on inheritance: note that
    # all classes inherit from ru.Config which is also Munch derivative, but
    # provides simpler initialization which we use in these tests.

    # --------------------------------------------------------------------------
    # empty class
    class Foo(ru.Config):
        _schema = {'one': int,
                   'two': {str: int}}
        pass

    # --------------------------------------------------------------------------
    # plain schema
    class Bar_1(ru.Config):
        _schema = {'one': int,
                   'two': {str: int}}

    # --------------------------------------------------------------------------
    # class whose schema is composed
    class Buz_1(ru.Config):
        _schema = {'foo': int,
                   'bar': Bar_1}


    cfg = {'foo'   : 2,
           'bar'   : {'one': 3,
                      'two': {'buz': 2}}}
    r = Buz_1(cfg=cfg)
    r.verify()
    assert(type(r.bar).__name__ == 'Bar_1')
    assert(r.as_dict() == cfg), [cfg, r.as_dict()]


    # confirm that checks are active
    cfg = {'foo'   : 'foo',
           'bar'   : {'one': 3,
                      'two': {'buz': 2}}}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


    # confirm that composition is checked
    cfg = {'foo'   : 2,
           'bar'   : {'buz':  2}}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


    # confirm that typed composition works
    cfg = {'foo'   : 2,
           'bar'   : Bar_1(cfg={'buz':  2})}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


    # confirm that typed composition is checked
    cfg = {'foo'   : 2,
           'bar'   : Bar_1(cfg={'buz':  'buz'})}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


    # confirm that composition type is checked
    cfg = {'foo'   : 2,
           'bar'   : Foo()}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


    # confirm that nested types are checked
    cfg = {'foo'   : 2,
           'bar'   : {'one': 3,
                      'two': {'buz': 'bar'}}}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()

    # --------------------------------------------------------------------------
    # check if an inherited schema can be extended
    class Buz_2(Buz_1):
        # extend schema: add a type
        Buz_1._schema['biz'] = {str: int}

    cfg = {'foo'   : 2,
           'biz'   : {'buz': 0},
           'bar'   : {'one': 3,
                      'two': {'buz': 1}}}
    r = Buz_2(cfg=cfg)
    r.verify()

    # confirm that new type is checked
    cfg = {'foo'   : 2,
           'biz'   : {'buz': 'buz'},
           'bar'   : {'one': 3,
                      'two': {'buz': 1}}}
    r = Buz_2(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()

    # confirm that old type is checked
    cfg = {'foo'   : 2,
           'biz'   : {'buz': 0},
           'bar'   : {'one': 3,
                      'two': {'buz': 'buz'}}}
    r = Buz_2(cfg=cfg)
    with pytest.raises(TypeError):
        r.verify()


# ------------------------------------------------------------------------------
#
def test_munch_update():

    # --------------------------------------------------------------------------
    # plain schema
    class Bar_1(ru.Munch):
        _schema = {'one': int,
                   'two': {str: int}}

    # --------------------------------------------------------------------------
    # class whose schema is composed
    class Buz_1(ru.Munch):
        _schema = {'foo': int,
                   'bar': Bar_1}

    # --------------------------------------------------------------------------
    # class whose schema is composed and has default values
    class Foo_1(ru.Munch):
        _schema   = {'buz': Buz_1}
        _defaults = {'buz': {'foo': 0,
                             'bar': {'one': 1,
                                     'two': {'sub-two': 2}}}}

        def __init__(self, from_dict=None):
            super().__init__(from_dict=self._defaults)
            if from_dict:
                self.update(from_dict)

    f = Foo_1()
    assert(f.buz.foo == 0)
    assert(f.buz.bar.two['sub-two'] == 2)

    f = Foo_1({'buz': {'foo': 3,
                       'bar': {'one': 11}}})
    assert(f.buz.bar.one == 11)
    assert(f.buz.bar.two['sub-two'] == 2)

    f = Foo_1({'buz': {'foo': 3,
                       'bar': {'one': 0,
                               'two': {2: 22}}}})
    f.verify()  # method `verify` convert int into str according to the scheme
    assert(f.buz.bar.two['2'] == 22)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_munch()
    test_munch_update()

# ------------------------------------------------------------------------------

