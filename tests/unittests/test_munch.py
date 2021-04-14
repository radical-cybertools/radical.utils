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
        # will not be able to convert attribute "foo" (cast=True)
        r.verify()


    # confirm that composition is checked
    cfg = {'foo'   : 2,
           'bar'   : {'buz':  2}}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        # will not be able to convert attribute "bar" (cast=True)
        r.verify()


    # confirm that typed composition works
    cfg = {'foo'   : 2,
           'bar'   : Bar_1(cfg={'buz':  2})}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        # will not be able to convert input data for `Bar_1` (cast=True)
        r.verify()


    # confirm that typed composition is checked
    cfg = {'foo'   : 2,
           'bar'   : Bar_1(cfg={'buz':  'buz'})}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        # will not be able to convert input data for `Bar_1` (cast=True)
        r.verify()


    # confirm that composition type is checked
    cfg = {'foo'   : 2,
           'bar'   : Foo()}
    r = Buz_1(cfg=cfg)
    # no TypeError, since value of attribute "bar" will be converted
    # classes `Foo` and `Bar_1` have the same schemas
    r.verify()


    # confirm that nested types are checked
    cfg = {'foo'   : 2,
           'bar'   : {'one': 3,
                      'two': {'buz': 'bar'}}}
    r = Buz_1(cfg=cfg)
    with pytest.raises(TypeError):
        # will not be able to convert input data for `Bar_1` (attribute `two`)
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
def test_munch_defaults():

    # --------------------------------------------------------------------------
    # plain schema
    class Foo(ru.Munch):
        _schema   = {'one'  : int,
                     'two'  : {str: int},
                     'three': [str],
                     'four' : float,
                     'five' : bool}
        _defaults = {'two'  : {'two-default': 0},
                     'three': [3, 3, 3],
                     'four' : None,
                     'five' : True}
    # --------------------------------------------------------------------------


    f = Foo()
    assert (f.one   is None)
    assert (f.two   == {'two-default': 0})
    assert (f.three == [3, 3, 3])
    assert (f.four  is None)
    assert (f.five  is True)


# ------------------------------------------------------------------------------
#
def test_munch_update():

    # --------------------------------------------------------------------------
    # plain schema
    class Bar_1(ru.Munch):
        _schema   = {'one'  : int,
                     'two'  : {str: int},
                     'three': [str]}
        _defaults = {'two'  : {'two-default': 0},
                     'three': [3, 3, 3]}

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

    # --------------------------------------------------------------------------
    f = Foo_1()
    assert (f.buz.foo == 0)
    assert (f.buz.bar.two['sub-two'] == 2)
    assert (f.buz.bar.three == [3, 3, 3])

    f = Foo_1({'buz': {'foo': 3,
                       'bar': {'one': 11}}})
    assert (f.buz.bar.one == 11)
    assert (f.buz.bar.two['sub-two'] == 2)

    f = Foo_1({'buz': {'foo': 3,
                       'bar': {'one': 0,
                               'two': {2: 22}}}})
    f.verify()  # method `verify` convert int into str according to the schema
    assert (f.buz.bar.two['2'] == 22)

    f = Foo_1()
    b = Bar_1()
    f.update({'buz': {'bar': b}})
    # default values from Bar_1 class
    assert (f.buz.bar.three == [3, 3, 3])
    assert (f.buz.bar['two'] == {'two-default': 0})
    assert (f.buz.bar.get('one') is None)

    # test `cls._self_default` flag (for method `update`)

    # --------------------------------------------------------------------------
    # cls._self_default is False
    class Bar_2a(ru.Munch):
        _self_default = False

    # --------------------------------------------------------------------------
    # cls._self_default is True
    class Bar_2b(ru.Munch):
        _self_default = True

    # --------------------------------------------------------------------------
    bar = Bar_2a(from_dict={'foo_0': {'foo_1': {'foo2': 'bar'}}})
    assert (not isinstance(bar.foo_0, Bar_2a))
    assert (isinstance(bar.foo_0.foo_1, ru.Munch))

    bar = Bar_2b(from_dict={'foo_0': {'foo_1': {'foo2': 'bar'}}})
    assert (isinstance(bar.foo_0, Bar_2b))
    assert (issubclass(type(bar.foo_0.foo_1), ru.Munch))


# ------------------------------------------------------------------------------
#
def test_munch_inherit():

    # --------------------------------------------------------------------------
    # plain schema
    class Foo(ru.Munch):
        _schema   = {'one'  : int,
                     'two'  : {str: int},
                     'three': [str]}
        _defaults = {'two'  : {'two-default': 0},
                     'three': [3, 3, 3]}

    # --------------------------------------------------------------------------
    # class whose schema is composed
    class Bar(Foo):
        _schema_extend   = {'four': int,
                            'five': float,
                            'six' : Foo}
        _defaults_extend = {'four': 42,
                            'six' : {'one'  : 1,
                                     'three': [33, 33]}}

    # --------------------------------------------------------------------------
    b = Bar()

    assert (b.one                is None)
    assert (b.two['two-default'] == 0)
    assert (b.three              == [3, 3, 3])
    assert (b.four               == 42), b.four
    assert (b.five               is None)
    assert (b.six.one            == 1)
    assert (b.six.three[1]       == 33)


# ------------------------------------------------------------------------------
#
def test_demunch():

    # --------------------------------------------------------------------------
    #
    class Bar_1(ru.Munch):
        _schema = {'bars': list}

    # --------------------------------------------------------------------------
    #
    class Buz_1(ru.Munch):
        _schema = {'foo': int,
                   'bar': Bar_1,
                   'buz': tuple}

    # --------------------------------------------------------------------------

    b = Bar_1({'bars': ['bar_1', Bar_1({'bars': [1, 2, 3]})]})
    b_dict = b.as_dict()
    assert isinstance(b_dict,            dict)
    assert not isinstance(b_dict,        Bar_1)
    assert isinstance(b_dict['bars'],    list)
    assert isinstance(b_dict['bars'][1], dict)
    assert b_dict['bars'][0] == 'bar_1'
    assert len(b_dict['bars'][1]['bars']) == 3

    b = Buz_1({'foo': 3, 'bar': Bar_1({'bars': [3, 2, 1]}), 'buz': tuple([5])})
    b_dict = b.as_dict()
    assert isinstance(b_dict,                dict)
    assert isinstance(b_dict['bar'],         dict)
    assert isinstance(b_dict['bar']['bars'], list)
    assert isinstance(b_dict['buz'],         tuple)
    assert not isinstance(b_dict['bar'],     Bar_1)
    assert b_dict['foo'] == 3
    assert b_dict['buz'][0] == 5


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_munch()
    test_munch_defaults()
    test_munch_update()
    test_munch_inherit()
    test_demunch()

# ------------------------------------------------------------------------------

