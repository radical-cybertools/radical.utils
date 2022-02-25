# pylint: disable=protected-access

__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from unittest import TestCase

from radical.utils.typeddict import TDError, TDKeyError
from radical.utils.typeddict import TDTypeError, TDValueError
from radical.utils           import TypedDict


# ------------------------------------------------------------------------------
#
class TypedDictTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        class TDEmpty(TypedDict):
            pass

        class TDSimple(TypedDict):

            _schema = {
                'attr_str' : str,
                'attr_int' : int,
                'attr_dict': {str: None}
            }

            _defaults = {
                'attr_str' : 'string_default',
                'attr_int' : '1',
                'attr_dict': {'dict_key_00': 100}
            }

        class TDDoubleLevel(TypedDict):

            _schema = {
                'id'       : int,
                'simple_td': TDSimple
            }

        # ----------------------------------------------------------------------

        cls.test_td_cases = [
            (TDEmpty,       {'attr_any_00': 'attr_00',
                             'attr_any_01': 'attr_01'}),
            (TDSimple,      {'attr_str' : 'non_default',
                             'attr_int' : 2,
                             'attr_dict': {'dict_key_00': 111}}),
            (TDDoubleLevel, {'id'       : 3,
                             'simple_td': {'attr_str' : 'simple_td_str',
                                           'attr_int' : 10}})
        ]

    # --------------------------------------------------------------------------
    #
    def test_init(self):

        def _assertion_in_loop(td_obj_value, input_value):
            if isinstance(input_value, (dict, TypedDict)):
                for _k, _v in input_value.items():
                    self.assertIn(_k, td_obj_value)
                    _assertion_in_loop(td_obj_value[_k], _v)
            elif isinstance(input_value, (list, tuple)):
                self.assertTrue(isinstance(td_obj_value, (list, tuple)))
                for _idx in range(len(input_value)):
                    _assertion_in_loop(td_obj_value[_idx],
                                       input_value[_idx])
            else:
                self.assertEqual(td_obj_value, input_value)

        # ----------------------------------------------------------------------

        for cls, input_data in self.test_td_cases:

            obj = cls()

            if obj._schema and obj._defaults:

                self.assertEqual(len(obj), len(obj._defaults))

                for k, v in cls._defaults.items():
                    self.assertEqual(obj[k], v)

                obj.verify()
                for k in obj.keys():
                    if isinstance(cls._schema[k], type):
                        t = cls._schema[k]
                    else:
                        t = type(cls._schema[k])
                    self.assertIsInstance(obj[k], t)

                obj.random_attr = 'random'
                with self.assertRaises(KeyError):
                    # due to "random_attr" is not in schema
                    obj.verify()

            elif not obj._schema:

                self.assertEqual(len(obj), 0)

                # add one element
                obj.random_attr = 'random'
                self.assertEqual(len(obj), 1)
                self.assertIn('random_attr', obj.keys())
                # verification will be skipped since there is no "_schema"
                obj.verify()

            if input_data:

                obj = cls(from_dict=input_data)
                _assertion_in_loop(td_obj_value=obj, input_value=input_data)

    # --------------------------------------------------------------------------
    #
    def test_hash(self):

        class TDHashed(TypedDict):

            def __hash__(self):
                return hash(tuple(sorted(self.items())))

        # ----------------------------------------------------------------------

        obj = TypedDict()

        with self.assertRaises(TypeError) as e:
            hash(obj)
        self.assertIn('unhashable type', str(e.exception))

        input_data = {'a': 'a', 'b': 'b'}
        self.assertEqual(hash(TDHashed(input_data)), hash(TDHashed(input_data)))

    # --------------------------------------------------------------------------
    #
    def test_self_default(self):

        # test `cls._self_default` flag (for method `update`)

        class TDNonSelf(TypedDict):

            _self_default = False

        class TDSelf(TypedDict):

            _self_default = True

        # ----------------------------------------------------------------------

        obj1 = TDNonSelf(from_dict={'level0': {'level1': {'level2': 'value'}}})
        self.assertIsInstance(obj1, TDNonSelf)
        self.assertNotIsInstance(obj1.level0, TDNonSelf)
        self.assertIsInstance(obj1.level0.level1, TypedDict)

        obj2 = TDSelf(from_dict={'level0': {'level1': {'level2': 'value'}}})
        self.assertIsInstance(obj2, TDSelf)
        self.assertIsInstance(obj2.level0, TDSelf)
        self.assertTrue(issubclass(type(obj2.level0.level1), TypedDict))

        # all instances are treated as `dict`
        self.assertIsInstance(obj1, dict)
        self.assertIsInstance(obj2, dict)

    # --------------------------------------------------------------------------
    #
    def test_verify(self):

        # attribute "_cast" is True by default

        class TDBase(TypedDict):

            _schema = {
                'attr_int' : int,
                'attr_dict': {str: int}
            }

        class TDMain(TypedDict):

            _schema = {
                'attr_main_int': int,
                'attr_main_td' : TDBase
            }

        class TDBaseCopy(TypedDict):

            _schema = {
                'attr_int' : int,
                'attr_dict': {str: int}
            }

        # ----------------------------------------------------------------------

        input_data = {'attr_main_int': 2,
                      'attr_main_td' : {'attr_int' : 3,
                                        'attr_dict': {'attr_any': 2}}}
        obj = TDMain(input_data)
        obj.verify()
        self.assertEqual(type(obj.attr_main_td).__name__, 'TDBase')
        self.assertEqual(obj.as_dict(), input_data)

        with self.assertRaises(TypeError):
            # will not be able to convert attribute "attr_main_int"
            TDMain({'attr_main_int': 'non_int',
                    'attr_main_td' : {'attr_int' : 3,
                                      'attr_dict': {'attr_any': 2}}}).verify()

        with self.assertRaises(KeyError):
            # will not be able to convert attribute "attr_main_td"
            TDMain({'attr_main_int': 2,
                    'attr_main_td' : {'not_in_schema': 3}}).verify()

        with self.assertRaises(KeyError):
            # will not be able to convert input data for `TDBase`
            TDMain({'attr_main_int': 2,
                    'attr_main_td' : TDBase({'not_in_schema': 3})}).verify()

        with self.assertRaises(TypeError):
            # will not be able to convert attribute "attr_main_td.attr_dict"
            TDMain({'attr_main_int': 2,
                    'attr_main_td' : {'attr_int' : 3,
                                      'attr_dict': {'attr_any': 'a'}}}).verify()

        obj = TDMain({'attr_main_int': 2,
                      'attr_main_td' : TDBaseCopy()})
        # no TDError, since value of attribute "attr_main_td" will be converted
        # classes `TDBase` and `TDBaseCopy` have the same schemas
        obj.verify()

    # --------------------------------------------------------------------------
    #
    def test_verify_setter(self):

        class TDBaseNoSchema(TypedDict):

            _check = True

        class TDBase(TypedDict):

            _check = True

            _schema = {
                'attr_str'  : str,
                'attr_float': float
            }

        # ----------------------------------------------------------------------

        # without set "_schema"
        obj_wo_schema = TDBaseNoSchema()

        obj_wo_schema.attr_str = 'str_value'
        self.assertIsInstance(obj_wo_schema.attr_str, str)
        obj_wo_schema.attr_dict = {}
        self.assertIsInstance(obj_wo_schema.attr_dict, dict)

        # with provided "_schema"
        obj = TDBase()

        with self.assertRaises(KeyError):
            # provided attribute is not in schema
            obj.attr_dict = {}

        # attribute "_cast" is True by default
        obj.attr_str = 1
        self.assertIsInstance(obj.attr_str, str)
        self.assertEqual(obj.attr_str, '1')
        obj.attr_float = 1
        self.assertIsInstance(obj.attr_float, float)
        self.assertEqual(obj.attr_float, 1.)
        with self.assertRaises(TypeError):
            # couldn't convert provided value into a corresponding type (float)
            obj.attr_float = 'str_value'

        # set "_cast" to False
        TDBase._cast = False
        obj.attr_str = 'new_str_value'
        self.assertIsInstance(obj.attr_str, str)
        with self.assertRaises(TypeError):
            # no attempts to convert between types
            obj.attr_str = 1

    # --------------------------------------------------------------------------
    #
    def test_base_methods(self):

        input_dict = {'attr_any': 'attr_any_str'}
        obj = TypedDict(from_dict=input_dict)

        self.assertEqual(obj.keys(), input_dict.keys())

        for k in obj:
            self.assertIn(k, input_dict)

        for k in obj.keys():
            self.assertIn(k, input_dict)

        for v in obj.values():
            self.assertIn(v, input_dict.values())

        for k, v in obj.items():
            self.assertIn(k, input_dict)
            self.assertEqual(v, input_dict[k])

        k = list(input_dict)[0]

        self.assertEqual(obj.get(k), input_dict[k])
        self.assertEqual(obj.get('non_set_attr', 'default_v'), 'default_v')

        self.assertEqual(obj.setdefault(k), input_dict[k])
        pre_setdefault_len = len(obj)
        self.assertEqual(obj.setdefault('new_attr', 'new_value'), 'new_value')
        self.assertIn('new_attr', obj)
        self.assertEqual(len(obj), pre_setdefault_len + 1)

        obj.clear()
        self.assertEqual(len(obj), 0)

        class TDSchemed(TypedDict):

            _schema = {
                'attr_str'  : str,
                'attr_float': float
            }

        tds = TDSchemed()
        self.assertIsNone(tds.attr_str)
        with self.assertRaises(KeyError):
            # not defined earlier and not from schema
            _ = tds.unknown_key

        tds.unknown_key = 'unknown_key_value'
        self.assertEqual(tds.unknown_key, 'unknown_key_value')

        del tds.unknown_key
        self.assertNotIn('unknown_key', tds)

        # `__str__` method checked
        self.assertEqual('%s' % tds, '{}')
        # `__repr__` method checked
        self.assertIn('TDSchemed object, schema keys', '%r' % tds)

    # --------------------------------------------------------------------------
    #
    def test_pop(self):

        for cls, _ in self.test_td_cases:

            obj = cls()
            if obj._schema and obj._defaults:

                initial_obj_keys = list(obj.keys())
                for k in initial_obj_keys:
                    v = obj.pop(k)
                    self.assertIsNotNone(v)
                    self.assertNotIn(k, obj)

            v = 'random_value'
            self.assertEqual(v, obj.pop('unknown_key', v))

            with self.assertRaises(KeyError) as e:
                obj.pop('unknown_key')
            self.assertIn('not found', str(e.exception))

    # --------------------------------------------------------------------------
    #
    def test_popitem(self):

        for cls, _ in self.test_td_cases:

            obj = cls()
            if obj._schema and obj._defaults:

                num_elements = len(obj)
                for _ in range(num_elements):
                    k, v = obj.popitem()
                    self.assertIsNotNone(v)
                    self.assertNotIn(k, obj)

            with self.assertRaises(TDError) as e:
                obj.popitem()
            self.assertIn('no data', str(e.exception))

    # --------------------------------------------------------------------------
    #
    def test_query(self):

        class TDLowLevel(TypedDict):

            _schema = {
                'low_dict': {str: None},
                'low_str' : str
            }

            _defaults = {
                'low_dict': {'low_dict_int': 1},
                'low_str' : 'low_any_string'
            }

        class TDHighLevel(TypedDict):

            _schema = {
                'high_dict': dict,
                'high_td'  : TDLowLevel,
                'high_str' : str
            }

        # ----------------------------------------------------------------------

        td_obj = TDHighLevel({
            'high_dict': {
                'high_dict_l1': {
                    'high_dict_str': 'l2_output'
                }
            },
            'high_td' : TDLowLevel(),
            'high_str': 'high_any_string'
        })

        self.assertIsInstance(
            td_obj._query('high_dict.high_dict_l1.high_dict_str'), str)
        self.assertIsInstance(
            td_obj._query(['high_dict', 'high_dict_l1', 'high_dict_str']), str)
        self.assertIsInstance(
            td_obj._query('high_str'), str)
        self.assertIsInstance(
            td_obj._query('high_td.low_str'), str)
        self.assertIsInstance(
            td_obj._query('high_td.low_dict'), dict)
        self.assertIsInstance(
            td_obj._query('high_td.low_dict.low_dict_int'), int)

        self.assertIsNone(td_obj._query('high_dict.high_dict_l1.unknown_key'))
        self.assertIsNone(td_obj._query('high_td.low_dict.unknown_key'))

        self.assertIsInstance(td_obj._query('high_td.low_dict.unknown_key',
                                            default=1), int)
        self.assertIsInstance(td_obj._query('high_td.unknown_sub_key.levelK',
                                            default=1,
                                            last_key=False), int)

        with self.assertRaises(KeyError):
            td_obj._query('')

        with self.assertRaises(TDKeyError):
            td_obj._query(None)

        with self.assertRaises(ValueError):
            td_obj._query('high_dict.high_dict_l1.high_dict_str.within_non_dict')

        with self.assertRaises(TDValueError):
            td_obj._query('high_td.low_dict.low_dict_int.within_non_dict')

        with self.assertRaises(KeyError):
            td_obj._query('high_td.unknown_sub_key.levelK')

    # --------------------------------------------------------------------------
    #
    def test_metaclass(self):

        class TD1Base(TypedDict):

            _self_default = True

            _schema = {
                'base_int': int,
                'base_str': str
            }

        class TD2Base(TD1Base):

            _cast = False

            _schema = {
                'base_int': float
            }

            _defaults = {
                'base_int': .5
            }

        class TD3Base(TD2Base):
            pass

        # ----------------------------------------------------------------------

        # check that "base" attributes are provided
        for base_attr in ['_schema',
                          '_defaults',
                          '_self_default',
                          '_check',
                          '_cast']:
            self.assertTrue(hasattr(TD1Base, base_attr))
            self.assertTrue(hasattr(TD2Base, base_attr))
            self.assertTrue(hasattr(TD3Base, base_attr))

        # inherited "_defaults" from TD2Base
        self.assertEqual(getattr(TD3Base, '_defaults')['base_int'], .5)

        # inherited "_schema" from TD2Base and TD1Base
        self.assertIs(getattr(TD3Base, '_schema')['base_int'], float)
        self.assertIs(getattr(TD2Base, '_schema')['base_int'], float)
        self.assertIs(getattr(TD1Base, '_schema')['base_int'], int)

        # inherited "_self_default" from TD1Base (default value is False)
        self.assertTrue(getattr(TD3Base, '_self_default'))

        # inherited "_cast" from TD2Base
        self.assertFalse(getattr(TD3Base, '_cast'))
        self.assertFalse(getattr(TD2Base, '_cast'))
        # inherited from TypedDict (TD1Base class didn't change this attribute)
        self.assertTrue(getattr(TD1Base, '_cast'))

        # inherited from TD1Base ("_schema")
        td3 = TD3Base({'base_int': 10, 'base_str': 20})
        # exception due to `TD3Base._cast = False` (inherited from TD2Base)
        with self.assertRaises(TypeError):
            td3.verify()
        with self.assertRaises(TDTypeError) as e:
            td3.verify()
        self.assertIn('attribute "base_int" - expected type', str(e.exception))
        # NOTE: control flags should be set through the class only
        TD3Base._cast = True
        td3.verify()
        self.assertIsInstance(td3.base_int, float)
        self.assertIsInstance(td3.base_str, str)
        self.assertEqual(td3.base_int, 10.)
        self.assertEqual(td3.base_str, '20')

    # --------------------------------------------------------------------------
    #
    def test_tderrors(self):

        def raise_exception(level=1):
            raise TDError('level %s' % level, level=level)

        def raise_exception_wrapper(level):
            raise_exception(level=level)

        class TDErrorClass:

            @classmethod
            def raise_cls_tderror(cls):
                raise TDError

            def raise_self_tderror(self):
                raise TDError

        # ----------------------------------------------------------------------

        with self.assertRaises(TDError) as e:
            raise_exception()
        self.assertIn('.raise_exception - level 1', str(e.exception))
        # no class or instance provided
        self.assertIn('<>', str(e.exception))

        with self.assertRaises(TDError) as e:
            raise_exception_wrapper(level=1)
        self.assertIn('.raise_exception - level 1', str(e.exception))

        with self.assertRaises(TDError) as e:
            raise_exception_wrapper(level=2)
        self.assertIn('.raise_exception_wrapper - level 2', str(e.exception))

        # exception raised within class/instance methods

        with self.assertRaises(TDError) as e:
            # `'cls' in f.f_locals` -> True
            TDErrorClass.raise_cls_tderror()
        self.assertIn('TDErrorClass', str(e.exception))

        with self.assertRaises(TDError) as e:
            # `'self' in f.f_locals` -> True
            TDErrorClass().raise_self_tderror()
        self.assertIn('TDErrorClass', str(e.exception))

        # catch exceptions by their parent classes

        with self.assertRaises(Exception):
            raise TDError

        with self.assertRaises(KeyError):
            raise TDKeyError

        with self.assertRaises(TypeError):
            raise TDTypeError

        with self.assertRaises(ValueError):
            raise TDValueError

# ------------------------------------------------------------------------------
