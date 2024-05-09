
__author__    = 'RADICAL-Cybertools Team'
__copyright__ = 'Copyright 2021-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

# ------------------------------------------------------------------------------
#
# We provide a base class for all kinds of dict-like data objects in the RCT
# stack: resource configs, agent configs, job descriptions, task descriptions,
# pilot descriptions, workload descriptions etc.
#
# Class provides:
#
#   - dict like API
#   - public dict keys exposed as attributes ("Munch" approach)
#   - schema based type definitions
#   - optional runtime type checking
#

import copy
import sys

from .misc      import as_list, as_tuple, is_string


# ------------------------------------------------------------------------------
#
class TDErrorMixin:

    # --------------------------------------------------------------------------
    #
    def __init__(self, msg=None, level=1):

        f = sys._getframe(level)
        if 'self' in f.f_locals:
            cls_name = type(f.f_locals['self']).__name__
        elif 'cls' in f.f_locals:
            cls_name = f.f_locals['cls'].__name__
        else:
            cls_name = '<>'

        super().__init__('%s.%s%s' % (cls_name,
                                      f.f_code.co_name,
                                      msg and ' - %s' % msg or ''))


# ------------------------------------------------------------------------------


class TDError(TDErrorMixin, Exception):
    pass


class TDKeyError(TDErrorMixin, KeyError):
    pass


class TDTypeError(TDErrorMixin, TypeError):
    pass


class TDValueError(TDErrorMixin, ValueError):
    pass


# ------------------------------------------------------------------------------
#
class TypedDictMeta(type):

    # --------------------------------------------------------------------------
    #
    def __new__(mcs, name, bases, namespace):

        # guaranteed class "base" attributes
        _base_namespace = {
            '_schema'      : {},
            '_defaults'    : {},
            '_self_default': False,  # convert unschemed-dict into class itself
            '_check'       : False,  # attribute type checking on set
            '_cast'        : True    # attempt to cast on type mismatch
        }

        for _cls in bases:
            for k in _base_namespace.keys():
                _cls_v = getattr(_cls, k, None)
                if _cls_v is not None:
                    if   k == '_schema':
                        _base_namespace[k].update(_cls_v)
                    elif k == '_defaults':
                        _base_namespace[k].update(copy.deepcopy(_cls_v))
                    else:
                        _base_namespace[k] = _cls_v

        for k, v in _base_namespace.items():
            if isinstance(v, dict):
                v.update(namespace.get(k, {}))
                namespace[k] = v
            elif k not in namespace:
                namespace[k] = v

        _new_cls = super().__new__(mcs, name, bases, namespace)

        if _new_cls.__base__ is not dict:

            # register sub-classes
            from .serialize import register_serializable
            register_serializable(_new_cls)

        return _new_cls



# ------------------------------------------------------------------------------
#
class TypedDict(dict, metaclass=TypedDictMeta):

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None, **kwargs):
        '''
        Create a typed dictionary (tree) from `from_dict`.

        from_dict: data to be used for initialization

        NOTE: the names listed below are valid keys when used via the
              dictionary API, and can also be *set* via the property API, but
              they cannot be *queried* via the property API as their names
              conflict with the class method names:

                  as_dict
                  clear
                  get
                  items
                  keys
                  pop
                  popitem
                  setdefault
                  update
                  values
                  verify

              Names with a leading underscore are not supported.

              Supplied `from_dict` and kwargs are used to initialize the object
              data -- the `kwargs` take preceedence over the `from_dict` if both
              are specified (note that `from_dict` and `self` are invalid
              `kwargs`).
        '''

        from .serialize import register_serializable

        register_serializable(self.__class__)

        self.update(copy.deepcopy(self._defaults))
        self.update(from_dict)

        if kwargs:
            self.update(kwargs)


    # --------------------------------------------------------------------------
    #
    def update(self, other):
        '''
        Overload `dict.update()`: the call is used to ensure that sub-dicts are
        instantiated as their respective TypedDict-inheriting class types,
        if so specified by the respective schema.

        So, if the schema contains::

          {
            ...
            'foo' : BarTypedDict
            'foos': [BarTypedDict]
            ...
          }

        where `BarTypedDict` is a valid type in the scope of the schema
        definition, which inherits from `ru.TypedDict`, then `update()` will
        ensure that the value for key `foo` is indeed of type `ru.TypedDict`.
        An error will be raised if (a) `BarTypedDict` does not have a single
        parameter constructor like `ru.TypedDict`, or (b) the `data` value for
        `foo` cannot be used as `from_dict` parameter to the `BarTypedDict`
        constructor.
        '''
        if not other:
            return

        for k, v in other.items():
            if isinstance(v, dict):
                t = self._schema.get(k) or \
                    (type(self) if self._self_default else TypedDict)
                if isinstance(t, type) and issubclass(t, TypedDict):
                    # cast to expected TypedDict type
                    if not self.get(k):
                        self[k] = t()
                    self[k].update(v)
                    continue
            self[k] = v


    # --------------------------------------------------------------------------
    #
    def __deepcopy__(self, memo):
        '''
        return a new instance of the same type, not an original TypedDict,
        otherwise if an instance of TypedDict-based has an attribute of other
        TypedDict-based type then `verify` method will raise an exception
        '''
        return type(self)(from_dict=copy.deepcopy(self._data))


    # --------------------------------------------------------------------------
    #
    # base functionality to manage items
    #
    def __getitem__(self, k):
        return self._data[k]

    def __setitem__(self, k, v):
        self._data[k] = self._verify_setter(k, v)

    def __delitem__(self, k):
        del self._data[k]

    def __contains__(self, k):
        return k in self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def clear(self):
        self._data.clear()


    # --------------------------------------------------------------------------
    #
    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
            return default
        return self[key]

    def pop(self, key, default=None):
        if key in self:
            value = self[key]
            del self[key]
            return value
        elif default is not None:
            return default
        else:
            raise TDKeyError('key "%s" not found' % key)

    def popitem(self):
        if len(self):
            key = list(self.keys())[-1]  # LIFO if applicable
            value = self[key]
            del self[key]
            return key, value
        else:
            raise TDError('no data')


    # --------------------------------------------------------------------------
    #
    # base functionality for attribute access
    #
    def __getattr__(self, k):

        if k == '_data':
            if '_data' not in self.__dict__:
                self.__dict__['_data'] = dict()
            return self.__dict__['_data']

        if k.startswith('__'):
            return object.__getattribute__(self, k)

        data   = self._data
        schema = self._schema

        if   not  schema: return data.get(k)
        elif k in schema: return data.get(k)
        else            : return data[k]

    def __setattr__(self, k, v):

        if k.startswith('__'):
            return object.__setattr__(self, k, v)

        self._data[k] = self._verify_setter(k, v)

    def __delattr__(self, k):

        if k.startswith('__'):
            return object.__delattr__(self, k)

        del self._data[k]


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return '%s: %s' % (type(self).__qualname__, str(self))


    # --------------------------------------------------------------------------
    #
    def as_dict(self, _annotate=False):
        return as_dict(self._data, _annotate)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def __raise_type_error(cls, k, v, t, level=4):
        raise TDTypeError(
            'attribute "%s" - expected type %s, got %s' % (k, t, type(v)),
            level=level)

    @classmethod
    def _verify_base(cls, k, v, t):
        if cls._cast:
            try:
                return t(v)
            except (TypeError, ValueError):
                pass
        cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_bool(cls, k, v, t):
        if cls._cast:
            if str(v).lower() in ['true', 'yes', '1']:
                return True
            if str(v).lower() in ['false', 'no', '0']:
                return False
        cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_tuple(cls, k, v, t):
        if cls._cast:
            v = as_tuple(v)
            return tuple(
                [cls._verify_kvt(k + ' tuple element', _v, t[0]) for _v in v])
        else:
            if isinstance(v, tuple):
                return v
            cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_list(cls, k, v, t):
        if cls._cast:
            v = as_list(v)
            return [cls._verify_kvt(k + ' list element', _v, t[0]) for _v in v]
        else:
            if isinstance(v, list):
                return v
            cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_dict(cls, k, v, t):
        if cls._cast:
            t_k = list(t.keys())[0]
            t_v = list(t.values())[0]
            return {cls._verify_kvt(_k, _k, t_k):
                    cls._verify_kvt(_k, _v, t_v)
                    for _k, _v in v.items()}
        else:
            if issubclass(type(v), dict):
                return v
            cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_typeddict(cls, k, v, t):
        if cls._cast:
            if issubclass(type(v), t): return v.verify()
            # different TypedDict-base, but has a subset of schema
            if isinstance(v, dict)   : return t(from_dict=v).verify()
        else:
            if issubclass(type(v), t):
                return v
        cls.__raise_type_error(k, v, t)

    @classmethod
    def _verify_kvt(cls, k, v, t):
        if t is None or v is None      : return v
        if isinstance(t, type):
            if issubclass(t, TypedDict): return cls._verify_typeddict(k, v, t)
            # check base types
            if isinstance(v, t)        : return v
            if t in [str, int, float]  : return cls._verify_base(k, v, t)
            if t is bool               : return cls._verify_bool(k, v, t)
            cls.__raise_type_error(k, v, t, level=3)
        if isinstance(t, tuple)        : return cls._verify_tuple(k, v, t)
        if isinstance(t, list)         : return cls._verify_list(k, v, t)
        if isinstance(t, dict)         : return cls._verify_dict(k, v, t)
        if cls._cast                   : return v
        raise TDTypeError('no verifier defined for type %s' % t, level=2)

    def verify(self):

        if self._schema:
            for k, v in self._data.items():

                if k.startswith('__'):
                    continue

                if k not in self._schema:
                    raise TDKeyError('key "%s" not in schema' % k)

                self._data[k] = self._verify_kvt(k, v, self._schema[k])
        self._verify()
        return self


    # --------------------------------------------------------------------------
    #
    def _verify_setter(self, k, v):

        if   not self._check : return v
        elif not self._schema: return v

        if k not in self._schema:
            raise TDKeyError('key "%s" not in schema' % k, level=2)
        return self._verify_kvt(k, v, self._schema[k])


    # --------------------------------------------------------------------------
    #
    def _verify(self):
        '''
        Can be overloaded
        '''
        pass


    # --------------------------------------------------------------------------
    #
    def _query(self, key, default=None, last_key=True):
        '''
        For a query like

            typeddict.query('some.path.to.key', 'foo')

        this method behaves like:

            typeddict['some']['path']['to'].get('key', default='foo')

        flag `last_key` allows getting `default` value if any sub-key is missing
        (by default only if the last key is missing then return `default` value)
        '''
        if not key:
            raise TDKeyError('empty key on query')
        key_seq = key.split('.') if is_string(key) else list(key)

        output = self
        while key_seq:

            sub_key = key_seq.pop(0)

            if sub_key in output:
                output = output[sub_key]
                # if there are more sub-keys in a key sequence
                if key_seq and not isinstance(output, dict):
                    raise TDValueError(
                        'value for sub-key "%s" is not of dict type' % sub_key)

            elif not key_seq or not last_key:
                output = default
                break

            else:
                raise TDKeyError('intermediate key "%s" missed' % sub_key)

        return output


# ------------------------------------------------------------------------------
#
def as_dict(src, _annotate=False):
    '''
    Iterate given object, apply `as_dict()` to all typed
    values, and return the result (effectively a shallow copy).
    '''
    if isinstance(src, TypedDict):
        tgt = {k: as_dict(v, _annotate) for k, v in src.items()}
        if _annotate:
            tgt['_type'] = type(src).__name__
    elif isinstance(src, dict):
        tgt = {k: as_dict(v, _annotate) for k, v in src.items()}
    elif isinstance(src, list):
        tgt = [as_dict(x, _annotate) for x in src]
    elif isinstance(src, tuple):
        tgt = tuple([as_dict(x, _annotate) for x in src])
    else:
        tgt = src
    return tgt


# ------------------------------------------------------------------------------

