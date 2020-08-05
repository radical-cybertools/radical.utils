
# ------------------------------------------------------------------------------
#
# We provide a base class for all kinds of dict-like data objects in the RCT
# stack: resource configs, agent configs, job descriptions, task descriptions,
# pilot descriptions, workload descriptions etc.  That class provides:
#
#   - dict like API
#   - public dict keys exposed as attributes
#   - schema based type definitions
#   - optional runtime type checking
#

import copy

from .misc       import as_list, as_tuple
from .dict_mixin import DictMixin


# ------------------------------------------------------------------------------
#
class Munch(DictMixin):

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None, schema=None):
        '''
        create a munchified dictionary (tree) from `from_dict`.

        from_dict: data to be used for initialization

        NOTE: the names listed below are valid keys when used via the
              dictionary API, and can also be *set* via the property API, but
              they cannot be *queried* via the property API as their names
              conflict with the class method names:

                  as_dict
                  clear
                  get
                  has_key
                  items
                  iterkeys
                  itervalues
                  keys
                  popitem
                  setdefault
                  update
                  values
                  verify

              Names with a leading underscore are not supported.
        '''

        if schema:
            self._schema = schema

        elif not hasattr(self, '_schema'):
            self._schema = dict()
          # raise RuntimeError('class %s has no schema defined' % self.__name__)

        self._data = dict()

        self.update(copy.deepcopy(from_dict))


    # --------------------------------------------------------------------------
    #
    def update(self, other):
        '''
        we overload `DictMixin.update()`: the call is used to ensure that
        sub-dicts are instantiated as their respective Munch-inheriting class
        types if so specified by the respective schema.  So, if the schema
        contains:

          {
            ...
            'foo': BarMunch
            ...
          }

        where `BarMunch` is a valid type in the scope of the schema definition
        which inherits from `ru.Munch`, then `update()` will ensure that the
        value for key `foo` is indeed of type `BarMunch`.  An error will be
        raised if (a) `BarMunch` does not have a single parameter constructor
        like `Munch`, or (b) the `data` value for `foo` cannot be used as
        `from_dict` parameter to the `BarMunch` constructor.
        '''

        if not other:
            return

        for k,v in other.items():
            if isinstance(v, dict):
                t = self._schema.get(k)
                if not t:
                    t = type(self)
                if not isinstance(t, dict):
                    if (issubclass(type(v), Munch)):
                        # no need to recast)
                        pass
                    elif issubclass(t, Munch):
                        # cast to expected Munch type
                        v = t(from_dict=v)
            self[k] = v


    # --------------------------------------------------------------------------
    #
    # base functionality for the DictMixin
    #
    def __getitem__(self, k):
        return self._data[k]

    def __setitem__(self, k, v):
        self._data[k] = v

    def __delitem__(self, k):
        del self._data[k]

    def keys(self):
        return self._data.keys()

    def __deepcopy__(self, memo):
        '''
        Note that we do not create the original class type, but return a Munch
        '''
        data   = object.__getattribute__(self, '_data')
        schema = object.__getattribute__(self, '_schema')
        c      = Munch(from_dict={k:v for k, v in data.items()},
                       schema=schema)
        return c


    # --------------------------------------------------------------------------
    #
    # base functionality for attribute access
    # TODO: Make optional
    #
    def __getattr__(self, k):

        if k.startswith('_'):
            return object.__getattribute__(self, k)

        data   = object.__getattribute__(self, '_data')
        schema = object.__getattribute__(self, '_schema')

        if k in schema: return data.get(k)
        else          : return data[k]


    def __setattr__(self, k, v):

        if   k.startswith('_'):
            return object.__setattr__(self, k, v)

        self._data[k] = v


    def __delattr__(self, k):

        if   k.startswith('_'):
            return object.__delattr__(self, k)

        del(self._data[k])


    def __dir__(self):

        return self._data.keys()


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str(self._data)


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        return str(self)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):

        def _demunch(data):
            out = dict()
            for k, v in data.items():
                if isinstance(v, Munch):
                    out[k] = v.as_dict()
                else:
                    out[k] = v
            return out

        return _demunch(self._data)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _verify_int(cls, k, v, t):
        try   : return int(v)
        except: raise TypeError('%s: expected int type for %s (%s)'
                               % (cls.__name__, k, type(v)))

    @classmethod
    def _verify_str(cls, k, v, t):
        try   : return str(v)
        except: raise TypeError('%s: expected str type for %s (%s)'
                               % (cls.__name__, k, type(v)))

    @classmethod
    def _verify_float(cls, k, v, t):
        try   : return float(v)
        except: raise TypeError('%s: expected float type for %s (%s)'
                               % (cls.__name__, k, type(v)))

    @classmethod
    def _verify_bool(cls, k, v, t):
        if v              in [True, False]       : return v
        if str(v).lower() in ['true', 'yes', '1']: return True
        if str(v).lower() in ['false', 'no', '0']: return False
        raise TypeError('%s: expected bool type for %s (%s)'
                       % (cls.__name__, k, type(v)))

    @classmethod
    def _verify_tuple(cls, k, v, t):
        v = as_tuple(v)
        return tuple([cls._verify_kvt(k + ' tuple element', _v, t[0])
                      for _v in v])

    @classmethod
    def _verify_list(cls, k, v, t):
        v = as_list(v)
        return [cls._verify_kvt(k + ' list element', _v, t[0]) for _v in v]

    @classmethod
    def _verify_dict(cls, k, v, t):
        t_k = list(t.keys())[0]
        t_v = list(t.values())[0]
        return {cls._verify_kvt(_k, _k, t_k) :
                cls._verify_kvt(_k, _v, t_v)
                    for _k, _v in v.items()}

    _verifiers = {
            int  : _verify_int.__func__,
            str  : _verify_str.__func__,
            float: _verify_float.__func__,
            bool : _verify_bool.__func__,
    }

    _verifier_keys = list(_verifiers.keys())

    @classmethod
    def _verify_kvt(cls, k, v, t):
        if t is None              : return v
        if t in cls._verifier_keys: return cls._verifiers[t](cls, k, v, t)
        if isinstance(t, tuple)   : return cls._verify_tuple(k, v, t)
        if isinstance(t, list)    : return cls._verify_list(k, v, t)
        if isinstance(t, dict)    : return cls._verify_dict(k, v, t)
        if issubclass(t, Munch)   : return v.verify(t)
        raise TypeError('%s: no verifier defined for type %s'
                       % (cls.__name__, t))


    def verify(self, ctype=None):

        if ctype and not issubclass(type(self), ctype):
            raise TypeError('class type mismatch: %s >< %s'
                    % (type(self), ctype))

        if self._schema:
            for k, v in self._data.items():
                if k.startswith('__'):
                    continue
                if k not in self._schema:
                    raise TypeError('%s: key %s not in schema'
                                   % (type(self).__name__, k))
                self._data[k] = self._verify_kvt(k, v, self._schema[k])
        self._verify()
        return self


    def _verify(self):
        '''
        Can be overloaded
        '''
        pass


# ------------------------------------------------------------------------------

