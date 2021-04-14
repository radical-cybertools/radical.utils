
# pylint: disable=raise-missing-from

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
from .misc       import is_string
from .misc       import expand_env as ru_expand_env
from .dict_mixin import DictMixin, dict_merge
from .json_io    import write_json


# ------------------------------------------------------------------------------
#
class Munch(DictMixin):

    _self_default = False

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):
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

        if not hasattr(self, '_check'):
            # check type checking on set
            self._check = False

        if not hasattr(self, '_cast'):
            # attempt to cast on type mismatch
            self._cast = True

        if not hasattr(self, '_schema'):
            self._schema = dict()
          # raise RuntimeError('class %s has no schema defined' % self.__name__)

        if hasattr(self, '_schema_extend'):
            self._schema.update(self._schema_extend)

        self._data = dict()

        if hasattr(self, '_defaults'):
            self.update(copy.deepcopy(self._defaults))

        if hasattr(self, '_defaults_extend'):
            self.update(copy.deepcopy(self._defaults_extend))

        self.update(from_dict)


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

        for k, v in other.items():
            if isinstance(v, dict):
                t = self._schema.get(k)
                if not t:
                    if self._self_default: t = type(self)
                    else                 : t = Munch
                if isinstance(t, type) and \
                        issubclass(t, Munch) and not issubclass(type(v), Munch):
                    # cast to expected Munch type
                    self._data.setdefault(k, t()).update(v)
                    continue
            self[k] = v


    # --------------------------------------------------------------------------
    #
    # base functionality for the DictMixin
    #
    def __getitem__(self, k):
        return self._data[k]

    def __setitem__(self, k, v):
        self._data[k] = self._verify_setter(k, v)

    def __delitem__(self, k):
        del self._data[k]

    def keys(self):
        return self._data.keys()

    def __deepcopy__(self, memo):
        # should return a new instance of the same type, not an original Munch,
        # otherwise if an instance of Munch-based has an attribute of another
        # Munch-based type then `verify` method will raise TypeError exception
        return type(self)(from_dict=copy.deepcopy(self._data))


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

        if   not  schema: return data.get(k)
        elif k in schema: return data.get(k)
        else            : return data[k]


    def __setattr__(self, k, v):

        if k.startswith('_'):
            return object.__setattr__(self, k, v)

        self._data[k] = self._verify_setter(k, v)


    def __delattr__(self, k):

        if k.startswith('_'):
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
    @classmethod
    def _demunch_value(cls, v):
        return v.as_dict() if isinstance(v, Munch) else cls.demunch(v)

    @classmethod
    def demunch(cls, src):
        '''
        iterate given object and apply `Munch.as_dict()` to all munch type
        values, and return the result (effectively a shallow copy).
        '''
        if isinstance(src, dict):
            tgt = {k: cls._demunch_value(v) for k, v in src.items()}
        elif isinstance(src, list):
            tgt = [cls._demunch_value(x) for x in src]
        elif isinstance(src, tuple):
            tgt = tuple([cls._demunch_value(x) for x in src])
        else:
            tgt = src
        return tgt

    def as_dict(self):
        return self.demunch(self._data)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _verify_int(cls, k, v, t, cast):
        if v is None or isinstance(v, int): return v
        if cast:
            try   : return int(v)
            except: raise TypeError('%s: expected int type for %s (%s)'
                                   % (cls.__name__, k, type(v)))
        else:
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_str(cls, k, v, t, cast):
        if v is None or isinstance(v, str): return v
        if cast:
            try   : return str(v)
            except: raise TypeError('%s: expected str type for %s (%s)'
                                   % (cls.__name__, k, type(v)))
        else:
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_float(cls, k, v, t, cast):
        if v is None or isinstance(v, float): return v
        if cast:
            try   : return float(v)
            except: raise TypeError('%s: expected float type for %s (%s)'
                                   % (cls.__name__, k, type(v)))
        else:
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_bool(cls, k, v, t, cast):
        if v is None or isinstance(v, bool): return v
        if cast:
            if str(v).lower() in ['true', 'yes', '1']: return True
            if str(v).lower() in ['false', 'no', '0']: return False
            raise TypeError('%s: expected bool type for %s (%s)'
                           % (cls.__name__, k, type(v)))
        else:
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_tuple(cls, k, v, t, cast):
        if v is None: return
        if cast:
            v = as_tuple(v)
            return tuple([cls._verify_kvt(k + ' tuple element', _v, t[0], cast)
                          for _v in v])
        else:
            if isinstance(v, tuple):
                return v
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_list(cls, k, v, t, cast):
        if v is None: return
        if cast:
            v = as_list(v)
            return [cls._verify_kvt(k + ' list element', _v, t[0], cast)
                    for _v in v]
        else:
            if isinstance(v, list):
                return v
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_dict(cls, k, v, t, cast):
        if v is None: return
        if cast:
            t_k = list(t.keys())[0]
            t_v = list(t.values())[0]
            return {cls._verify_kvt(_k, _k, t_k, cast) :
                    cls._verify_kvt(_k, _v, t_v, cast)
                        for _k, _v in v.items()}
        else:
            if isinstance(v, dict):
                return v
            raise TypeError('attribute type error for %s: expected %s, got %s'
                           % (k, t, type(v)))

    @classmethod
    def _verify_munch(cls, k, v, t, cast):
        if v is None: return
        if cast:
            if issubclass(type(v), t): return v.verify()
            if isinstance(v, dict)   : return t(from_dict=v).verify()
            raise TypeError('attribute type error for %s: expected %s, got %s'
                            % (k, t, type(v)))
        else:
            if issubclass(type(v), t):
                return v
            raise TypeError('attribute type error for %s: expected %s, got %s'
                            % (k, t, type(v)))

    _verifiers = {
            int  : _verify_int.__func__,
            str  : _verify_str.__func__,
            float: _verify_float.__func__,
            bool : _verify_bool.__func__,
    }

    _verifier_keys = list(_verifiers.keys())


    @classmethod
    def _verify_kvt(cls, k, v, t, cast):
        if t is None              : return v
        if t in cls._verifier_keys: return cls._verifiers[t](cls, k, v, t, cast)
        if isinstance(t, tuple)   : return cls._verify_tuple(k, v, t, cast)
        if isinstance(t, list)    : return cls._verify_list(k, v, t, cast)
        if isinstance(t, dict)    : return cls._verify_dict(k, v, t, cast)
        if issubclass(t, Munch)   : return cls._verify_munch(k, v, t, cast)
        if cast: return v
        raise TypeError('%s: no verifier defined for type %s'
                       % (cls.__name__, t))

    def verify(self):
        if self._schema:
            for k, v in self._data.items():
                if k.startswith('__'):
                    continue
                if k not in self._schema:
                    raise TypeError('%s: key %s not in schema'
                                   % (type(self).__name__, k))
                self._data[k] = self._verify_kvt(k, v, self._schema[k],
                                                 self._cast)
        self._verify()
        return self


    # --------------------------------------------------------------------------
    #
    def _verify_setter(self, k, v):

        if not self._check:
            # no type checking on set
            return v

        if self._schema:

            if k not in self._schema:
                raise TypeError('%s: key %s not in schema'
                               % (type(self).__name__, k))
            return self._verify_kvt(k, v, self._schema[k], self._cast)


    # --------------------------------------------------------------------------
    #
    def _verify(self):
        '''
        Can be overloaded
        '''
        pass



    # --------------------------------------------------------------------------
    #
    def merge(self, src, expand=True, env=None, policy='overwrite', log=None):
        '''
        merge the given munch into the existing config settings, overwriting
        any values which already existed
        '''

        if expand:
            # NOTE: expansion is done on reference, not copy
            ru_expand_env(src, env=env)

        dict_merge(self, src, policy=policy, log=log)


    # --------------------------------------------------------------------------
    #
    def write(self, fname):

        write_json(self.as_dict(), fname)


    # --------------------------------------------------------------------------
    #
    def query(self, key, default=None):
        '''
        For a query like

            munch.query('some.path.to.key', 'foo')

        this method behaves like:

            munch['some']['path']['to'].get('key', default='foo')
        '''

        if is_string(key): elems = key.split('.')
        else             : elems = key

        if not elems:
            raise ValueError('empty key on query')

        pos  = self
        path = list()
        for elem in elems:

            if not isinstance(pos, dict):
                raise KeyError('no such key [%s]' % '.'.join(path))

            if elem in pos: pos = pos[elem]
            else          : pos = None

            path.append(elem)

        if pos is None:
            pos = default

        return pos


# ------------------------------------------------------------------------------

demunch = Munch.demunch

# ------------------------------------------------------------------------------

