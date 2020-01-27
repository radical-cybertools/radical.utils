
# ------------------------------------------------------------------------------
#
# We provide a base class for all kinds of description objects in the RCT
# stack: job descriptions, task descriptions, pilot descriptions, workload
# descriptions etc.  That base class provides:
#
#   - dict like API
#   - schema based type definitions
#   - optional runtime type checking
#
# The Description base class provides a property API, similar to the `ru.Config`
# class.
#

from .misc       import as_list, as_tuple
from .dict_mixin import DictMixin


# ------------------------------------------------------------------------------
#
class Munch(DictMixin):

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

              Underscore names are allowed, but SHOULD *only* be used via the
              dictionary API.
        '''

        self._data = dict()

        if from_dict:
            self.update(from_dict)


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
        c = self.__class__(from_dict={k:v for k, v in self.items()})
        object.__setattr__(c, '_schema', self._schema)
        return c


    # --------------------------------------------------------------------------
    #
    # base functionality for attribute access
    # TODO: Make optional
    #
    def __getattr__(self, k):

        # TODO: default values
        if k == '_data'     : return object.__getattribute__(self, k)
        if k in self._schema: return self._data.get(k)
        else                : return self._data[k]

    def __setattr__(self, k, v):
        if k == '_data': return object.__setattr__(self, k, v)
        else           : self._data[k] = v

    def __delattr__(self, k):
        del(self._data[k])

    def __dir__(self):
        return self._data.keys()


  # # --------------------------------------------------------------------------
  # #
  # def __str__(self):
  #     return str(self._data)
  #
  #
  # # --------------------------------------------------------------------------
  # #
  # def __repr__(self):
  #     return str(self)
  #
  #
    # --------------------------------------------------------------------------
    #
    def as_dict(self):

        return self._data


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _verify_int(k, v, t):
        try   : return int(v)
        except: raise TypeError('expected int type for %s (%s)' % (k, type(v)))

    @staticmethod
    def _verify_str(k, v, t):
        try   : return str(v)
        except: raise TypeError('expected str type for %s (%s)' % (k, type(v)))

    @staticmethod
    def _verify_float(k, v, t):
        try   : return float(v)
        except: raise TypeError('expected float type for %s (%s)' % (k, type(v)))

    @staticmethod
    def _verify_bool(k, v, t):
        if v              in [True, False]       : return v
        if str(v).lower() in ['true', 'yes', '1']: return True
        if str(v).lower() in ['false', 'no', '0']: return False
        raise TypeError('expected bool type for %s (%s)' % (k, type(v)))

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
        return {cls._verify_kvt(_k, _k, t_k) : cls._verify_kvt(_k, _v, t_v)
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
        if t in cls._verifier_keys: return cls._verifiers[t](k, v, t)
        if isinstance(t, tuple)   : return cls._verify_tuple(k, v, t)
        if isinstance(t, list)    : return cls._verify_list(k, v, t)
        if isinstance(t, dict)    : return cls._verify_dict(k, v, t)
        raise TypeError('no verifier defined for type %s' % t)

    def verify(self, schema):
        for k, v in self._data.items():
            if k not in schema: raise TypeError('key %s not in schema' % k)
            self._data[k] = self._verify_kvt(k, v, schema[k])
        self._verify()
        return True

    def _verify(self):
        '''
        Can be overloaded
        '''
        pass


# ------------------------------------------------------------------------------
#
class Description(Munch):
    '''
    This is an abstract base class for RCT description types.  Any inheriting
    class MUST provide a `self._schema` class member (not class instance member)
    which is used to verify the description's data validity.  Validation can be
    performed on request (`d.verify()`), or when setting description properties.
    The default is to verify on explicit calls only.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None, verify_setter=False):

        if not hasattr(self, '_schema'):
            raise RuntimeError('class %s has no schema defined' % self.__name__)

        super(Description, self).__init__(from_dict=from_dict)

        if verify_setter:
            raise NotImplemented('setter verification is not yet implemented')

        # TODO: setter verification should be done by attempting to cast the
        #       value to the target type and raising on failure.  Non-trivial
        #       types (lists, dicts) can use `as_list` and friends, or
        #       `isinstance` if that is not available
        #
        # TODO: setter verification should verify that the property is allowed


    # --------------------------------------------------------------------------
    #
    def verify(self):

        return super(Description, self).verify(self._schema)


# ------------------------------------------------------------------------------

