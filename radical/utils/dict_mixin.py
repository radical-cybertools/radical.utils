
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


# see http://code.activestate.com/recipes/117236-dictionary-mixin-framework/

# ------------------------------------------------------------------------------
#
class DictMixin :
    '''Mixin defining all dictionary methods for classes that already have
       a minimum dictionary interface including getitem, setitem, delitem,
       and keys '''

    # --------------------------------------------------------------------------
    #
    # first level definitions should be implemented by the sub-class
    #
    def __getitem__(self, key):
        raise NotImplementedError

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError    

    def keys(self):
        raise NotImplementedError

    
    # --------------------------------------------------------------------------
    #
    # second level definitions which assume only getitem and keys
    #
    def has_key(self, key):
         return key in self.keys()

    def __iter__(self):
        for k in self.keys():
            yield k


    # --------------------------------------------------------------------------
    #
    # third level uses second level instead of first
    #
    def __contains__(self, key):
        return self.has_key(key)            

    def iteritems(self):
        for k in self:
            yield (k, self[k])


    # --------------------------------------------------------------------------
    #
    # fourth level uses second and third levels instead of first
    #
    def iterkeys(self):
        return self.__iter__()

    def itervalues(self):
        for _, v in self.iteritems():
            yield v

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def clear(self):
        for key in self.keys():
            del self[key]

    def setdefault(self, key, default):
        if key not in self:
            self[key] = default
            return default
        return self[key]

    def popitem(self):
        key = self.keys()[0]
        value = self[key]
        del self[key]
        return (key, value)

    def update(self, other):
        for key in other.keys():
            self[key] = other[key]

    def get(self, key, default):
        if key in self:
            return self[key]
        return default

    def __repr__(self):
        return repr(dict(self.items()))


# ------------------------------------------------------------------------------
#
def dict_merge (a, b, merge_policy=None, _path=[]):
    # thanks to 
    # http://stackoverflow.com/questions/7204805/python-dictionaries-of-dictionaries-merge
    """
    This merges two dict in place, modifying the original dict in a.

    Merge Policies :
        None (default) : raise an exception on conflicts
        preserve       : original value in a are preserved, new values 
                         from b are only added where the original value 
                         is None / 0 / ''
        overwrite      : values in a are overwritten by new values from b

    """

    for key in b:
        
        if  key in a:

            # need to resolve conflict
            if  isinstance (a[key], dict) and isinstance (b[key], dict):
                dict_merge (a[key], b[key], 
                            merge_policy = merge_policy, 
                            _path        = _path + [str(key)])
            
            elif a[key] == b[key]:
                pass # same leaf value

            elif  not a[key] and b[key] :
                a[key] = b[key] # use b value

            elif  not b[key] and a[key] :
                pass # keep a value

            elif  not b[key] and not a[key] :
                pass # keep no a value

            else:
                if  merge_policy == 'preserve' :
                    pass # keep original value

                elif merge_policy == 'overwrite' :
                    a[key] = b[key] # use new value

                else :
                    raise ValueError ('Conflict at %s (%s : %s)' \
                                   % ('.'.join(_path + [str(key)]), a[key], b[key]))
        
        else:
            # no conflict - simply add.  Not that this is a potential shallow
            # copy if b[key] is a complex type.
            a[key] = b[key]
    
    return a

# ------------------------------------------------------------------------------


