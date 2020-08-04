
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'

import re
import fnmatch


PRESERVE  = 'preserve'
OVERWRITE = 'overwrite'


# see http://code.activestate.com/recipes/117236-dictionary-mixin-framework/

# ------------------------------------------------------------------------------
#
class DictMixin:
    '''
    Mixin defining all dictionary methods for classes that already have
    a minimum dictionary interface including getitem, setitem, delitem,
    and keys.  Based on those methods, the mixin provides the remaining
    interface functionality to make the class look like a fully compliant
    dictionary.
    '''

    # --------------------------------------------------------------------------
    #
    # mascerade as dict for `isinstance` calls
    #
    @property
    def __class__(self):
        return dict


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
        return key in list(self.keys())

    def __iter__(self):
        for k in list(self.keys()):
            yield k


    # --------------------------------------------------------------------------
    #
    # third level uses second level instead of first
    #
    def __contains__(self, key):
        return self.has_key(key)                                    # noqa: W601

    def items(self):
        for k in self:
            yield(k, self[k])


    # --------------------------------------------------------------------------
    #
    # fourth level uses second and third levels instead of first
    #
    def iterkeys(self):
        return self.__iter__()

    def itervalues(self):
        for _, v in self.items():
            yield v

    def values(self):
        return list(self.values())

    def clear(self):
        for key in list(self.keys()):
            del self[key]

    def setdefault(self, key, default):
        if key not in self:
            self[key] = default
            return default
        return self[key]

    def popitem(self):
        key = list(self.keys())[0]
        value = self[key]
        del self[key]
        return key, value

    def update(self, other):
        for key in list(other.keys()):
            self[key] = other[key]

    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def __repr__(self):
        return repr(dict(list(self.items())))

    def __len__(self):
        return len(self.keys())


# ------------------------------------------------------------------------------
#
def dict_merge(a, b, policy=None, wildcards=False, log=None, _path=None):
    # thanks to
    # http://stackoverflow.com/questions/7204805/ \
    #                          python-dictionaries-of-dictionaries-merge
    '''
    This merges two dict in place, modifying the original dict in a.

    Merge Policies:
        None (default) : raise an exception on conflicts
        PRESERVE       : original value in a are preserved, new values
                         from b are only added where the original value
                         is not set.
        OVERWRITE      : values in a are overwritten by new values from b

    '''

    if  a    is None: return a
    if  b    is None: return a
    if _path is None: _path = list()

    if  not isinstance(a, dict):
        raise TypeError('*dict*_merge expects dicts, not %s' % type(a))

    if  not isinstance(b, dict):
        raise TypeError('*dict*_merge expects dicts, not %s' % type(b))

    # --------------------------------------------------------------------------
    def merge_key(a, key_a, b, key_b):

        # need to resolve conflict
        if  isinstance(a[key_a], dict) and isinstance(b[key_b], dict):
            dict_merge(a[key_a], b[key_b],
                       policy    = policy,
                       wildcards = wildcards,
                       log       = log,
                       _path     = _path + [str(key_a)])


        elif key_a not in a and key_b in b:
            a[key_a] = b[key_b]  # use b value

        elif key_a in a and key_b not in b:
            pass  # keep a value

        elif a[key_a] == b[key_b]:
            pass  # same leaf value

        elif key_a not in a and key_b not in b:
            pass  # keep no a value

        else:
            if  policy == PRESERVE:
                if  log:
                    log.debug('preserving key %s:%s \t(%s)'
                              % (':'.join(_path), key_b, b[key_b]))

            elif policy == OVERWRITE:
                if  log:
                    log.debug('overwriting key %s:%s \t(%s)'
                              % (':'.join(_path), key_b, b[key_b]))
                a[key_a] = b[key_b]  # use new value
            else:
                raise ValueError('Conflict at %s (%s : %s)'
                              % ('.'.join(_path + [str(key_a)]),
                                 a[key_a], b[key_b]))
    # --------------------------------------------------------------------------

    # first a clean merge, i.e. no interpretation of wildcards
    for key in sorted(b.keys()):

        if  key in a:

            # need to resolve conflict
            merge_key(a, key, b, key)

        else:
            # no conflict - simply add.  Not that this is a potential shallow
            # copy if b[key] is a complex type.
            a[key] = b[key]


    # optionally, check if other merge options are also valid
    for key_b in sorted(b.keys()):
        if  wildcards:
            if  '*' in key_b:
                pat = re.compile(fnmatch.translate(key_b))
                for key_a in sorted(a.keys()):
                    if  pat.match(key_a):
                        merge_key(a, key_a, b, key_b)

    return a


# ------------------------------------------------------------------------------
#
def dict_stringexpand(target, sources=None):
    '''
    This expands dict entries (strings only) with keys from a second dict. For
    example, the dicts::

        target  = {'workdir'  : '/home/%(user)s/',
                   'resource' : '%(resource)s'}
        sources = {'user'     : 'peer_gynt',
                   'protocol' : 'ssh',
                   'host'     : 'localhost',
                   'resource' : '%(protocol)s://%(host)s/'}

    would result in::
        target = {'workdir'  : '/home/peer_gynt/',
                  'resource' : 'ssh://localhost'}

    Note that expansion happened twice, for the `resource` tag to be fully
    specified.
    '''

    assert(isinstance(target, dict))

    # expand from self, and all given dicts, but only use
    # first-level primitive types (string, int, float)
    if  sources:
        if  isinstance(sources, dict):
            sources = [sources]
    else:
        sources = list()

    if  not isinstance(sources, list):
        raise TypeError('Need dict as expansion source, not %s' % type(sources))

    # target must be first source, to avoid cycles (other sources are likely to
    # have *other* info)
    sources.insert(0, target)

    repl_source = dict()
    for source in sources:
        for key, val in source.items():
            if  isinstance(val, str) or \
                isinstance(val, int       ) or \
                isinstance(val, float     ) :
                repl_source[key] = val

    again = True
    while again:
        target, again = _generic_stringexpand(target, repl_source)

    return target


# ------------------------------------------------------------------------------
#
def _generic_stringexpand(target, source):

    if  isinstance(target, str):
        return _string_stringexpand(target, source)

    elif  isinstance(target, list):
        return _list_stringexpand(target, source)

    elif  isinstance(target, dict):
        return _dict_stringexpand(target, source)

    else:
        # ignore other types for now
        return target, False


# ------------------------------------------------------------------------------
#
def _list_stringexpand(target, source):

    assert(isinstance(target, list))
    assert(isinstance(source, dict))

    all_again = 0
    for idx, elem in enumerate(target):
        target[idx], again = _generic_stringexpand(elem, source)
        all_again += again

    return target, all_again


# ------------------------------------------------------------------------------
#
def _dict_stringexpand(target, source):

    assert (isinstance(target, dict))
    assert (isinstance(source, dict))

    all_again = 0
    for key, val in target.items():
        target[key], again = _generic_stringexpand(val, source)
        all_again += again

    return target, all_again


# ------------------------------------------------------------------------------
#
def _string_stringexpand(target, source):

    assert (isinstance(target, str))
    assert (isinstance(source, dict))

    orig = str(target)
    try:
        expanded = target % source

    except KeyError:
        # ignore incomplete expands
        return orig, False

    except ValueError:
        # ignore incomplete expands
        return orig, False

    # only check for success after success.  Duh!
    if  orig == expanded: return expanded, False
    else                : return expanded, True


# ------------------------------------------------------------------------------
#
def iter_diff(a, b):

    if isinstance(a, list) and isinstance(b, list):
        for ea, eb in zip(a, b):
            iter_diff(ea, eb)
    elif isinstance(a, dict) and isinstance(b, dict):
        assert(len(list(a.keys())) == len(list(b.keys()))), \
               (len(a), sorted(list(a.keys())), len(b), sorted(list(b.keys())))
        for k in a:
            iter_diff(a[k], b[k])
    else:
        if a != b:
            print('elem diff: %s' % a)
            print('elem diff: %s' % b)
            print()
        assert(a == b), [a, b]


# ------------------------------------------------------------------------------
#
def dict_diff(a, b):
    '''
    return a dict of the form:

        {
           'k1': {'a': 'foo',
                  'b': 'bar'},
           'k2': {'a': 'foo'},
           'k3': {'b': 'bar'},
        }
    which contains only those keys which are different in the two given dicts.
    Keys which are missing in either one are not included (to distinguish from
    `None` values).  This methods operates recursively over the given dicts.
    '''

    def _list_diff(a, b):

        if len(a) != len(b):
            la, lb = sorted([len(a), len(b)])
            ret = ['len(%d) != len(%d)' % (la, lb)]
        else:
            ret = list()
            for va, vb in zip(a, b):
                if isinstance(va, dict) and isinstance(vb, dict):
                    tmp = _dict_diff(va, vb)
                    if tmp:
                        ret.append(tmp)
                elif isinstance(va, list) and isinstance(vb, list):
                    tmp = _list_diff(va, vb)
                    if tmp:
                        ret.append(tmp)
                else:
                    if va != vb:
                        ret.append({'a': va,
                                    'b': vb})

        return ret


    def _dict_diff(a, b):

        ka  = sorted(list(a.keys()))
        kb  = sorted(list(b.keys()))
        ret = dict()

        for k in ka:
            if k not in kb:
                ret[k] = {'a': a[k]}
            else:
                va = a[k]
                vb = b[k]
                if isinstance(va, dict) and isinstance(vb, dict):
                    tmp = _dict_diff(va, vb)
                    if tmp:
                        ret[k] = tmp
                elif isinstance(va, list) and isinstance(vb, list):
                    tmp = _list_diff(va, vb)
                    if tmp:
                        ret[k] = tmp
                elif va != vb:
                    ret[k] = {'a': va,
                              'b': vb}

        for k in kb:
            if k not in ka:
                ret[k] = {'b': b[k]}
            else:
                va = a[k]
                vb = b[k]
                if isinstance(vb, dict) and isinstance(va, dict):
                    tmp = _dict_diff(vb, va)
                    if tmp:
                        ret[k] = tmp
                elif isinstance(va, list) and isinstance(vb, list):
                    tmp = _list_diff(va, vb)
                    if tmp:
                        ret[k] = tmp
                elif va != vb:
                    ret[k] = {'a': va,
                              'b': vb}

        return ret

    return _dict_diff(a, b)


# ------------------------------------------------------------------------------

