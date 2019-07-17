
__author__    = 'Radical.Utils Development Team (Andre Merzky)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import regex
import collections


# ------------------------------------------------------------------------------
#
# comparison helper: convert non-iterables to list of one element
#
def _cmp_iterable(other):

    if not isinstance(other, collections.abc.Iterable):
        return [other]
    return other


# ------------------------------------------------------------------------------
#
class ReSult(object):
    '''
    This class is a container around a regular expression match, which provides
    some more conventient access methods, boolean tests, etc.

    We only handle base strings, not unicode strings!
    '''

    # -------------------------------------------------------------------------
    #
    def __init__(self, result=None):
        '''
        construct with a `regex.MatchObject` instance.  This ctor should only
        be called from within the `ReString` class.
        '''

        self._glist  = list()
        self._gdict  = dict()
        self._result = result

        if result:
            # fuck python
            if not isinstance(result, type(regex.match('',''))):
                raise TypeError('Need regex.MatchObject, not %s' % type(result))

            self._glist = result.groups()
            self._gdict = result.groupdict()


    # -------------------------------------------------------------------------
    #
    def __str__(self):
        '''
        The string representation is based on the match *list*, as the dict may
        not include all matches...
        '''

        return str(self._glist)


    # -------------------------------------------------------------------------
    #
    def __len__(self):
        '''
        The len representation is based on the match *list*, as the dict may
        not include all matches...
        '''

        return len(self._glist)


    # -------------------------------------------------------------------------
    #
    def get(self, key, default=None):
        '''
        get is supported for default based dict access,
        '''

        if isinstance(key, str):
            return self._gdict.get(key, default)
        else:
            raise TypeError('key %s needs to be integer, not %s'
                          % (key, type(key)))

    # -------------------------------------------------------------------------
    #
    def start(self, idx):

        return self._result.start(idx)


    # -------------------------------------------------------------------------
    #
    def __getitem__(self, idx):
        '''
        getitem is supported for both array type access (using an int index),
        and for dict type access (using a string name).  All other key types
        will cause an exception.
        '''

        if isinstance(idx, str):
            if idx in self._gdict:
                return self._gdict[idx]
        elif isinstance(idx, int):
            if len(self) > idx:
                return self._glist[idx]
        else:
            raise TypeError('index %s needs to be integer or string, not %s'
                           % (idx, type(idx)))
        return None


    # -------------------------------------------------------------------------
    #
    def __iter__(self):
        '''
        the matches can be iterated over
        '''

        for m in self._glist:
            yield m


    # -------------------------------------------------------------------------
    #
    def __getattr__(self, name):
        '''
        Matches can be accessed as properties
        '''

        return self[name]


    # -------------------------------------------------------------------------
    #
    def __bool__(self):
        '''
        Boolean check for 'if / elif / else' constructs
        '''

        if len(self):
            return True
        return False


    # -------------------------------------------------------------------------
    #
    def __enter__(self):
        '''
        support context manager interface for with-statement based constructs
        '''
        return self


    # -------------------------------------------------------------------------
    #
    def __exit__(self, a, b, c):
        '''
        second part of the context manager interface
        '''
        pass


    # -------------------------------------------------------------------------
    #
    # compare to another ReSult or to a tuple.  As they are both iterable, we
    # compare based on the iterable interface
    #
    #
    def __lt__(self, other):

        other = _cmp_iterable(other)
        return self < other


    # -------------------------------------------------------------------------
    #
    def __gt__(self, other):

        other = _cmp_iterable(other)
        return self > other


    # -------------------------------------------------------------------------
    #
    def __le__(self, other):

        other = _cmp_iterable(other)
        return self <= other


    # -------------------------------------------------------------------------
    #
    def __ge__(self, other):

        other = _cmp_iterable(other)
        return self >= other


    # -------------------------------------------------------------------------
    #
    def __ne__(self, other):

        other = _cmp_iterable(other)
        return not self == other


    # -------------------------------------------------------------------------
    #
    def __eq__(self, other):

        other = _cmp_iterable(other)

        if len(self) != len(other):
            return len(self) - len(other)

        for i, m in enumerate(self):
            if m != other[i]:
                print('%s != %s' % (m, other[i]))
                return m == other[i]

        return True


# ------------------------------------------------------------------------------
#
class ReString(str):
    '''
    This is a string class which supports simplified regular expression
    matching. It is not thought that the regex language or expressions are
    simplified, but rather that the invokation of the matching is simple, as is
    the handling of the match results:

        txt = ReString('The quick brown fox jumps over the lazy dog')

        # the '//' operator is overloaded to match against a regular
        # expression. The result is a `ReSult` instance, which allows simple
        # access to the matches
        with txt // r'(\\s.u)(?P<x>.*?j\\S+)' as res:
            if res: print 'Matched!'               # boolean check
            print 'res     : '%%s' ' %% res          # list of results
            print 'res[0]  : '%%s' ' %% res[0]       # index by number ...
            print 'res[1]  : '%%s' ' %% res[1]       # ... for all matches
            print 'res['x']: '%%s' ' %% res['x']     # index by match name
            print 'res.x   : '%%s' ' %% res.x        # ...   as properties
            for i, r in enumerate(res):
                print 'res %%d   : '%%s' ' %% (i, r)  # matches as iterable

            assert(len(res) == 2)                  # number of matches
            assert(res == [' qu', 'ick brown fox jumps'])  # compare to list

        if txt // r'(rabbit)':                      # simple use in if / elif
            res = txt.get()                        # get ReSult of last match

        elif txt // r'((?:\\s).{12,15}?(\\S+))':      # full Python regex slang
            res = txt.get()

        else:
            print 'no match'
    '''

    # -------------------------------------------------------------------------
    #
    def __new__(cls, *args, **kw):

        cls._result = None
        return str.__new__(cls, *args, **kw)


    # -------------------------------------------------------------------------
    #
    def __floordiv__(self, re):

        compiled_regex = None
        if isinstance(re, str):
            compiled_regex = regex.compile(re)

        else:
            # assume we got a compiled regex
            # FIXME: type check
            compiled_regex = re

        if re:
            self._result = ReSult(compiled_regex.search(self))
            return self._result

        return None


    # -------------------------------------------------------------------------
    #
    def get(self, key=None, default=None):

        if self._result and key:

            try:
                return self._result[key]

            except KeyError:
                if default:
                    return default
                raise

        return self._result


# ------------------------------------------------------------------------------
#
def _example_re_string():

    txt = ReString('The quick brown fox jumps over the lazy dog')

    with txt // r'(\s.u)(?P<x>.*?j\S+)' as res:

        if res: print('Matched!')            # boolean check
        print('res     : %s' % res)          # list of results
        print('res[0]  : %s' % res[0])       # index by number ...
        print('res[1]  : %s' % res[1])       # ... for all matches
        print('res["x"]: %s' % res['x'])     # index by match name
        print('res.x   : %s' % res.x)        # ...   as properties
        for i, r in enumerate(res):
            print('res %d   : %s' % (i, r))  # matches as iterable

        assert(len(res) == 2)                # number of matches
        assert(res == [' qu', 'ick brown fox jumps'])  # compare to list


    if txt // '(rabbit)':                    # simple use in if / elif / ...
        res = txt.get()                      # get ReSult of last match

    elif txt // r'((?:\s).{12,15}?(\S+))':   # for full Python regex slang
        res = txt.get()

    else:
        print('no match')


# ------------------------------------------------------------------------------

