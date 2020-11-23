#!/usr/bin/env python

__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import copy
import time

import radical.utils as ru

# create a virgin env
old_env = copy.deepcopy(os.environ)
for _key in list(os.environ.keys()):
    if _key.startswith('RADICAL_'):
        del(os.environ[_key])


# ------------------------------------------------------------------------------
#
def _cmd(cmd):

    _, _, ret = ru.sh_callout(cmd)

    if ret == 0: return True
    else       : return False


# ------------------------------------------------------------------------------
#
def test_profiler():
    '''
    create and check profile timestamps
    '''

    pname = 'ru.%d'        % os.getpid()
    fname = '/tmp/%s.prof' % pname
    now   = time.time()

    try:
        os.environ['RADICAL_PROFILE'] = 'True'
        prof = ru.Profiler(name=pname, ns='radical.utils', path='/tmp/')

        prof.prof('foo')
        prof.prof('bar', uid='baz')
        prof.prof('buz', ts=now)
        prof.flush()

        assert(os.path.isfile(fname))

        def _grep(pat):
            return _cmd('grep -e "%s" %s' % (pat, fname))

        assert(_grep('^[0-9\\.]*,foo,%s,MainThread,,,$'    %       pname ))
        assert(_grep('^[0-9\\.]*,bar,%s,MainThread,baz,,$' %       pname ))
        assert(_grep('^%.7f,buz,%s,MainThread,,,$'         % (now, pname)))

    finally:
        try   : del(os.environ['RADICAL_PROFILE'])
        except: pass
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_enable():

    pname = 'ru.%d'        % os.getpid()
    fname = '/tmp/%s.prof' % pname

    try:
        os.environ['RADICAL_PROFILE'] = 'True'
        prof = ru.Profiler(name=pname, ns='radical.utils', path='/tmp/')

        prof.prof('foo')
        prof.disable()
        prof.prof('bar')
        prof.enable()
        prof.prof('buz')
        prof.flush()

        assert(os.path.isfile(fname))

        def _grep(pat):
            return _cmd('grep -e "%s" %s' % (pat, fname))

        assert     _grep('foo')
        assert not _grep('bar')
        assert     _grep('buz')

    finally:
        try   : del(os.environ['RADICAL_PROFILE'])
        except: pass
        try   : os.unlink(fname)
        except: pass


# ------------------------------------------------------------------------------
#
def test_env():
    '''
    Print out some messages with different log levels
    '''

    # --------------------------------------------------------------------------
    #
    def _assert_profiler(key, val, res):

        try:
            os.environ[key] = val

            pname = 'ru.%d'        % os.getpid()
            fname = '/tmp/%s.prof' % pname
            prof  = ru.Profiler(name=pname, ns='radical.utils.test',
                                path='/tmp/')
            prof.prof('foo')
            prof.flush()

            assert(res == os.path.isfile(fname))
            assert(res == _cmd('grep -e "^[0-9\\.]*,foo,%s," %s'
                              % (pname, fname)))

        finally:
            try   : del(os.environ[key])
            except: pass
            try   : os.unlink(fname)
            except: pass


    # --------------------------------------------------------------------------
    #
    for key in ['RADICAL_PROFILE',
                'RADICAL_UTILS_PROFILE',
                'RADICAL_UTILS_TEST_PROFILE']:

        for k in list(os.environ.keys()):
            if k.startswith('RADICAL'):
                del(os.environ[k])

        _assert_profiler('', '', True)

        for val, res in [
                         ['false', False],
                         ['',      True ],
                         ['1',     True ],
                         ['true',  True ],
                         ['True',  True ],
                         ['TRUE',  True ],
                         ['false', False],
                         ['False', False],
                         ['FALSE', False],
                         ['0',     False]]:

            for k in list(os.environ.keys()):
                if k.startswith('RADICAL'):
                    del(os.environ[k])

            os.environ[key] = val
            _assert_profiler(key, val, res)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_profiler()
    test_env()
    test_enable()


# ------------------------------------------------------------------------------

