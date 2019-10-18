#!/usr/bin/env python

import os
import time

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def _cmd(cmd):

    _, _, ret = ru.sh_callout(cmd)

    if ret == 0:
        return True
    else:
      # print 'cmd: %s' % cmd
      # print 'out: %s' % out
      # print 'err: %s' % err
        return False


# ------------------------------------------------------------------------------
#
def test_reporter():

    pname = 'ru.%d'        % os.getpid()
    fname = '/tmp/%s.prof' % pname

    os.environ['RADICAL_UTILS_REPORT']     = 'True'
    os.environ['RADICAL_UTILS_REPORT_TGT'] = fname

    rep = ru.Reporter(name=pname, ns='radical.utils')

    rep.header  ('header  \n')
    rep.info    ('info    \n')
    rep.progress('progress\n')
    rep.ok      ('ok      \n')
    rep.warn    ('warn    \n')
    rep.error   ('error   \n')
    rep.plain   ('plain   \n')


    rep.info('test idler:')
    rep.idle(mode='start')
    for _ in range(3):
        rep.idle()
        time.sleep(0.3)
    rep.idle(color='ok', c='.')
    rep.idle(color='error', c='.')
    for _ in range(3):
        rep.idle()
        time.sleep(0.1)

    rep.idle(mode='stop')
    rep.ok('>>done\n')


    # pylint disable=E0501
    rep.info('idle test\n')
    rep.info('1234567891         2         3         4         5         6         7         8\n\t')  # noqa
    rep.info('.0.........0.........0.........0.........0.........0.........0.........0')              # noqa
    # pylint enable=E0501

    rep.idle(mode='start')
    for _ in range(200):
        rep.idle() ; time.sleep(0.01)
        rep.idle() ; time.sleep(0.01)
        rep.idle() ; time.sleep(0.01)
        rep.idle() ; time.sleep(0.01)
        rep.idle(color='ok', c="+")
    rep.idle(mode='stop')

    rep.set_style('error', color='yellow', style='ELTTTTMELE', segment='X')
    rep.error('error')

    try                  : rep.exit('exit', 1)
    except SystemExit    : assert(True)
    except Exception as e: assert(False), 'expected system exit, got %s' % e

    assert(os.path.isfile(fname))
    assert(_cmd('grep -e "header"    %s' % fname))

    try   : os.unlink(fname)
    except: pass


# ------------------------------------------------------------------------------
#
def test_env():
    '''
    Print out some messages with different log levels
    '''

    def _assert_reporter(pname, fname, val=True):

        rep  = ru.Reporter(name=pname, ns='radical.utils.test', path='/tmp/')
        rep.info('foo')

        if fname:
            assert(val == os.path.isfile(fname))
            assert(val == _cmd('grep -e "foo" %s' % fname))

            try   : os.unlink(fname)
            except: pass



    for key in ['RADICAL_REPORT',
                'RADICAL_UTILS_REPORT',
                'RADICAL_UTILS_TEST_REPORT']:

        for k in list(os.environ.keys()):
            if k.startswith('RADICAL'):
                del(os.environ[k])

        pname = 'ru.%d'        % os.getpid()
        fname = '/tmp/%s.prof' % pname
        _assert_reporter(pname, fname, False)

        os.environ['RADICAL_UTILS_REPORT_TGT'] = fname

        os.environ[key] = ''     ;  _assert_reporter(pname, fname)
        os.environ[key] = '0'    ;  _assert_reporter(pname, None)
        os.environ[key] = '1'    ;  _assert_reporter(pname, fname)
        os.environ[key] = 'True' ;  _assert_reporter(pname, fname)
        os.environ[key] = 'False';  _assert_reporter(pname, fname)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_env()
    test_reporter()


# ------------------------------------------------------------------------------

