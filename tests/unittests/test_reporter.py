#!/usr/bin/env python

import sys

import radical.utils as ru


# ------------------------------------------------------------------------------

def test_reporter():

    r = ru.Reporter(title='test')

    r.header  ('header  \n')
    r.info    ('info    \n')
    r.progress('progress\n')
    r.ok      ('ok      \n')
    r.warn    ('warn    \n')
    r.error   ('error   \n')
    r.plain   ('plain   \n')

    i = 0
    j = 0
    for cname,col in r.COLORS.items():
        if cname == 'reset':
            continue
        i += 1
        for mname,mod in r.COLOR_MODS.items():
            if mname == 'reset':
                continue
            j += 1
            sys.stdout.write("%s%s[%12s-%12s] " % (col, mod, cname, mname))
            sys.stdout.write("%s%s" % (r.COLORS['reset'], r.COLOR_MODS['reset']))
        sys.stdout.write("\n")
        j = 0

    import time

    r.info('test idler:')
    r.idle(mode='start')
    for i in range(3):
        r.idle()
        time.sleep(0.3)
    r.idle(color='ok', c='.')
    r.idle(color='error', c='.')
    for i in range(3):
        r.idle()
        time.sleep(0.1)

    r.idle(mode='stop')
    r.ok('>>done\n')

    r.info('idle test\n')
    r.info('1234567891         2         3         4         5         6         7         8\n\t')
    r.info('.0.........0.........0.........0.........0.........0.........0.........0')
    r.idle(mode='start')
    for i in range(200):
        r.idle(); time.sleep(0.01)
        r.idle(); time.sleep(0.01)
        r.idle(); time.sleep(0.01)
        r.idle(); time.sleep(0.01)
        r.idle(color='ok', c="+")
    r.idle(mode='stop')

    r.set_style('error', color='yellow', style='ELTTTTMELE', segment='X')
    r.error('error')
    r.exit('exit', 1)

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    test_reporter()


# ------------------------------------------------------------------------------

