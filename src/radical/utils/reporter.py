
import os
import sys
import string

# import colorama as c

from .misc    import get_env_ns as ru_get_env_ns
from .config  import DefaultConfig


# ------------------------------------------------------------------------------
#
def _open(target):

    try:
        os.makedirs(os.path.abspath(os.path.dirname(target)))
    except OSError:
        pass  # exists

    return open(target, 'w')


# ------------------------------------------------------------------------------
#
class Reporter(object):

    # COLORS = {'white'       : c.Style.BRIGHT    + c.Fore.WHITE   ,
    #           'yellow'      : c.Style.BRIGHT    + c.Fore.YELLOW  ,
    #           'green'       : c.Style.BRIGHT    + c.Fore.GREEN   ,
    #           'blue'        : c.Style.BRIGHT    + c.Fore.BLUE    ,
    #           'cyan'        : c.Style.BRIGHT    + c.Fore.CYAN    ,
    #           'red'         : c.Style.BRIGHT    + c.Fore.RED     ,
    #           'magenta'     : c.Style.BRIGHT    + c.Fore.MAGENTA ,
    #           'black'       : c.Style.BRIGHT    + c.Fore.BLACK   ,
    #           'darkwhite'   : c.Style.DIM       + c.Fore.WHITE   ,
    #           'darkyellow'  : c.Style.DIM       + c.Fore.YELLOW  ,
    #           'darkgreen'   : c.Style.DIM       + c.Fore.GREEN   ,
    #           'darkblue'    : c.Style.DIM       + c.Fore.BLUE    ,
    #           'darkcyan'    : c.Style.DIM       + c.Fore.CYAN    ,
    #           'darkred'     : c.Style.DIM       + c.Fore.RED     ,
    #           'darkmagenta' : c.Style.DIM       + c.Fore.MAGENTA ,
    #           'darkblack'   : c.Style.DIM       + c.Fore.BLACK   ,
    #           'reset'       : c.Style.RESET_ALL + c.Fore.RESET
    #       }

    COLORS = {'reset'        : '\033[39m',
              'black'        : '\033[30m',
              'red'          : '\033[31m',
              'green'        : '\033[32m',
              'yellow'       : '\033[33m',
              'blue'         : '\033[34m',
              'magenta'      : '\033[35m',
              'cyan'         : '\033[36m',
              'lightgray'    : '\033[37m',
              'darkgray'     : '\033[90m',
              'lightred'     : '\033[91m',
              'lightgreen'   : '\033[92m',
              'lightyellow'  : '\033[93m',
              'lightblue'    : '\033[94m',
              'lightmagenta' : '\033[95m',
              'lightcyan'    : '\033[96m',
              'white'        : '\033[97m'}

    MODS   = {'reset'        : '\033[0m',
              'bold'         : '\033[1m',
              'underline'    : '\033[4m',
              'blink'        : '\033[5m',
              'inverse'      : '\033[7m',
              ''             : ''}


    # define terminal colors and other output options
    TITLE    = 'bold lightblue'
    HEADER   = 'bold lightyellow'
    INFO     = 'lightblue'
    IDLE     = 'lightwhite'
    PROGRESS = 'lightwhite'
    OK       = 'lightgreen'
    WARN     = 'lightyellow'
    ERROR    = 'lightred'

    EMPTY    = ''
    DOTTED   = '.'
    SINGLE   = '-'
    DOUBLE   = '='
    HASHED   = '#'


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, ns=None, path=None, targets=None):
        '''
        settings.style:
          E : empty line
          T : tabulator
          L : line of line segments
          M : text to report
        '''

        ru_def = DefaultConfig()

        self._name = name
        if not ns:
            ns = name

        # check if this profile is enabled via an env variable
        self._enabled = True
        if ru_get_env_ns('report', ns) is None:
            self._enabled = str(ru_def['report']).lower()

        if self._enabled in ['0', 'false', 'off', False, None]:
            self._enabled = False
            # disabled
            return
        else:
            self._enabled = True


        self._use_color = ru_get_env_ns('report_color', ns, default='True')
        if self._use_color.lower() in ['0', 'false', 'off']:
            self._use_color = False
        else:
            self._use_color = True


        self._use_anime = ru_get_env_ns('report_anime', ns, default='True')
        if self._use_anime.lower() in ['0', 'false', 'off']:
            self._use_anime = False
        else:
            self._use_anime = True


        self._line_len = int(ru_get_env_ns('report_llen', ns, default=80))


        if not path:
            path = os.getcwd()

        if not targets:
            targets = ru_get_env_ns('report_tgt', ns)
            if not targets:
                targets = ru_def['report_tgt']

        if isinstance(targets, str):
            targets = targets.split(',')

        if not isinstance(targets, list):
            targets = [targets]

        if '/' in name:
            try:
                os.makedirs(os.path.normpath(os.path.dirname(name)))
            except OSError:
                pass  # dir exists

        self._prog_tgt = None
        self._prog_cnt = 0
        self._prog_pos = 0
        self._prog_off = 0

        C = 'color'
        S = 'style'
        G = 'segment'

        self._pos      = 0
        self._settings = {
            'title'    : {C: self.TITLE    , S: 'ELMLE' , G: self.DOUBLE},
            'header'   : {C: self.HEADER   , S: 'ELME'  , G: self.SINGLE},
            'info'     : {C: self.INFO     , S: 'M'     , G: self.EMPTY },
            'idle'     : {C: self.IDLE     , S: 'M'     , G: self.EMPTY },
            'progress' : {C: self.PROGRESS , S: 'M'     , G: self.EMPTY },
            'ok'       : {C: self.OK       , S: 'M'     , G: self.EMPTY },
            'warn'     : {C: self.WARN     , S: 'M'     , G: self.EMPTY },
            'error'    : {C: self.ERROR    , S: 'M'     , G: self.EMPTY },
            'plain'    : {C: ''            , S: 'M'     , G: self.EMPTY },
        }

        if not self._use_color:
            for k in self._settings:
                self._settings[k]['color'] = ''

        self._idle_sequence = '/-\\|'
        self._idle_pos      = dict()
        self._idle_count    = 0
        self._streams       = list()

        for t in targets:
            if   t in ['0', 'null']       : continue
            elif t in ['-', '1', 'stdout']: h = sys.stdout
            elif t in ['=', '2', 'stderr']: h = sys.stderr
            elif t in ['.']               : h = _open("%s/%s.rep" % (path,name))
            elif t.startswith('/')        : h = _open(t)
            else                          : h = _open("%s/%s"     % (path,t))

            self._streams.append(h)


    # --------------------------------------------------------------------------
    #
    def _out(self, color, msg, count=None):

        if not self._enabled:
            return

        if self._use_color:
            color_mod = ''
            if  ' ' in color:
                color_mod, color = color.split(' ', 2)

            color     = self.COLORS.get(color.lower(), '')
            color_mod = self.MODS.get(color_mod.lower(), '')

            color += color_mod


        # make sure we count tab length on line start correctly
        msg = msg.replace('\n\t', '\n        ')

        # make sure we don't extent a long line further
        if self._pos >= (self._line_len) and msg and msg[0] != '\n':
            while msg[0] == '\b':
                msg = msg[1:]
            if count:
                msg = ' %6d\n        %s' % (count, msg)
            else:
                msg = '\n        %s' % msg

        # special control characters:
        #
        #   * '>>' will, at it's place, insert sufficient spaces to make the
        #     remainder of the string right-aligned.  Only one >> is
        #     interpreted, linebreaks before it are ignored
        #
        #   * '<<' will insert a line break if the position is not already on
        #     the beginning of a line
        #
        slash_f  = msg.find('>>')
        if slash_f >= 0:
            copy   = msg[slash_f + 1:].strip()
            spaces = self._line_len - self._pos - len(copy) + 1  # '>>'
            spaces = max(0, spaces)
            msg    = msg.replace('>>', spaces * ' ')

        slash_cr = msg.find('<<')
        if slash_cr >= 0:
            if self._pos + slash_cr > 0:
                spaces = self._line_len - self._pos - 1
                msg    = msg.replace('<<', '%s\\\n' % (spaces * ' '))
            else:
                msg = msg.replace('<<', '')

        mlen  = len([x for x in msg if x in string.printable])
        mlen -= msg.count('\b')

        # find the last \n and then count how many chars we are writing after it
        slash_n = msg.rfind('\n')

        if slash_n >= 0:
            self._pos = mlen - slash_n - 1
        else:
            self._pos += mlen

        for stream in self._streams:
            if self._use_color:
                stream.write(color)
            stream.write(msg)
            if self._use_color:
                stream.write(self.COLORS['reset'])
                stream.write(self.MODS['reset'])
            try:
                stream.flush()
            except Exception:
                pass


    # --------------------------------------------------------------------------
    #
    def _format(self, msg, settings=None):

        if not self._enabled:
            return

        if not msg:
            msg = ''

        if not settings:
            settings = dict()

        color   = settings.get('color',   '')
        style   = settings.get('style',   'M')
        segment = settings.get('segment', '')

        for c in style:

            if  c == 'M':
                self._out(color, "%s" % msg)

            if  c == 'T':
                self._out(color, "\t")

            elif c == 'E':
                self._out(color, "\n")

            elif c == 'L':
                if segment:
                    self._out(color, "%s\n" % (self._line_len * segment))


    # --------------------------------------------------------------------------
    #
    def set_style(self, which, color=None, style=None, segment=None):

        if not self._enabled:
            return

        if which not in self._settings:
            raise LookupError('reporter does not support style "%s"' % which)

        settings = self._settings[which]

        if color  : settings['color']   = color
        if style  : settings['style']   = style
        if segment: settings['segment'] = segment


    # --------------------------------------------------------------------------
    #
    def title(self, title):

        if not self._enabled:
            return

        if not title:
            title = self._name

        if not title:
            return

        fmt = " %%-%ds\n" % (self._line_len - 1)
        self._format(fmt % title, self._settings['title'])


    # --------------------------------------------------------------------------
    #
    def header(self, msg=''):

        if not self._enabled:
            return

        if msg:
            fmt = "%%-%ds\n" % self._line_len
            msg = fmt % msg

        self._format(msg, self._settings['header'])


    # --------------------------------------------------------------------------
    #
    def info(self, msg=''):

        if not self._enabled:
            return

        self._format(msg, self._settings['info'])


    # --------------------------------------------------------------------------
    #
    def idle(self, c=None, mode=None, color=None, idle_id=None):

        if not self._enabled:
            return

        if not self._use_anime:
            return

        if not idle_id:
            idle_id = 'default'

        if color: col = self._settings[color]['color']
        else    : col = self._settings['idle']['color']

        idx = 0
        if mode == 'start':
            self._out(col, 'O')
            self._idle_count = 0

        elif mode == 'stop':
            self._out(col, '\b %6d' % self._idle_count)

        else:
            if not c:
                idx  = self._idle_pos.get(idle_id, 0)
                c    = self._idle_sequence[idx % len(self._idle_sequence)]
                idx += 1
                self._out(col, '\b%s' % c)
            else:
                idx += 1
                self._idle_count += 1
                self._out(col, '\b%s|' % c, count=self._idle_count)

        self._idle_pos[idle_id] = idx


    # --------------------------------------------------------------------------
    #
    def progress_tgt(self, tgt=None, label=None):

        if not self._enabled:
            return

        if label:
            front = '%-6s: ' % label
        else:
            front = '%-6d: ' % tgt

        self._prog_len = self._line_len - len(front)
        self._prog_tgt = tgt
        self._prog_cnt = 0
        self._prog_pos = 0

        self._format(front, self._settings['progress'])


    # --------------------------------------------------------------------------
    #
    def progress_done(self):
        
        if not self._enabled:
            return

        self._prog_tgt = None
        self._prog_cnt = 0
        self._prog_pos = 0
        self._format('\n', self._settings['progress'])


    # --------------------------------------------------------------------------
    #
    def progress(self, msg=None):

        if not self._enabled:
            return

        if self._prog_tgt:
            self._prog_cnt += 1

            val  = int(self._prog_cnt * self._prog_len / self._prog_tgt)

            while val > self._prog_pos:
                self._prog_pos += 1
                self._format('#', self._settings['progress'])

        else:

            if not msg:
                msg = '.'

            self._format(msg, self._settings['progress'])


    # --------------------------------------------------------------------------
    #
    def ok(self, msg=''):

        if not self._enabled:
            return

        self._format(msg, self._settings['ok'])


    # --------------------------------------------------------------------------
    #
    def warn(self, msg=''):

        if not self._enabled:
            return

        self._format(msg, self._settings['warn'])


    # --------------------------------------------------------------------------
    #
    def error(self, msg=''):

        if not self._enabled:
            return

        self._format(msg, self._settings['error'])


    # --------------------------------------------------------------------------
    #
    def exit(self, msg='', exit_code=0):

        if not self._enabled:
            return

        self.error(msg)
        sys.exit(exit_code)


    # --------------------------------------------------------------------------
    #
    def plain(self, msg=''):

        if not self._enabled:
            return

        self._format(msg)


# ------------------------------------------------------------------------------

