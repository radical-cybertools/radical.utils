
__author__    = "Radical.Utils Development Team (Andre Merzky, Matteo Turilli)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import singleton
# import colorama as c


# ------------------------------------------------------------------------------
#
class Reporter (object) :

    # we want reporter style to be consistent in the scope of an application
    __metaclass__ = singleton.Singleton

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

    COLORS     = {'reset'        : '\033[39m',
                  'black'        : '\033[30m',
                  'red'          : '\033[31m',
                  'green'        : '\033[32m',
                  'yellow'       : '\033[33m',
                  'blue'         : '\033[34m',
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

    COLOR_MODS = {'reset'        : '\033[0m',
                  'bold'         : '\033[1m',
                  'underline'    : '\033[4m',
                  'blink'        : '\033[5m',
                  'inverse'      : '\033[7m', 
                  ''             : ''}


    # Define terminal colors for the reporter
    TITLE   = 'underline blue'
    HEADER  = 'blue'
    INFO    = 'bold green'
    OK      = 'green'
    WARN    = 'magenta'
    ERROR   = 'inverse red'

    DOTTED  = '.'
    SINGLE  = '-'
    DOUBLE  = '='
    HASHED  = '#'

    LINE_LENGTH = 80

    # --------------------------------------------------------------------------
    #
    def __init__ (self, title=None, targets=['stdout']) :

        '''
        settings.style:
          E : empty line
          T : tabulator
          L : line of line segments
          M : text to report
        '''

        self._title    = title
        self._targets  = targets
        self._settings = {
                'title' : {
                    'color'   : self.TITLE,
                    'style'   : 'ELMLE',
                    'segment' : self.HASHED
                    },
                'header' : {
                    'color'   : self.HEADER,
                    'style'   : 'EEML',
                    'segment' : self.DOUBLE
                    },
                'info' : {
                    'color'   : self.INFO,
                    'style'   : 'EM',
                    'segment' : self.SINGLE
                    },
                'ok' : {
                    'color'   : self.OK,
                    'style'   : 'M',
                    'segment' : self.DOTTED
                    },
                'warn' : {
                    'color'   : self.WARN,
                    'style'   : 'M',
                    'segment' : self.DOTTED
                    },
                'error' : {
                    'color'   : self.ERROR,
                    'style'   : 'M',
                    'segment' : self.DOTTED
                    }
                }

        # set up the output target streams
        self._color_streams = list()
        self._streams       = list()

        for tgt in self._targets :

            if  tgt.lower() == 'stdout' :
                self._color_streams.append (sys.stdout)

            elif tgt.lower() == 'stderr' :
                self._color_streams.append (sys.stderr)

            else :
                # >>&     color stream in append    mode
                # >&      color stream in overwrite mode (default)
                # >>  non-color stream in append    mode
                # >   non-color stream in overwrite mode

                if   tgt.startswith ('>>&') : self._color_streams.append (open (tgt[3:], 'a'))
                elif tgt.startswith ('>&')  : self._color_streams.append (open (tgt[2:], 'w'))
                elif tgt.startswith ('>>')  : self._streams.append       (open (tgt[2:], 'a'))
                elif tgt.startswith ('>')   : self._streams.append       (open (tgt[1:], 'w'))
                else                        : self._color_streams.append (open (tgt,     'w'))

        # and send the title to all streams
        self.title (self._title)



    # --------------------------------------------------------------------------
    #
    def _out (self, color, msg) :

        for stream in self._color_streams :
            stream.write (color)
            stream.write (msg)
            stream.write (self.COLORS['reset'])
            stream.write (self.COLOR_MODS['reset'])

        for stream in self._streams :
            stream.write (msg)


    # --------------------------------------------------------------------------
    #
    def _format (self, msg, settings) :

        color   = settings.get ('color',     '')
        style   = settings.get ('style',     '')
        segment = settings.get ('segment',   '')

        color_mod = ''
        if  ' ' in color :
            color_mod, color = color.split (' ', 2)

        if  color.lower() not in self.COLORS :
            raise LookupError ('reporter does not support color "%s"' % color)

        color     = self.COLORS[color.lower()]
        color_mod = self.COLOR_MODS[color_mod.lower()]

        color  += color_mod

        for c in style :

            if  c == 'M' :
                self._out (color, "%s\n" % msg)

            if  c == 'T' :
                self._out (color, "\t")

            elif c == 'E' :
                self._out (color, "\n")

            elif c == 'L' :
                self._out (color, "%s\n" % (self.LINE_LENGTH * segment))
    

    # --------------------------------------------------------------------------
    #
    def set_style (self, which, color=None, style=None, segment=None) :

        if which not in self._settings :
            raise LookupError ('reporter does not support style "%s"' % which)

        settings = self._settings[which]

        if color   : settings['color']   = color 
        if style   : settings['style']   = style
        if segment : settings['segment'] = segment


    # --------------------------------------------------------------------------
    #
    def title (self, title=None) :

        if not title :
            title = self._title

        self._format (title, self._settings['title'])

    
    # --------------------------------------------------------------------------
    #
    def header (self, msg) :

        self._format (msg, self._settings['header'])


    # --------------------------------------------------------------------------
    #
    def info (self, msg) :

        self._format (msg, self._settings['info'])


    # --------------------------------------------------------------------------
    #
    def ok (self, msg) :

        self._format (msg, self._settings['ok'])


    # --------------------------------------------------------------------------
    #
    def warn (self, msg) :

        self._format (msg, self._settings['warn'])


    # --------------------------------------------------------------------------
    #
    def error (self, msg) :
        
        self._format (msg, self._settings['error'])


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    import radical.utils as ru
    
    
    r = ru.Reporter (title='test')
    
    r.header ('header')
    r.info   ('info  ')
    r.ok     ('ok    ')
    r.warn   ('warn  ')
    r.error  ('error ')
    
    r.set_style ('error', color='yellow', style='ELTTMLE', segment='X')
    r.error  ('error ')


