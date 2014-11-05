
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
    #           'off'         : c.Style.RESET_ALL + c.Fore.RESET
    #       }

    COLORS = {'off'          : '\033[39m',
              'default'      : '\033[39m',
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



    # Define terminal colors for the reporter
    HEADER  = 'blue'
    INFO    = 'green'
    OK      = 'lightgreen'
    WARN    = 'magenta'
    ERROR   = 'red'
    ENDC    = 'off'

    DOTTED  = '.'
    SINGLE  = '-'
    DOUBLE  = '='
    HASHED  = '#'

    LINE_LENGTH = 80

    # --------------------------------------------------------------------------
    #
    def __init__ (self, title=None) :

        '''
        settings.style:
          E : empty line
          T : tabulator
          L : line of line segments
          M : text to report
        '''

        self._title    = title
        self._settings = {
                'title' : {
                    'color'   : self.HEADER,
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

        self.title (self._title)


    # --------------------------------------------------------------------------
    #
    def set_style (self, which, color=None, style=None, segment=None) :

        if which not in self._settings :
            raise LookupError ('reporter does not support style "%s"' % which)

        settings = self._settings[which]

        if  color   : 
            if  color.lower() not in self.COLORS :
                raise LookupError ('reporter does not support color "%s"' % color)
            settings['color'] = color

        if style   : settings['style']   = style
        if segment : settings['segment'] = segment


    # --------------------------------------------------------------------------
    #
    def _out (self, color, msg) :
        sys.stdout.write (color)
        sys.stdout.write (msg)
        sys.stdout.write (self.COLORS[self.ENDC])


    # --------------------------------------------------------------------------
    #
    def _format (self, msg, settings) :

        color   = self.COLORS[settings['color'].lower()]
        style   = settings['style']
        segment = settings['segment']

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
    
    r.set_style ('error', color='yellow', style='ELMLE', segment='X')
    r.error  ('error ')


