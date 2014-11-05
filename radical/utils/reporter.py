
__author__    = "Radical.Utils Development Team (Andre Merzky, Matteo Turilli)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import singleton
import colorama as c


# ------------------------------------------------------------------------------
#
class Reporter (object) :

    # we want reporter style to be consistent in the scope of an application
    __metaclass__ = singleton.Singleton

    COLORS = {'white'       : c.Style.BRIGHT    + c.Fore.WHITE   ,
              'yellow'      : c.Style.BRIGHT    + c.Fore.YELLOW  ,
              'green'       : c.Style.BRIGHT    + c.Fore.GREEN   ,
              'blue'        : c.Style.BRIGHT    + c.Fore.BLUE    ,
              'cyan'        : c.Style.BRIGHT    + c.Fore.CYAN    ,
              'red'         : c.Style.BRIGHT    + c.Fore.RED     ,
              'magenta'     : c.Style.BRIGHT    + c.Fore.MAGENTA ,
              'black'       : c.Style.BRIGHT    + c.Fore.BLACK   ,
              'darkwhite'   : c.Style.DIM       + c.Fore.WHITE   ,
              'darkyellow'  : c.Style.DIM       + c.Fore.YELLOW  ,
              'darkgreen'   : c.Style.DIM       + c.Fore.GREEN   ,
              'darkblue'    : c.Style.DIM       + c.Fore.BLUE    ,
              'darkcyan'    : c.Style.DIM       + c.Fore.CYAN    ,
              'darkred'     : c.Style.DIM       + c.Fore.RED     ,
              'darkmagenta' : c.Style.DIM       + c.Fore.MAGENTA ,
              'darkblack'   : c.Style.DIM       + c.Fore.BLACK   ,
              'off'         : c.Style.RESET_ALL + c.Fore.RESET
          }


    # Define terminal colors for the reporter
    HEADER  = 'darkblue'
    INFO    = 'darkgreen'
    OK      = 'green'
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
          L : line of line segments
          T : text to report
        '''

        self._title    = title
        self._settings = {
                'title' : {
                    'color'   : self.HEADER,
                    'style'   : 'ELTLE',
                    'segment' : self.HASHED
                    },
                'header' : {
                    'color'   : self.HEADER,
                    'style'   : 'EETL',
                    'segment' : self.DOUBLE
                    },
                'info' : {
                    'color'   : self.INFO,
                    'style'   : 'ET',
                    'segment' : self.SINGLE
                    },
                'ok' : {
                    'color'   : self.OK,
                    'style'   : 'T',
                    'segment' : self.DOTTED
                    },
                'warn' : {
                    'color'   : self.WARN,
                    'style'   : 'T',
                    'segment' : self.DOTTED
                    },
                'error' : {
                    'color'   : self.ERROR,
                    'style'   : 'T',
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

            if  c == 'T' :
                self._out (color, "%s\n" % msg)

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
    
    r.set_style ('error', color='yellow', style='ELTLE', segment='X')
    r.error  ('error ')


