
__author__    = "Radical.Utils Development Team (Andre Merzky, Matteo Turilli)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import singleton


# ------------------------------------------------------------------------------
#
class Reporter (object) :

    # we want reporter style to be consistent in the scope of an application
    __metaclass__ = singleton.Singleton


    # Define terminal colors for the reporter
    HEADER  = '\033[95m'
    INFO    = '\033[94m'
    OK      = '\033[92m'
    WARN    = '\033[93m'
    ERROR   = '\033[91m'
    ENDC    = '\033[0m'

    DOTTED = '.'
    SINGLE = '-'
    DOUBLE = '='
    HASHED = '#'

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

        if color   : settings['color']   = color 
        if style   : settings['style']   = style
        if segment : settings['segment'] = segment


    # --------------------------------------------------------------------------
    #
    def _out (self, color, msg) :
        sys.stdout.write (color)
        sys.stdout.write (msg)
        sys.stdout.write (self.ENDC)


    # --------------------------------------------------------------------------
    #
    def _format (self, msg, settings) :

        color   = settings['color']
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
    
    r.set_style ('error', style='EELLTLLEEL', segment='X')
    r.error  ('error ')


