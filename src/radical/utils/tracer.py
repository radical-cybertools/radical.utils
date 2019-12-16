
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys
import linecache

_trace_external  = True
_trace_namespace = 'radical'
_trace_logger    = None


# ------------------------------------------------------------------------------
#
'''
  kudos:
  http://www.dalkescientific.com/writings/diary/archive/2005/04/20/\
                                                        tracing_python_code.html


  This module will trace all python function calls, printing each line as it is
  being executed.  It will not print traces for system libraries (i.e. modules
  which are not in the configured namespace), but will indicate when the code
  descents to the system level.

  Python system traces are not following Python's `exec()` call (and
  derivatives), so the resulting trace may be incomplete.   The tracer
  may also fail to step through loaded plugins or adaptors.

  Use like this::

      def my_call(url):

          ru.tracer.trace('radical')

          u = radical.Url(url_str)
          print str(u)

          ru.tracer.untrace()

'''

_trace_namespace = 'radical'


# ------------------------------------------------------------------------------
#
def _tracer(frame, event, _):

    global _trace_external                               # pylint: disable=W0603
    global _trace_logger                                 # pylint: disable=W0603

  # if  event == "call":
    if  event == "line":

        filename = frame.f_globals["__file__"]
        lineno   = frame.f_lineno

        if filename.endswith(".pyc") or \
           filename.endswith(".pyo"):
            filename = filename[:-1]

        line = linecache.getline(filename, lineno)

        if _trace_namespace in filename:

            idx  = filename.rindex(_trace_namespace)
            name = filename[idx:]
            if _trace_logger:
                _trace_logger.debug('[trace]: %s %4d %s', name, lineno, line.rstrip())
            else:
                print("%-60s:%4d: %s" % (name, lineno, line.rstrip()))
            _trace_external = False

        else:

            if not _trace_external:
                name = '/'.join(filename.split('/')[-3:])
                if _trace_logger:
                    _trace_logger.debug('[trace]: %s %4d %s', name, lineno, line.rstrip())
                else:
                    print("--> %-56s:%4d: %s" % (name, lineno, line.rstrip()))
            _trace_external = True

    return _tracer


# ------------------------------------------------------------------------------
def trace(namespace='radical', log=None):

    global _trace_namespace                              # pylint: disable=W0603
    global _trace_logger                                 # pylint: disable=W0603

    _trace_namespace = namespace
    _trace_logger    = log

    sys.settrace(_tracer)


# ------------------------------------------------------------------------------
def untrace():

    sys.settrace(None)


# ------------------------------------------------------------------------------

