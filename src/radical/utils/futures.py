
__author__    = "Radical.Utils Development Team"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import traceback

import threading as mt


_out_lock = mt.RLock()


# ------------------------------------------------------------------------------
#
# our futures have state, the states are defined here
#
# NOTE: these strings are carefully chosen to match the state specifiers of 
#       `radical.saga.Task` and `radical.saga.Job` instances.
#
NEW      = 'New'
RUNNING  = 'Running'
DONE     = 'Done'
FAILED   = 'Failed'
CANCELED = 'Canceled'

INITIAL  = [NEW]
FINAL    = [DONE, FAILED, CANCELED]


# ------------------------------------------------------------------------------
#
class Future(mt.Thread):
    """
    This `Future` class is a thin wrapper around Python's native `mt.Thread`
    class.  It is expected to wrap a callable, and to watch its execution.
    """

    # FIXME: we may want to use a thread pool to limit the number of threads


    # --------------------------------------------------------------------------
    #
    def __init__(self, call, *args, **kwargs):
        '''
        Construct the Future.  The first argument is expected to be a Python
        `callable` which is executed in its own thread.  All other arguments are
        passed blindly to that callable when `self.start()` is called.
        '''

        if not callable(call):
            raise ValueError("Thread requires a callable to function, not %s"
                            % (str(call)))

        mt.Thread.__init__(self)

        # NOTE: we use daemon threads to avoid termination issues
        self.daemon = True

        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._result    = None
        self._exception = None
        self._traceback = None


    # --------------------------------------------------------------------------
    #
    @classmethod
    def Run(self, call, *args, **kwargs):
        """
        This is a shortcut to 

          f = ru.Future(callable); f.start()

        """

        t = self(call, *args, **kwargs)
        t.start()
        return t


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            self._state  = RUNNING
            self._result = self._call(*self._args, **self._kwargs)
            self._state  = DONE

        except Exception as e:
            # NOTE: `state` and `exception` updates are racing.
            tb = traceback.format_exc()
            self._traceback = tb
            self._exception = e
            self._state     = FAILED


    # --------------------------------------------------------------------------
    #
    def wait(self, timeout=None):

        if self.is_alive():
            self.join(timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def cancel(self):

        # FIXME: this is not really implementable generically, so we ignore 
        #        cancel requests for now.  This *should* be handled on the SAGA
        #        layer where cancel requests are forwarded to the callable's
        #        adaptor.
        pass


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):     return self._state 

    @property
    def result(self):    return self._result

    @property
    def exception(self): return self._exception

    @property
    def traceback(self): return self._traceback


# ------------------------------------------------------------------------------

