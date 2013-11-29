
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import threading
import singleton


# ------------------------------------------------------------------------------
#
TIMEOUT = 10


# ------------------------------------------------------------------------------
#
class ObjectCache (object) :

    """ 
    This is a singleton object caching class -- it maintains a reference
    counted registry of existing objects.
    """
    
    # TODO: we should introduce namespaces -- this is a singleton, but we may want
    # to use it in several places, thus need to make sure to not use colliding
    # names...

    __metaclass__ = singleton.Singleton
    _lock         = threading.RLock ()

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        """
        Make sure the object cache dict is initialized, exactly once.
        """

        with self._lock :
            self._cache  = {}



    # --------------------------------------------------------------------------
    #
    def get_obj (self, oid, creator) :
        """
        For a given object id, attempt to retrieve an existing object.  If that
        object exists, increase the reference counter, as there is now one more
        user for that object.  
        
        If that object does not exist, call the given creator, then register and
        return the object thusly created.
        """

        with self._lock :

            oid = str(oid)

            if  not oid in self._cache :

                obj = creator ()

                self._cache [oid]        = {}
                self._cache [oid]['cnt'] = 0
                self._cache [oid]['obj'] = obj

            self._cache [oid]['cnt'] += 1

            return self._cache [oid]['obj']


    # --------------------------------------------------------------------------
    #
    def rem_obj (self, obj) :
        """
        For a given objects instance, decrease the refcounter as the caller
        stops using that object.  Once the ref counter is '0', remove all traces
        of the object -- this should make that object eligable for Python's
        garbage collection.  Returns 'True' if the given object was indeed
        registered, 'False' otherwise.

        The removal of the object is actually time-delayed.  That way, we will
        keep the object around *just* a little longer, which provides caching
        semantics in the case of frequent creation/dstruction cycles.
        """

        with self._lock :

            for oid in self._cache.keys () :

                if  obj == self._cache [oid]['obj'] :

                    # delay actual removeal by TIMEOUT seconds)
                    threading.Timer (TIMEOUT, self._delayed_rem_obj, oid)
                    return True

            return False  # obj not found


    # --------------------------------------------------------------------------
    #
    def _delayed_rem_obj (self, oid) :
        """
        actual removal of an object (identified by oid) from the cache -- see
        :func:`rem_obj()` for details.
        """

        with self._lock :

            self._cache [oid]['cnt'] -= 1

            if  self._cache [oid]['cnt'] == 0 :
                self._cache [oid]['obj'] = None  # free the obj reference
                self._cache.pop (oid, None)      # remove the cache entry


# ------------------------------------------------------------------------------


