
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import time
import lockable
import singleton

MAX_LEASE_POOL_SIZE = 10
MAX_LEASE_POOL_WAIT = 60 # seconds



# ------------------------------------------------------------------------------
#
@lockable.Lockable
class _LeaseObject (object) :

    # --------------------------------------------------------------------------
    def __init__ (self) :

        self._instance = None


    # --------------------------------------------------------------------------
    @classmethod
    def create (cls, creator) :

        self = cls ()
        self._instance = creator ()

        return self


    # --------------------------------------------------------------------------
    def get (self) :

        return self._instance


    # --------------------------------------------------------------------------
    def __cmp__ (self, other) :

        return (other == self._instance)


# ------------------------------------------------------------------------------
#
@lockable.Lockable
class LeaseManager (object) :

    """ 

    This is a singleton object lease manager -- it creates resource instances on
    demand and hands out leases to them.  If for a given ID no object instance
    exists, one is created, locked and a lease is returned.  If for an ID an
    object instance exists and is not leased, it is locked and returned.  If one
    or more instances exist but all are in use already (leased), a new instance
    is created (up to MAX_LEASE_POOL_SIZE -- can be overwritten in the lease
    call).  If that limit is reached, no objects are returned, and instead the
    lease call blocks until one of the existing objects gets released.

    """
    
    # TODO: we should introduce namespaces -- this is a singleton, but we may want
    # to use it in several places, thus need to make sure to not use colliding
    # names...

    __metaclass__ = singleton.Singleton


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        """
        Make sure the object dict is initialized, exactly once.
        """

        self._pools = dict()


    # --------------------------------------------------------------------------
    #
    def _initialize_pool (self, pool_id, creator, max_pool_size) :
        """
        set up a new pool, but do not create any instances, yet.
        """

        with self :

            if  pool_id in self._pools :
                # nothing to be done
                return

            self._pools[pool_id] = dict()
            self._pools[pool_id]['max']     = max_pool_size
            self._pools[pool_id]['objects'] = list()

            return self._pools[pool_id]


    # --------------------------------------------------------------------------
    #
    def _create_object (self, pool_id, creator) :
        """
        a new instance is needed -- create one, unless max_pool_size is reached.
        If that is the case, return `None`, otherwise return the created object
        (which is locked before return).
        """

        with self :

            if  pool_id not in self._pools :
                raise RuntimeError ("internal error: no pool for '%s'!" % pool_id)

            pool = self._pools[pool_id]

            # check if a poolsize cap is set..
            if  pool['max'] :

                # ... and if it is reached
                if  len(pool['objects'] >= pool['max']) :

                    # no more space...
                    return None

            # poolsize cap not reached -- increase pool 
            obj = _LeaseObject.create (creator)
            obj.lock ()
            pool['objects'].append (obj)

            return obj.get()



    # --------------------------------------------------------------------------
    #
    def lease_obj (self, pool_id, creator, max_pool_size=None) :
        """

        For a given object identified, attempt to retrieve an existing object
        from the pool.  If such a free (released) object is found, lock and
        refturn it.  If such objects exist but all are in use, create a new one
        up to max_pool_size (default: 10).  
        used, block untill it is freed.  If that object does not exist, create
        it and proceed per above.
        return the object thusly created.

        pool_id       : id of the pool to lease from.  The pool ID essentially
                        determines the scope of validity for the managed objects
                        -- they form a namespace, where objects are shared when
                        living under the same name space entry (the pool_id).

        creator       : method to use to create a new object instance

                        Example:
                            def creator () :
                                return getLogger (name)

                            ret   = lease_manager.lease (name, creator)

        max_pool_size : maximal number of objects in the pool.  The
                        max_pool_size specified on the first lease() call for
                        any given pool ID is used for the pool for that pool ID.
                        `None` is used for an unlimited pool size.  Non-positive
                        values are otherwise ognored.  
        """

        pool_id = str(pool_id)

        with self :

            # make sure the pool exists
            pool = self._initialize_pool (pool_id, creator, max_pool_size)

            # find an unlocked object instance in the pool
            for obj in pool['objects'] :

                if  not obj.is_locked () :

                    # found one -- lease/lock and return it
                    obj.lock ()
                    return obj.get ()

            # no unlocked object found -- create a new one 
            obj = self._create_object (pool_id, creator)

            # check if we got an object
            if  obj is not None :

                # we got a locked object -- return
                return obj.get ()


        # pool is full, nothing is free -- we need to wait for an event on
        # the pool to free an instance.
        # Now, this is where deadlocks will happen: any application leasing
        # too many instances in the same thread (with 'too many' meaning
        # more than max_pool_size) will lock that thread here, thus having no
        # chance to release other instances.  We thus will print a log error
        # here, and will raise a timeout exception after MAX_LEASE_POOL_WAIT
        # seconds.
        # Not that we release our lock here, to give other threads the
        # chance to release objects.
        timer_start = time.time()
        timer_now   = time.time()

        while (timer_now - timer_start) < MAX_LEASE_POOL_WAIT :

            # wait for any release activity on the pool
            timer_left = MAX_LEASE_POOL_WAIT - (timer_now - timer_start)
            pool['event'].wait (timer_left)

            with self :

                if  pool['event'].is_set () :

                    # make sure we don't lock up
                    pool['event'].clear ()

                    # we won the race!  now get the freed object
                    obj = pool['freed']
                    pool['freed'] = None

                    if  obj is None :

                        # object was deleted -- we should have space to create
                        # a new one!
                        obj = self._create_object (pool_id, creator)

                        # check if we got an object
                        if  obj is not None :

                            # we got a locked object -- return
                            return obj.get ()

                        else :

                            # find did not find a freed object, did not get to
                            # create a new one -- is there a free one in the
                            # pool?
                            for obj in pool['objects'] :

                                if  not obj.is_locked () :

                                    # found one -- lease/lock and return it
                                    obj.lock ()
                                    return obj.get ()

                            # apparently not - so we give up for now...


                    else :
                        # we got a freed object -- lock and return
                        obj.lock ()
                        return obj.get ()


                # none free, none created - or we lost the rase for handling the 
                # event.  Wait again.
                timer_now = time.time ()


        # at this point we give up: we can't create a new object, can't find
        # a free one, and we are running out of wait time...
        raise LookupError ("stop waiting on object lease")


    # --------------------------------------------------------------------------
    #
    def release_obj (self, instance, delete=False) :
        """
        the given object is not needed right now -- unlock it so that somebody
        else can lease it.  This will not delete the object,
        """

        with self :

            for pool_id in self._pools :

                for obj in self._pools [pool_id]['objects'] :

                    if  instance is not obj :

                        # this is not the object you are looking for.
                        continue


                    # remove the lease lock on the object
                    obj.unlock ()

                    if  delete :

                        # remove the object from the pool (decreasing its 
                        # ref counter and thus making it eligible for 
                        # garbage collection).  

                        self._pools [pool_id]['objects'].remove (obj)
                        self._pools [pool_id]['freed'] = None

                    else :

                        # mark the object as freed for lease.  
                        self._pools [pool_id]['freed'] = obj

                    # notify waiting threads about the lease or creation
                    # opportunity.
                    self._pools [pool_id]['event'].set ()

                    # object has been released
                    return 

            raise RuntimeError ("cannot release object -- not managed")


# ------------------------------------------------------------------------------

