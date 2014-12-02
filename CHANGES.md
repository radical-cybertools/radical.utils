

Version 0.7.8 release                                      2014-10-29
---------------------------------------------------------------------

* For a list of bug fixes, see 
  https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
* For a list of open issues and known problems, see
  https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aopen+
  
* implemented lease manager (manage finite set of resources with 
  finite lifetime over multiple threads)
* implemented DebugHelper class (prints stack traces for all threads 
  on SIGUSR1
* implement decorator for class method timings
* cache configuration settings on logger creation, which significantly 
  speeds up logging over different log objects
* remove deepcopy from configuration management (improves performance)
* add wildcard expanstion on  dict_merge
* make pymongo and nose dependencies optional


Version 0.7.7 release                                      2014-08-27
---------------------------------------------------------------------

* lease manager which handles resource leases (like, leases ssh connections to saga adaptors)
* fixes on deepcopy, logging and config handling 


Version 0.7.5 release                                      2014-07-22
---------------------------------------------------------------------

* Some small bug fixes.

