
  - For a list of bug fixes, see
    https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
  - For a list of open issues and known problems, see
    https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aopen+


Version 0.41.1 release                                                2016-06-02
--------------------------------------------------------------------------------

  - add fs barrier to debug helper 
  - add get_traceback 
  - clean up counter management 
  - fix a log handler 
  - fix path creation for logfile output 
  - Fix regression on query split. 
  - fix thread inspection problem 
  - include atfork in RU 
  - keep doc in sync with implementation 
  - log version info is now optional 
  - make sig checking conditional 
  - make sure log level is string before calling "upper()" 
  - fix split_module
  - monkeypatch python's logging module 


Version 0.40 release                                                  2016-02-03
--------------------------------------------------------------------------------

  - add an algorithm to create balanced partitions of a space 
  - add ssl support to mongodb_connect 
  - allow fragment parsing in Url class
  - allow reporter and debug log to coexist if log target is set. 
  - fix default for get in dict mixin 
  - fix type conversion for int, float configurables from env vars 
  - implement reset_id_counters 


Version 0.38 release                                                  2015-11-06
--------------------------------------------------------------------------------

  - support install on anaconda


Version 0.37 release                                                  2015-10-15
--------------------------------------------------------------------------------

  - fix reporter log level
  - several changes to reporter class
  - align reporter, logger, and logreporter


Version 0.36 release                                                  2015-10-08
--------------------------------------------------------------------------------

  - clean up logging (getLogger -> get_logger)
  - log pid and tid on log creation 
  - fix type conversion on some config data types 
  - make uid generation independent if getpwuid
  - fix log message for warning about log level 
  - add boolean test for url 
  - add SAGA_VERBOSE backport 
  - fix some more import shadowing 
  - fix layer violation: make logs look nice in RP, finally
  - do not use signals in threads... 
  - rename and install version tool. 
  - fix the debug helper 


Version 0.35 release                                                  2015-08-27
--------------------------------------------------------------------------------

  - small bugfix release


Version 0.29 release                                                  2015-07-13
--------------------------------------------------------------------------------

  - attempt to avoid sdist naming error during pypi installation
  - add uuid support, avoid counter reset on reboot
  - add two convenience methods which support range finding
  - have multiple threads lease from same pool.  Speed up LM test
  - make sure that the pull is filled during lease manager testing
  - make sure json is exported as encoded unicode
  - make sure we write encoded strings for json
  - add test for LeaseManager


Version 0.28 release                                                  2015-04-16
--------------------------------------------------------------------------------

  - minimize an unlock/release race
  - resilience against missing git in setup.py
  - cleaner version string
  - sync setup.py with recent changes in RP
  - export range collapsing algorithm
  - fix documentation
  - fix reporting of type checks when multiple types are valid and optional; fix URL init sig
  - fix url tests
  - install sdist, export sdist location
  - tool rename 
  - add version tool 


Version 0.8 release                                                   2015-02-24
--------------------------------------------------------------------------------

  - Preserve case for "hostnames" in GO URIs.
  - Allow # in host part of Globus Online URIs.
  - preserve trailing slashes on URL paths
  - move sources into src/
  - fix version strings.  again.
  - fix id counters to start from 0
  - fix comment parsing fix in second json read


Version 0.7.11 release                                                2014-12-11
--------------------------------------------------------------------------------

  - fixed botched merge from devel


Version 0.7.10 release                                                2014-12-11
--------------------------------------------------------------------------------

  - fixed lease manager debug output


Version 0.7.9 release                                                 2014-12-02
--------------------------------------------------------------------------------

  - fix to json comment parsing
  - att flexible pylint util
  - support append / overwrite mode for log targets 
  - add daemonize class
  - fix leaking logs
  - reduce lease manager logging noise 
  - log python version on startup 
  - add reporter class for nice demo output etc
  - add namespace for object_cache, backward compatible 
  - fix bson/json/timestamp mangling 
  - json support for json writing


Version 0.7.8 release                                                 2014-10-29
--------------------------------------------------------------------------------
 
  - implemented lease manager (manage finite set of resources with
    finite lifetime over multiple threads)
  - implemented DebugHelper class (prints stack traces for all threads
    on SIGUSR1
  - implement decorator for class method timings
  - cache configuration settings on logger creation, which
    significantly speeds up logging over different log objects
  - remove deepcopy from configuration management (improves
    performance)
  - add wildcard expanstion on  dict_merge
  - make pymongo and nose dependencies optional


Version 0.7.7 release                                                 2014-08-27
--------------------------------------------------------------------------------

  - lease manager which handles resource leases (like, leases ssh connections to saga adaptors)
  - fixes on deepcopy, logging and config handling 


Version 0.7.5 release                                                 2014-07-22
--------------------------------------------------------------------------------

  - Some small bug fixes.


--------------------------------------------------------------------------------

