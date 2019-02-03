
  - For a list of bug fixes, see
    https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
  - For a list of open issues and known problems, see
    https://github.com/radical-cybertools/radical.utils/issues?q=is%3Aissue+is%3Aopen+



0.50.3 Release                                                        2018-12-19
--------------------------------------------------------------------------------

  - fix msg evaluation in process watcher (#149) 
  - update setup.py (#146) 
  - a bitarray based scheduler 
  - use config files for bitarray scheduler configuration 
  - add install info for macos 
  - adding git-error in version check (#145) 
  - fix div-by-zero on fast runs
  - handle `None` messages in watcher (#141) 


0.50.2 Release                                                        2018-08-20
--------------------------------------------------------------------------------

  - fix profile cleanup to work w/o state models


0.50.1 Release                                                        2018-07-03
--------------------------------------------------------------------------------

  - fix relocation ID state storage 
  - fix issue RS-661
  - support dynamic code snippet injection 
  - avoid mkdir race 
  - expand exe excution logic from RP 
  - fix array test 
  - uniform env getter for radical namespace


0.47.5 Release                                                        2018-06-02
--------------------------------------------------------------------------------

  - fix and cleanup log, prof and rep settings


0.47.4 Release                                                        2018-03-27
--------------------------------------------------------------------------------

  - fix RA issue #65


0.47.3 Release                                                        2018-03-20
--------------------------------------------------------------------------------

  - more thorough approach to relocate ID state storage (#131) 
  - travis badge 


0.47.2 Release                                                        2018-02-28
--------------------------------------------------------------------------------

  - relocate ID state storage (#131) 


0.47.1 Release                                                        2018-02-21
--------------------------------------------------------------------------------

  - introduce name spaces for the ID generator
  - iterate on some of the debug methods


0.46.2 Release                                                        2017-11-19
--------------------------------------------------------------------------------

  - backport of profile handle checks
  - add tid arg to prof
  - add support for legacy profiles
  - clean profile handle check
  - add shell callout helper
  - function stack recognizes anaconda virtual envs
  - reduce profile buffering
  - fixes issue #120
  - add comments on profil format
  - fix #RA-52
  - relax time limits on process / thread creation
  - radical-stack-clone supports Conda Envs Now!! :)
  - add -p to specify python executable to stack-clone
  - add a `get_size()` debug method
  - cooperative process termination
  - make ID generator behave under docker
  - move gettid to where it belongs
  - add close() method to logger
  - expand poller lock scope
  - remove some debug prints
  - support changes in RA
  - improve runtime behavior of concurrency calculation


0.46.2 Release                                                        2017-08-23
--------------------------------------------------------------------------------

  - hotfix for RP #1415


0.46.1 Release                                                        2017-08-23
--------------------------------------------------------------------------------

  - hotfix for RP #1415


Version 0.46 release                                                  2017-08-11
--------------------------------------------------------------------------------

  - Feature/managed process (#104) 
  - Feature/poll (#106) 
  - Fix/version strings (#107) 
  - Revert "Feature/managed process" (#102) 
  - add a managed process class 
  - add close() method to logger 
  - add some doc on raise_in_thread() 
  - apply ru.Process syntax/semantics to ru.Thread 
  - be friendly to MacOS 
  - clean out and comment on utils thread wrapper 
  - cleanup on ru Process class, tests 
  - fix logging, leave parent termination alone 
  - fix process termination test case 
  - fix raise_on messaging and eval; work on cancellation (#101) 
  - fix thread locality, logging 
  - fuck python 
  - get stack info and clone in sync 
  - improve error reporting on child processes 
  - iterate and complete documentation; 
  - iteration on reliable process management 
  - make a sanity check non-fatal 
  - make sure that thread local data are accessible 
  - move gettid to where it belongs 
  - proper handling of null logger 
  - remove potential filename collission in test suite 
  - rename ru.Thread to ru.Future (#95) 
  - remove some debug prints/logs, add assert 
  - simplify radical-stack, make programmatic 
  - support for cprofile in ru.Thread 
  - rename ru.Thread to ru.Future


Version 0.45 release                                                  2017-02-28
--------------------------------------------------------------------------------

  - add radical-stack, radical-stack-clone tools
  - setup fix 
  - silence a warning 
  - Silence empty profile warning 


Version 0.44 release                                                  2016-11-01
--------------------------------------------------------------------------------

  - remote debugging! 
  - SIGINT is nono in threads 
  - comment on exception types in raise_in_thread 
  - add range matching alg 
  - add condition watcher 
  - add some tracing methods
  - add get_thread_name and get_thread_id 
  - add raise_in_thread 
  - add netifaces dep 
  - hardcode the netiface version until it is fixed upstream. 
  - allow to overwrite profiler name 
  - fix double releases 
  - convert atfork monkeypatch exception into warning 
  - make singleton fork-safe 
  - move get_hostip and get_hostname to ru 
  - move profiler from RP to RU 
  - move raise_on functionality from rpu to ru, to support random 
    error triggers for stress testing 
  - make raise_on thread save, get limits from env 


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

