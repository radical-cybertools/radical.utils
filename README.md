
[![Build Status](https://travis-ci.org/radical-cybertools/radical.utils.svg?branch=devel)](https://travis-ci.org/radical-cybertools/radical.utils)

Package radical.utils
=====================

This Python package contains shared code and tools for various 
[Radical Group](http://radical.rutgers.edu) projects. Sometimes we call it the 
Radical Kitchen Sink.  

The radical.utils package contains the following things:

* A [URL class](https://github.com/saga-project/radical.utils/blob/master/radical/utils/url.py) (A SAGA-compliant url parser class)
* A [Plugin manager](https://github.com/saga-project/radical.utils/blob/master/radical/utils/plugin_manager.py) (A simple yet flexible plugin manager and loader)
* A [Config parser](https://github.com/saga-project/radical.utils/tree/master/radical/utils/config) (Config file reader writeer parser)
* A [Logger](https://github.com/saga-project/radical.utils/tree/master/radical/utils/logger) (Support for color log output and tracing)
* An [Object cache](https://github.com/saga-project/radical.utils/blob/master/radical/utils/object_cache.py) (Well, an object cache)
* A [Type checking module](https://github.com/saga-project/radical.utils/blob/master/radical/utils/signatures.py) (Type checking for python call signatures)
* A [Singleton metaclass](https://github.com/saga-project/radical.utils/blob/master/radical/utils/singleton.py) (Implements the Sigleton pattern)
* A [Thread wrapper](https://github.com/saga-project/radical.utils/blob/master/radical/utils/threads.py) (A thin convenience wrapper around threading.Thread)
* A [Call tracer](https://github.com/saga-project/radical.utils/blob/master/radical/utils/tracer.py) (Support for call tracing / debugging)
* A ['which' function](https://github.com/saga-project/radical.utils/blob/master/radical/utils/which.py)(Simliar to the 'which' command line tool)

The package should in general be compatible with Python 2.6.



License
-------

This software is released under the 
[MIT License](http://opensource.org/licenses/MIT).

Parts of the module (radical.utils.atfork) are licensed under the [Apache-v2.0
license](http://www.apache.org/licenses/).


Installation 
------------

You can install the latest radical.utils directly from [PyPi](https://pypi.python.org/pypi/radical.utils/):

    pip install --upgrade radical.utils

You can also install the latest development version (which might be broken)
from GitHub directly:

    pip install -e git://github.com/saga-project/radical.utils.git#egg=radical.utils

If you want to use radical.utils in your own project, include it in your 
`setup.py` file:

    'install_requires': ['radical.utils']


Documentation
-------------

You can generate the documentation yourself with [Sphinx](http://sphinx-doc.org/)

    pip install sphinx
    make docs

The resulting documentation will be in `open docs/build/html/index.html`.

