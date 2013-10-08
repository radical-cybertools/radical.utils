Package radical.utils
=====================

This Python package contains shared code and tools for various 
[Radical Group](http://radical.rutgers.edu) projects. Sometimes we call it the 
Radical Kitchen Sink. 

The radical.utils package contains the following things:

* A [Plugin Manager](https://github.com/saga-project/radical.utils/blob/master/radical/utils/plugin_manager.py)
* A Config Parser
* A Logging Facility


License
-------

This software is released under the 
[MIT License](http://opensource.org/licenses/MIT).


Installation 
------------

You can install the latest radical.utils directly from [PyPi](XXX):

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