#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils as ru
from   radical.utils.contrib.urlparse25 import urljoin


# ------------------------------------------------------------------------------
#
def test_contrib():

    test_cases = [('http://a/b/c/d', ''                    ),
                  ('g:h',            'g:h'                 ),
                  ('http:g',         'http://a/b/c/g'      ),
                  ('http:',          'http://a/b/c/d'      ),
                  ('g',              'http://a/b/c/g'      ),
                  ('./g',            'http://a/b/c/g'      ),
                  ('g/',             'http://a/b/c/g/'     ),
                  ('/g',             'http://a/g'          ),
                  ('//g',            'http://g'            ),
                  ('?y',             'http://a/b/c/?y'     ),  # [1]
                  ('g?y',            'http://a/b/c/g?y'    ),
                  ('g?y/./x',        'http://a/b/c/g?y/./x'),
                  ('.',              'http://a/b/c/'       ),
                  ('./',             'http://a/b/c/'       ),
                  ('..',             'http://a/b/'         ),
                  ('../',            'http://a/b/'         ),
                  ('../g',           'http://a/b/g'        ),
                  ('../..',          'http://a/'           ),
                  ('../../g',        'http://a/g'          ),
                  ('../../../g',     'http://a/../g'       ),
                  ('./../g',         'http://a/b/g'        ),
                  ('./g/.',          'http://a/b/c/g/'     ),
                  ('/./g',           'http://a/./g'        ),
                  ('g/./h',          'http://a/b/c/g/h'    ),
                  ('g/../h',         'http://a/b/c/h'      ),
                  ('http:g',         'http://a/b/c/g'      ),
                  ('http:',          'http://a/b/c/d'      ),
                  ('http:?y',        'http://a/b/c/?y'     ),  # [1]
                  ('http:g?y',       'http://a/b/c/g?y'    ),
                  ('http:g?y/./x',   'http://a/b/c/g?y/./x')]

    # [1] https://bugs.python.org/issue18828 - open since 2013 :-/
    # This test case *should* result in `http://a/b/c/d?y`


    base = ''
    for tc in test_cases:

        url    = tc[0]
        check  = tc[1]
        result = urljoin(base, url)

        if check:
            assert(result == check), '%s == %s' % (result, check)

        if not base:
            base = result


# -------------------------------------------------------------------------
#
def test_url_api():

    # test basic functionality for valid schemas

    u1 = ru.Url("ssh://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "ssh"
    assert u1.username == "user"
    assert u1.password == "pwd"
    assert u1.host     == "hostname.domain"
    assert u1.port     == int(9999)


# ------------------------------------------------------------------------------
#
def test_url_scheme_issue():

    # test basic functionality for invalid schemas

    u1 = ru.Url("unknownscheme://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "unknownscheme"
    assert u1.username == "user"
    assert u1.password == "pwd"
    assert u1.host     == "hostname.domain"
    assert u1.port     == int(9999)


# ------------------------------------------------------------------------------
#
def test_url_issue_49():

    # ensure correct str serialization after setting elements

    url = ru.Url   ("scheme://pass:user@host:123/dir/file?query#fragment")
    url.set_host   ('remote.host.net')
    url.set_scheme ('sftp')
    url.set_path   ('/tmp/data')

    assert str(url) == "sftp://pass:user@remote.host.net:123/tmp/data"


# ------------------------------------------------------------------------------
#
def test_url_issue_61():

    # ensure correct query extraction

    url = ru.Url ("advert://localhost/?dbtype=sqlite3")
    assert url.query == "dbtype=sqlite3"


# ------------------------------------------------------------------------------
#
def test_url_issue_rs_305():
    # This compensates
    #
    #     >>> import os
    #     >>> os.path.normpath('//path//to//dir//')
    #     '//path/to/dir'
    #
    # to a normalization resulting in
    #
    #     '/path/to/dir'
    #
    # as required by the SAGA spec

    url1 = ru.Url ("advert://localhost/path/to/file")
    url2 = ru.Url ("advert://localhost//path/to/file")
    assert url1.path == url2.path


# ------------------------------------------------------------------------------
#
def test_url_properties():

    # test various properties

    url = ru.Url("")
    assert str(url)           == ""

    url.scheme = "scheme"
    assert str(url)           == "scheme://"
    assert url.get_scheme()   == "scheme"

    url.set_scheme("tscheme")
    assert url.get_scheme()   == "tscheme"

    url.scheme = "scheme"
    url.host   = "host"
    assert str(url)           == "scheme://host"
    assert url.get_host()     == "host"

    url.set_host("thost")
    assert url.get_host()     == "thost"

    url.host = "host"
    url.port = 42
    assert str(url)           == "scheme://host:42"
    assert url.get_port()     == 42

    url.set_port(43)
    assert url.get_port()     == 43

    url.port     = 42
    url.username = "username"
    assert str(url)           == "scheme://username@host:42"
    assert url.get_username() == "username"

    url.set_username("tusername")
    assert url.get_username() == "tusername"

    url.username = "username"
    url.password = "password"
    assert str(url)           == "scheme://username:password@host:42"
    assert url.get_password() == "password"

    url.set_password("tpassword")
    assert url.get_password() == "tpassword"

    url.password = "password"
    url.path     = "/path/"
    assert str(url)           == "scheme://username:password@host:42/path/"
    assert url.get_path()     == "/path/"

    url.set_path("tpath")
    assert url.get_path()     == "/tpath"


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == "__main__":

    test_contrib()
    test_url_api()
    test_url_scheme_issue()
    test_url_issue_49()
    test_url_issue_61()
    test_url_issue_rs_305()
    test_url_properties()


# ------------------------------------------------------------------------------

