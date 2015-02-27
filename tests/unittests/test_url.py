
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils as ru


# -------------------------------------------------------------------------
#
def test_url_compatibility () :
    """
    Test basic url compatibility
    """

    u1 = ru.Url("ssh://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "ssh"
    assert u1.username == "user"
    assert u1.password == "pwd"
    assert u1.host     == "hostname.domain"
    assert u1.port     == int(9999)


# ------------------------------------------------------------------------------
#
def test_url_scheme_issue () :
    """
    Test url schema issues
    """

    u1 = ru.Url("unknownscheme://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "unknownscheme"
    assert u1.username == "user"
    assert u1.password == "pwd"
    assert u1.host     == "hostname.domain"
    assert u1.port     == int(9999)

# ------------------------------------------------------------------------------
#
def test_url_issue_49 () : 
    """
    Test url issue #49
    """

    url = ru.Url ("scheme://pass:user@host:123/dir/file?query#fragment")
    url.set_host   ('remote.host.net')
    url.set_scheme ('sftp') 
    url.set_path   ('/tmp/data')
    
    assert str(url) == "sftp://pass:user@remote.host.net:123/tmp/data"

# ------------------------------------------------------------------------------
#
def test_url_issue_61 () : 
    """
    Test url issue #61
    """

    url = ru.Url ("advert://localhost/?dbtype=sqlite3")
    assert url.query == "dbtype=sqlite3"

# ------------------------------------------------------------------------------
#
def test_url_properties () :
    """
    Test url properties
    """

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

    test_url_compatibility ()
    test_url_scheme_issue  ()
    test_url_issue_49      ()
    test_url_issue_61      ()
    test_url_properties    ()

