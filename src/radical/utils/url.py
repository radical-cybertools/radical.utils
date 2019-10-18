
__author__    = 'Radical.Utils Development Team (Andre Merzky, Ole Weidner)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import os

from .contrib import urlparse25 as urlparse
from .        import signatures as rus


# ------------------------------------------------------------------------------
#
class Url(object):
    '''
    The RADICAL Url class.

    URLs are used in several places in the RADICAL software projects: to specify
    service endpoints for job submission or resource management, for file or
    directory locations, etc.

    The URL class is designed to simplify URL management for these purposes --
    it allows to manipulate individual URL elements, while ensuring that the
    resulting URL is well formatted. Example::

      # create a URL from a string
      location = radical.utils.Url('file://localhost/tmp/file.dat')
      d = radical.utils.filesystem.Directory(location)

    A URL consists of the following components (where one ore more can be
    'None')::

      <scheme>://<user>:<pass>@<host>:<port>/<path>?<query>#<fragment>

    Each of these components can be accessed via its property or alternatively,
    via getter / setter methods. Example::

      url = ru.Url('scheme://pass:user@host:port/path?query#fragment')

      # modify the scheme
      url.scheme = 'anotherscheme'

      # above is equivalent with
      url.set_scheme('anotherscheme')
    '''

    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 rus.optional((str, 'Url')))
    @rus.returns(rus.nothing)
    def __init__(self, url_in=None):
        '''
        Create a new Url object from a string or another Url object.
        '''

        if url_in is None:
            url_in = ''

        self._urlobj = urlparse.urlparse(str(url_in), allow_fragments=True)
        self._renew_url()


    # --------------------------------------------------------------------------
    #
    ##
    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def __str__ (self):
        '''
        String representation.
        '''

        return self._urlobj.geturl()


    # --------------------------------------------------------------------------
    #
    ##
    @rus.takes  ('Url')
    @rus.returns(str)
    def __unicode__(self):
        '''
        Unicode representation.
        '''

        return '%s'  %  str(self._urlobj.geturl())


    # --------------------------------------------------------------------------
    #
    ##
    @rus.takes  ('Url',
                 ('Url', dict))
    @rus.returns('Url')
    def __deepcopy__(self, memo):
        '''
        Deep copy of a Url
        '''

        return Url(self)


    # --------------------------------------------------------------------------
    #
    ##
    @rus.takes  ('Url')
    @rus.returns(bool)
    def __nonzero__(self):

        return bool(str(self))


    # --------------------------------------------------------------------------
    #
    ##
    @rus.takes  ('Url',
                 rus.optional(str),
                 rus.optional(str),
                 rus.optional(str),
                 rus.optional((str, int)))
    @rus.returns(str)
    def _make_netloc(self, username, password, hostname, port):
        '''
        helper function to generate netloc string.
        '''

        netloc = ''

        if   username and password: netloc += '%s:%s@' % (username, password)
        elif username             : netloc += '%s@'    %  username
        if   hostname             : netloc += '%s'     %  hostname
        if   port                 : netloc += ':%s'    %  port

        return netloc


    # --------------------------------------------------------------------------
    #
    def _renew_netloc(self, username='', password='', hostname='', port=''):

        newloc = self._make_netloc(username or self._urlobj.username,
                                   password or self._urlobj.password,
                                   hostname or self._urlobj.hostname,
                                   port     or self._urlobj.port)

        self._renew_url(netloc=newloc)


    # --------------------------------------------------------------------------
    #
    def _renew_url(self, scheme='', netloc='', path='',
                         params='', query='',  fragment='', force_path=False):

        # always normalize the path.
        path = self.normpath(path)

        if force_path:
            path = path or '/'

        newurl = urlparse.urlunparse((scheme   or self._urlobj.scheme,
                                      netloc   or self._urlobj.netloc,
                                      path     or self._urlobj.path,
                                      params   or self._urlobj.params,
                                      query    or self._urlobj.query,
                                      fragment or self._urlobj.fragment))

        self._urlobj = urlparse.urlparse(newurl, allow_fragments=True)


    # --------------------------------------------------------------------------
    #
    def normpath(self, path):

        if path:

            # Alas, os.path.normpath removes trailing slashes,
            # so we re-add them.
            if len(path) > 1 and path.endswith('/'):
                trailing_slash = True
            else:
                trailing_slash = False

            path = os.path.normpath(path)

            if trailing_slash and not path.endswith('/'):
                path += '/'

            # os.path.normpath does not normalize for multiple leading slashes
            while path.startswith('//'):
                path = path[1:]

        return path


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_scheme(self, scheme):
        '''
        Set the URL 'scheme' component.

        :param scheme: The new scheme
        :type  scheme: str

        '''

        self._renew_url(scheme=scheme)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_scheme(self):
        '''
        Return the URL 'scheme' component.
        '''

        return self._urlobj.scheme


    scheme = property(get_scheme, set_scheme)
    schema = scheme  # alias, as both terms are used...
    ''' The scheme component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_host(self, hostname):
        '''
        Set the 'hostname' component.

        :param hostname: The new hostname
        :type  hostname: str
        '''

        self._renew_netloc(hostname=hostname)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_host(self):
        '''
        Return the URL 'hostname' component.
        '''

        return self._urlobj.hostname


    host = property(get_host, set_host)
    ''' The hostname component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str, int))
    @rus.returns(rus.nothing)
    def set_port(self, port):
        '''
        Set the URL 'port' component.

        :param port: The new port
        :type  port: int
        '''

        self._renew_netloc(port=port)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, int))
    def get_port(self):
        '''
        Return the URL 'port' component.
        '''

        if self._urlobj.port is None:
            return None

        return int(self._urlobj.port)


    port = property(get_port, set_port)
    ''' The port component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_username(self, username):
        '''
        Set the URL 'username' component.

        :param username: The new username
        :type  username: str
        '''

        self._renew_netloc(username=username)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_username(self):
        '''
        Return the URL 'username' component.
        '''

        return self._urlobj.username


    username = property(get_username, set_username)
    ''' The username component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_password(self, password):
        '''
        Set the URL 'password' component.

        :param password: The new password
        :type password:  str
        '''

        self._renew_netloc(password=password)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_password(self):
        '''
        Return the URL 'username' component.
        '''

        return self._urlobj.password


    password = property(get_password, set_password)
    ''' The password component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_fragment(self, fragment):
        '''
        Set the URL 'fragment' component.

        :param fragment: The new fragment
        :type fragment:  str
        '''

        self._renew_url(fragment=fragment)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_fragment(self):
        '''
        Return the URL 'fragment' component.
        '''

        return self._urlobj.fragment


    fragment = property(get_fragment, set_fragment)
    ''' The fragment component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_path(self, path):
        '''
        Set the URL 'path' component.

        :param path: The new path
        :type path:  str
        '''

        self._renew_url(path=path, force_path=True)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_path(self):
        '''
        Return the URL 'path' component.
        '''

        path = self._urlobj.path
        path = path.split('?')[0]  # remove query

        return self.normpath(path)


    path = property(get_path, set_path)
    ''' The path component.  '''


    # --------------------------------------------------------------------------
    #
    @rus.takes  ('Url',
                 (rus.nothing, str))
    @rus.returns(rus.nothing)
    def set_query(self, query):
        '''
        Set the URL 'query' component.

        :param query: The new query
        :type query:  str
        '''

        self._renew_url(query=query)


    @rus.takes  ('Url')
    @rus.returns((rus.nothing, str))
    def get_query(self):
        '''
        Return the URL 'query' component.
        '''

        if self._urlobj.query:
            return self._urlobj.query

        if '?' in self._urlobj.path:
            return self._urlobj.path.split('?', 1)[1]  # remove path


    query = property(get_query, set_query)
    ''' The query component.  '''


# --------------------------------------------------------------------

