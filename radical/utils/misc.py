

import os
import regex


# ------------------------------------------------------------------------------
#
def split_dburl (url, default_url=None) :
    """
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    """

    # if the given URL does not contain schema nor host, the default URL is used
    # as base, and the given URL string is appended to the path element.
    
    if  '://' not in url and default_url:
        url = "%s/%s" % (default_url, url)

    slashes = [idx for [idx,elem] in enumerate(url) if elem == '/']

    if  len(slashes) < 3 :
        url += '/'

    slashes = [idx for [idx,elem] in enumerate(url) if elem == '/']

    if  url[:slashes[0]].lower() != 'mongodb:' :
        raise ValueError ("url must be a 'mongodb://' url, not %s" % url)

  # if  len(url) <= slashes[2]+1 :
  #     raise ValueError ("url needs to be a mongodb url, the path element " \
  #                       "must specify the database and collection id")

    base_url = url[slashes[1]+1:slashes[2]]
    path     = url[slashes[2]+1:]

    if  ':' in base_url :
        host, port = base_url.split (':', 1)
        port = int(port)
    else :
        host, port = base_url, None

    path = os.path.normpath(path)
    if  path.startswith ('/') :
        path = path[1:]
    path_elems = path.split ('/')


    dbname = None
    cname  = None
    pname  = None

    if  len(path_elems)  >  0 :
        dbname = path_elems[0]

    if  len(path_elems)  >  1 :
        dbname = path_elems[0]
        cname  = path_elems[1]

    if  len(path_elems)  >  2 :
        dbname = path_elems[0]
        cname  = path_elems[1]
        pname  = '/'.join (path_elems[2:])

    if  dbname == '.' : 
        dbname = None

  # print str([host, port, dbname, cname, pname])
    return [host, port, dbname, cname, pname]


# ------------------------------------------------------------------------------
#
def parse_file_staging_directives (directives) :
    """
    staging directives

       [local_path] [operator] [remote_path]

    local path: 
        * interpreted as relative to the application's working directory
        * must point to local storage (localhost)
    
    remote path
        * interpreted as relative to the job's working directory

    operator :
        * >  : stage to remote target, overwrite if exists
        * >> : stage to remote target, append    if exists
        * <  : stage to local  target, overwrite if exists
        * << : stage to local  target, append    if exists

    This method returns a tuple [src, tgt, op] for each given directive.  This
    parsing is backward compatible with the simple staging directives used
    previously -- any strings which do not contain staging operators will be
    interpreted as simple paths (identical for src and tgt), operation is set to
    '=', which must be interpreted in the caller context.  
    """

    bulk = True
    if  not isinstance (directives, list) :
        bulk       = False
        directives = [directives]

    ret = list()

    for directive in directives :

        if  not isinstance (directive, basestring) :
            raise TypeError ("file staging directives muct by of type string, "
                             "not %s" % type(directive))

        rs = regex.ReString (directive)

        if  rs // '^(?P<one>.+?)\s*(?P<op><|<<|>|>>)\s*(?P<two>.+)$' :
            res = rs.get ()
            ret.append ([res['one'], res['two'], res['op']])

        else :
            ret.append ([directive, directive, '='])

    if  bulk : return ret
    else     : return ret[0]


# ------------------------------------------------------------------------------
#
def time_diff (dt_abs, dt_stamp) :
    """
    return the time difference bewteen  two datetime 
    objects in seconds (incl. fractions).  Exceptions (like on improper data
    types) fall through.
    """

    delta = dt_stamp - dt_abs

    import datetime
    if  not isinstance  (delta, datetime.timedelta) :
        raise TypeError ("difference between '%s' and '%s' is not a .timedelta" \
                      % (type(dt_abs), type(td_stamp)))

    # get seconds as float 
    seconds = delta.seconds + delta.microseconds/1E6
    return seconds


# ------------------------------------------------------------------------------

