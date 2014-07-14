

import os
import regex
import url as ruu


# ------------------------------------------------------------------------------
#
def split_dburl (dburl, default_url=None) :
    """
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    """

    # if the given URL does not contain schema nor host, the default URL is used
    # as base, and the given URL string is appended to the path element.
    
    url = ruu.Url (dburl)

    if  not url.schema and not url.host :
        url      = ruu.Url (default_url)
        url.path = dburl

    if  url.schema != 'mongodb' :
        raise ValueError ("url must be a 'mongodb://' url, not %s" % dburl)

    host = url.host
    port = url.port
    path = url.path
    user = url.username
    pwd  = url.password

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
    return [host, port, dbname, cname, pname, user, pwd]


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

