

import os


# ------------------------------------------------------------------------------
#
def split_dburl (url) :
    """
    we split the url into the base mongodb URL, and the path element, whose
    first element is the database name, and the remainder is interpreted as
    collection id.
    """

    slashes = [idx for [idx,elem] in enumerate(url) if elem == '/']

    if  len(slashes) < 3 :
        raise ValueError ("url needs to be a mongodb URL, the path element " \
                          "must specify the database and collection id")

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

