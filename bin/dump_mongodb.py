#!/usr/bin/env python


import os
import sys
import pymongo


_DEFAULT_DBURL = 'mongodb://localhost:27017/'
_DEFAULT_DBURL = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'


# ------------------------------------------------------------------------------
#
def usage (msg=None) :

    if  msg :
        print "\n\t%s\n" % msg

    print """

      usage   : %s [command] [url]
      example : %s mongodb://localhost/synapse_profiles/profiles/

      The URL is interpreted as:
          [schema]://[host]:[port]/[database]/[collection]/[document_id]

      Commands are:

        tree:   show a tree of the hierarchy, but only  document IDs, no content
        dump:   show a tree of the hierarchy, including document contents
        list:   list entries in the subtree, but do not traverse
        remove: remove the specified subtree

      The default command is 'tree'.  
      The default URL is """ + "%s\n\n" % _DEFAULT_DBURL

    if  msg :
        sys.exit (1)

    sys.exit (0)


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

# ------------------------------------------------------------------------------
#
def dump (url, mode) :
    """
    Connect to mongodb at the given location, and traverse the data bases
    """

    [host, port, dbname, cname, pname] = split_dburl (url)

    db_client  = pymongo.MongoClient (host=host, port=port)

    print 'host      : %s' % host
    print 'port      : %s' % port

 
    if  dbname : dbnames = [dbname]
    else       : dbnames = db_client.database_names ()

    for name in dbnames :

        if  mode == 'list' and not dbname :
            print " +-- db   %s" % name

        elif  mode == 'remove' :
            
            if (not dbname) or (name == dbname) :
                try :
                    db_client.drop_database (name)
                    print "  removed database %s" % name
                except :
                    pass # ignore system tables

        else :
            handle_db (db_client, mode, name, cname, pname)

    db_client.disconnect ()


# ------------------------------------------------------------------------------
def handle_db (db_client, mode, dbname, cname, pname) :
    """
    For the given db, traverse collections
    """

    database = db_client[dbname]
    print " +-- db   %s" % dbname


    if  cname : cnames = [cname]
    else      : cnames = database.collection_names ()

    for name in cnames :

        if  mode == 'list' and not cname :
            print " | +-- coll %s" % name

        elif  mode == 'remove' and not pname :
            try :
                database.drop_collection (name)
                print "  removed collection %s" % name
            except :
                pass # ignore errors

        else :
            handle_coll (database, mode, name, pname)



# ------------------------------------------------------------------------------
def handle_coll (database, mode, cname, pname) :
    """
    For a given collection, traverse all documents
    """

    if 'indexes' in cname :
        return

    collection = database[cname]
    print " | +-- coll %s" % cname

    docs = collection.find ()

    for doc in docs :

        name = doc['_id']

        if  mode == 'list' and not pname :
            print " | | +-- doc  %s" % name

        elif  mode == 'remove' :
            if (not pname) or (str(name)==str(pname)) :
                try :
                    collection.remove (name)
                    print "  removed document %s" % name
                except Exception as e:
                    pass # ignore errors

        else :
            if (not pname) or (str(name)==str(pname)) :
                handle_doc (collection, mode, doc)


# ------------------------------------------------------------------------------
def handle_doc (collection, mode, doc) :
    """
    And, surprise, for a given document, show it according to 'mode'
    """

    name = doc['_id']

    if  mode == 'list' :

        for key in doc :
            print " | | | +-- %s" % (key)

    elif  mode == 'tree' :
        print " | | +-- doc  %s" % (name)
        for key in doc :
            print " | | | +-- %s" % (key)

    elif  mode == 'dump' :
        print " | | +-- doc  %s" % (name)
        for key in doc :
            print " | | | +-- %-10s : %s" % (key, doc[key])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__' :

    if  '--help' in sys.argv or \
        'help'   in sys.argv or \
         '-h'    in sys.argv :
        usage ()

    elif  len(sys.argv) == 3 :
        mode = sys.argv[1]
        url  = sys.argv[2]
    
    elif len(sys.argv) == 2 :
        mode = sys.argv[1]
        url  = _DEFAULT_DBURL

    elif len(sys.argv) == 1 :
        mode = 'tree'
        url  = _DEFAULT_DBURL

    else :
        usage ("incorrect usage -- too many arguments")

    dump (url, mode)


# ------------------------------------------------------------------------------

