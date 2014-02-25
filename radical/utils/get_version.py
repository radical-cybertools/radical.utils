

import re
import os
import sys
import subprocess as sp


# ------------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - short_version:  1.2.3                   - is used for installation
#   - long_version:   1.2.3-9-g0684b06-devel  - is used as runtime (ru.version)
#   - both are derived from the last git tag and branch information
#   - VERSION files are created on demand, with the long_version
#
def get_version (paths=None):
    """
    paths:
        a VERSION file containing the long version is created in every directpry
        listed in paths.  Those VERSION files are used when they exist to get
        the version numbers, if they exist prior to calling this method.  If 
        not, we cd into the first path, try to get version numbers from git tags 
        in that location, and create the VERSION files in all dirst given in 
        paths.
    """

    try:

        if  None == paths :
            # by default, get version for myself
            pwd     = os.path.dirname (__file__)
            root    = "%s/.." % pwd
            paths = [root, pwd]

        if  not isinstance (paths, list) :
            paths = [paths]

        # if in any of the paths a VERSION file exists, we use the long version
        # in there.
        long_version  = None
        short_version = None
        branch_name   = None

        for path in paths :
            try :
                filename = "%s/VERSION" % path
                with open (filename) as f :
                    lines = [line.strip() for line in f.readlines()]

                    if len(lines) >= 1 : long_version  = lines[0]
                    if len(lines) >= 2 : short_version = lines[1]
                    if len(lines) >= 3 : branch_name   = lines[2]

                    if  long_version :
                      # print 'reading  %s' % filename
                        break

            except Exception as e :
                pass

        # if we didn't find it, get it from git 
        if  not long_version :

            # make sure we look at the right git repo
            if  len(paths) :
                git_cd  = "cd %s ;" % paths[0]

            # attempt to get version information from git
            p   = sp.Popen ('%s'\
                            'git describe --tags --always ; ' \
                            'git branch   --contains | grep -e "^\*"' % git_cd,
                            stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
            out = p.communicate()[0]

            if  p.returncode != 0 or not out :

                # the git check failed -- its likely that we are called from
                # a tarball, so use ./VERSION instead
                out=open ("%s/VERSION" % srcroot, 'r').read().strip()


            pattern = re.compile ('(?P<long>(?P<short>[\d\.]+).*?)(\s+\*\s+(?P<branch>\S+))?$')
            match   = pattern.search (out)

            if  match :
                long_version  = match.group ('long')
                short_version = match.group ('short')
                branch_name   = match.group ('branch')
              # print 'inspecting git for version info'

            else :
                import sys
                sys.stderr.write ("Cannot determine version from git or ./VERSION\n")
                sys.exit (-1)
                

            if  branch_name :
                long_version = "%s-%s" % (long_version, branch_name)


        # make sure the version files exist for the runtime version inspection
        for path in paths :
            vpath = '%s/VERSION' % path
          # print 'creating %s'  % vpath
            with open (vpath, 'w') as f :
                f.write (long_version  + "\n")
                f.write (short_version + "\n")
                f.write (branch_name   + "\n")
    
        return short_version, long_version, branch_name


    except Exception as e :
        print 'Could not extract/set version: %s' % e
        import sys
        sys.exit (-1)


