
import re
import os

_pat = r'^\s*(?P<detail>(?P<short>[^-]*)(?P<base>-[^-@]+?)?(-[^@]+?)?(?P<branch>@.+?)?)\s*$'


# ------------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - version_short :  1.2.3                       - used for installation
#   - version_detail:  1.2.3-v1.1-9-g0684b06@devel - used at runtime (ru.version)
#   - both are derived from the last git tag and branch information
#   - VERSION files are created on install, with the version_detail
#
def get_version(paths=None):
    """
    paths:
        a VERSION file containing the detailed version is checked for in every
        directory listed in paths.   When we find a VERSION file, we also look
        for an SDIST file, and return the found name and location as absolute
        path to the sdist.
    """

    if not paths:
        # by default, get version for myself
        pwd   = os.path.dirname(__file__)
        root  = "%s/.." % pwd
        paths = [root, pwd]

    if not isinstance(paths, list):
        paths = [paths]

    version_short  = None
    version_detail = None
    version_base   = None
    version_branch = None
    sdist_name     = None
    sdist_path     = None
    err            = ''

    # if in any of the paths a VERSION file exists, we use the detailed version
    # in there.
    for path in paths:

        try:
            version_path = "%s/VERSION" % path

            with open(version_path) as f:
                data    = f.read()
                lines   = data.split('\n', 1)
                lines   = [line.strip() for line in lines]
                lines   = [line         for line in lines if line]
                detail  = lines[-1]
                pattern = re.compile(_pat)
                match   = pattern.search(detail)

                if match:
                    version_short  = match.group('short').strip()
                    version_detail = match.group('detail').strip()
                    version_base   = match.group('base').strip()
                    version_branch = match.group('branch').strip()
                    break

        except Exception as e:
            # ignore missing VERSION file -- this is caught below.  But ew keep
            # the error message
            err += '%s\n' % repr(e)

    if version_detail:
        # check if there is also an SDIST near the version_path
        sdist_path = version_path.replace('/VERSION', '/SDIST')
        try:
            with open(sdist_path) as fh:
                sdist_name = fh.read().strip()
        except Exception as e:
            # ignore missing SDIST file
            pass

        sdist_path = version_path.replace('/VERSION', '/%s' % sdist_name)

    # check if any one worked ok
    if version_detail:
        return (version_short, version_detail, version_base, version_branch,
                sdist_name, sdist_path)
    else:
        raise RuntimeError("Cannot determine version from %s (%s)"
                          % (paths, err.strip()))


# ------------------------------------------------------------------------------

