
import re
import os

from .misc import ru_open

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
        directory listed in paths.
    """

    if not paths:
        # by default, get version for myself
        pwd   = os.path.dirname(__file__)
        root  = "%s/.." % pwd
        paths = [root, pwd]

    if not isinstance(paths, list):
        paths = [paths]

    version_short  = None
    version_base   = None
    version_branch = None
    version_tag    = None
    version_detail = None
    err            = ''

    # if in any of the paths a VERSION file exists, we use the detailed version
    # in there.
    for path in paths:

        try:
            version_path = "%s/VERSION" % path

            with ru_open(version_path) as f:
                data    = f.read()
                lines   = data.split('\n')
                lines   = [line.strip() for line in lines]


                # make sure we have a valid version file
                assert len(lines) > 1

                version_short = lines[0]

                if len(lines) > 2:
                    version_base   = lines[1]
                    version_branch = lines[2]
                    version_tag    = lines[3]
                    version_detail = lines[4]

        except Exception as e:
            # ignore missing VERSION file, but keep error message
            err += '%s\n' % repr(e)

    # check if any one worked ok
    if not version_short:
        raise RuntimeError("Cannot determine version from %s (%s)"
                          % (paths, err.strip()))

    if version_detail is None:
        version_detail = version_short

    return (version_short, version_base, version_branch,
            version_tag, version_detail)


# ------------------------------------------------------------------------------

