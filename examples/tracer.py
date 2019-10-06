#!/usr/bin/env python

# ------------------------------------------------------------------------------
#
# This example demonstrates the use of the Python code tracing facility.
#
# ------------------------------------------------------------------------------

import radical.utils as ru

ru.tracer.trace('radical')
print(ru.get_version())
ru.tracer.untrace()


