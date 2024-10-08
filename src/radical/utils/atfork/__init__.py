# Copyright 2009 Google Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# Licensed to the PSF under a Contributor Agreement.
#
# Author: Gregory P. Smith <greg@krypto.org>

# flake8: noqa: F401

from .atfork       import monkeypatch_os_fork_functions, atfork
from .stdlib_fixer import fix_logging_module

# Python 3.13+ has a fix for the logging locks
import sys as _sys
_py_version = float("%d.%d" % _sys.version_info[:2])
if _py_version < 3.13:
     fix_logging_module()

monkeypatch_os_fork_functions()

