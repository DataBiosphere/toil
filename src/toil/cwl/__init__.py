# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version

from toil.version import cwltool_version


@lru_cache(maxsize=None)
def check_cwltool_version() -> None:
    """
    Check if the installed cwltool version matches Toil's expected version.

    A warning is printed to standard error if the versions differ. We do not
    assume that logging is set up already. Safe to call repeatedly; only one
    warning will be printed.
    """

    try:
        # Setuptools 66+ will raise this if any package on the system has a version that isn't PEP440.
        # See https://github.com/pypa/setuptools/issues/3772
        from setuptools.extern.packaging.version import InvalidVersion  # type: ignore
    except ImportError:
        # It's not clear that this exception is really part fo the public API, so fake it.
        class InvalidVersion(Exception):  # type: ignore
            pass

    try:
        installed_version = version("cwltool")

        if installed_version != cwltool_version:
            sys.stderr.write(
                f"WARNING: You are using cwltool version {installed_version}, which is "
                f"not the version Toil is tested against. To install the correct cwltool "
                f"for Toil, do:\n\n\tpip install cwltool=={cwltool_version}\n\n"
            )
    except InvalidVersion:
        # cwltool installation can't be inspected
        pass
    except PackageNotFoundError:
        # cwltool is not installed
        pass
