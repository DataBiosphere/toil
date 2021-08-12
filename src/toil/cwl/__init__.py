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
import logging
from pkg_resources import get_distribution, DistributionNotFound
from toil.version import cwltool_version

logger = logging.getLogger(__name__)


def check_cwltool_version() -> None:
    """
    Check if the installed cwltool version matches Toil's expected version. A
    warning is printed if the versions differ.
    """
    try:
        installed_version = get_distribution("cwltool").version

        if installed_version != cwltool_version:
            logger.warning(f"You are using cwltool version {installed_version}, which might not be compatible with "
                           f"version {cwltool_version} used by Toil. You should consider running 'pip install cwltool=="
                           f"{cwltool_version}' to match Toil's cwltool version.")
    except DistributionNotFound:
        logger.warning("cwltool is not installed.")


check_cwltool_version()
