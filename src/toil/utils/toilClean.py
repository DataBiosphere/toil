# Copyright (C) 2015-2020 Regents of the University of California
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
"""
Delete a job store used by a previous Toil workflow invocation.
"""
import logging

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import jobStoreLocatorHelp, Config
from toil.jobStores.utils import create_jobstore
from toil.jobStores.errors import NoSuchJobStoreException
from toil.version import version

log = logging.getLogger(__name__)


def main():
    parser = getBasicOptionParser()
    parser.add_argument("jobStore", type=str,
                        help=f"The location of the job store to delete.  {jobStoreLocatorHelp}")
    parser.add_argument("--version", action='version', version=version)
    config = Config()
    config.setOptions(parseBasicOptions(parser))
    try:
        jobStore = create_jobstore(config.jobStore)
        jobStore.resume()
        jobStore.destroy()
        log.info(f"Successfully deleted the job store: {config.jobStore}")
    except NoSuchJobStoreException:
        log.info(f"Failed to delete the job store: {config.jobStore} is non-existent.")
    except:  # noqa
        log.info(f"Failed to delete the job store: {config.jobStore}")
        raise
