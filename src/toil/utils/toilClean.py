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
"""Delete a job store used by a previous Toil workflow invocation."""
import logging

from toil.common import Toil, jobStoreLocatorHelp
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.lib.bioio import parser_with_common_options, set_logging_from_options

log = logging.getLogger(__name__)


def main():
    parser = parser_with_common_options()
    parser.add_argument("jobStore", type=str,
                        help=f"The location of the job store to delete.\n{jobStoreLocatorHelp}")

    options = parser.parse_args()
    set_logging_from_options(options)
    try:
        jobstore = Toil.getJobStore(options.jobStore)
        jobstore.resume()
        jobstore.destroy()
        log.info(f"Successfully deleted the job store: {options.jobStore}")
    except NoSuchJobStoreException:
        log.info(f"Failed to delete the job store: {options.jobStore} is non-existent.")
    except:
        log.info(f"Failed to delete the job store: {options.jobStore}")
        raise
