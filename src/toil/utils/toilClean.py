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
"""Delete a job store used by a previous Toil workflow invocation."""
import logging

from toil.common import Toil, parser_with_common_options
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True, prog="toil clean")

    options = parser.parse_args()
    set_logging_from_options(options)
    try:
        jobstore = Toil.getJobStore(options.jobStore)
        try:
            # TODO: We need to resume successfully to delete encrypted AWS job
            # stores supposedly. We should avoid needing this.
            # See https://github.com/DataBiosphere/toil/issues/1041
            jobstore.resume()
        except NoSuchJobStoreException:
            logger.warning("Job store does not appear to exist. Deleting anyway.")
        except:
            logger.exception("Job store appears to be in a bad state. Deleting anyway.")
        jobstore.destroy()
        logger.info(f"Successfully deleted the job store: {options.jobStore}")
    except:
        logger.critical(f"Failed to delete the job store: {options.jobStore}")
        raise
