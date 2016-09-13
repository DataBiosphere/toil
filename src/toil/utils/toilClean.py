# Copyright (C) 2015-2016 Regents of the University of California
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
Delete the job store used by a previous Toil workflow invocation
"""
from __future__ import absolute_import
import logging

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    parser = getBasicOptionParser()
    parser.add_argument("jobStore", type=str,
                        help="The location of the job store to delete. " + jobStoreLocatorHelp)
    parser.add_argument("--version", action='version', version=version)
    config = Config()
    config.setOptions(parseBasicOptions(parser))
    logger.info("Attempting to delete the job store")
    jobStore = Toil.getJobStore(config.jobStore)
    jobStore.destroy()
    logger.info("Successfully deleted the job store")
