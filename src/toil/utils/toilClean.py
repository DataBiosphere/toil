# Copyright (C) 2015 UCSC Computational Genomics Lab
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
"""Removes the JobStore from a toil run.
"""
from __future__ import absolute_import
import logging
import os
import sys

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore
from toil.jobStores.abstractJobStore import JobStoreCreationException
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    """Removes the JobStore from a toil run.
    """

    ##########################################
    #Construct the arguments.
    ##########################################

    parser = getBasicOptionParser()
    parser.add_argument("jobStore", type=str,
                      help=("Store in which to place job management files \
                      and the global accessed temporary files"
                      "(If this is a file path this needs to be globally accessible "
                      "by all machines running jobs).\n"
                      "If the store already exists and restart is false an"
                      " ExistingJobStoreException exception will be thrown."))
    parser.add_argument("--version", action='version', version=version)
    options = parseBasicOptions(parser)
    logger.info("Parsed arguments")

    ##########################################
    #Survey the status of the job and report.
    ##########################################
    logger.info("Checking if we have files for toil")
    try:
        jobStore = loadJobStore(options.jobStore)
    except JobStoreCreationException:
        logger.info("The specified JobStore does not exist, it may have already been deleted")
        sys.exit(0)

    logger.info("Deleting the JobStore")
    jobStore.deleteJobStore()


def _test():
    import doctest
    return doctest.testmod()
