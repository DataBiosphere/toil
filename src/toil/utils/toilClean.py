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
from __future__ import absolute_import
import logging
import os
import sys

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore
from toil.jobStores.abstractJobStore import JobStoreCreationException

logger = logging.getLogger( __name__ )

def main():
    """Reports the state of the toil.
    """

    ##########################################
    #Construct the arguments.
    ##########################################

    parser = getBasicOptionParser("usage: %prog [--jobStore] JOB_STORE", "%prog 0.1")

    parser.add_option("--jobStore", dest="jobStore",
                      help="Job store path. Can also be specified as the single argument to the script.\
                       default=%default", default=os.path.abspath("./toil"))

    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")

    assert len(args) <= 1 #Only toil may be specified as argument
    if len(args) == 1: #Allow toil directory as arg
        options.jobStore = args[0]

    ##########################################
    #Do some checks.
    ##########################################

    logger.info("Checking if we have files for toil")
    assert options.jobStore != None

    ##########################################
    #Survey the status of the job and report.
    ##########################################
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
