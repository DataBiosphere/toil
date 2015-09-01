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

    parser = getBasicOptionParser("usage: %prog clean [--jobStore] JOB_STORE", "%prog "+version)

    parser.add_option("--jobStore", dest="jobStore",
                      help="Job store path. Can also be specified as the single argument to the script.\
                       default=%default", default=os.path.abspath("./toil"))

    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")

    # the JobStore may also be passed as an argument directly to the script
    assert len(args) <= 1
    if len(args) == 1:
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
