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

"""Kills any running jobs trees in a rogue toil.
"""
from __future__ import absolute_import
import logging
import sys

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore, loadBatchSystem
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    parser = getBasicOptionParser("usage: %prog kill [--jobStore] JOB_TREE_DIR [more options]", "%prog "+version)
    
    parser.add_option("--jobStore", dest="jobStore",
                      help="Job store path. Can also be specified as the single argument to the script.")
    
    options, args = parseBasicOptions(parser)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only toil may be specified as argument
    if len(args) == 1: #Allow toil directory as arg
        options.jobStore = args[0]
        
    logger.info("Parsed arguments")
    if options.jobStore == None:
        parser.error("Specify --jobStore")

    jobStore = loadJobStore(options.jobStore)
    
    logger.info("Starting routine to kill running jobs in the toil workflow: %s" % options.jobStore)
    ####This behaviour is now broken
    batchSystem = loadBatchSystem(jobStore.config) #This should automatically kill the existing jobs.. so we're good.
    for jobID in batchSystem.getIssuedBatchJobIDs(): #Just in case we do it again.
        batchSystem.killBatchJobs(jobID)
    logger.info("All jobs SHOULD have been killed")

def _test():
    import doctest
    return doctest.testmod()
