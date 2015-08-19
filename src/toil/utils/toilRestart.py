# Copyright 2015 Benedict Paten (benedictpaten@gmail.com)
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

"""A script to setup and run a hierarchical run of cluster jobs.
"""

from __future__ import absolute_import
import sys
from optparse import OptionParser

from toil.leader import mainLoop
from toil.common import addOptions, setupToil
from toil.lib.bioio import setLoggingFromOptions
from toil.job import Job
import logging
logger = logging.getLogger( __name__ )

def main():
    """Restarts a toil workflow.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = OptionParser()
    addOptions(parser)
    
    options, args = parser.parse_args()
    
    if len(args) != 0:
        parser.error("Unrecognised input arguments: %s" % " ".join(args))
        
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only toil may be specified as argument
    if len(args) == 1: #Allow toil directory as arg
        options.jobStore = args[0]
        
    ##########################################
    #Now run the toil construction/leader
    ##########################################  
        
    setLoggingFromOptions(options)
    options.restart = True
    with setupToil(options) as (config, batchSystem, jobStore):
        jobStore.clean()
        return mainLoop(config, batchSystem, jobStore, Job._loadRootJob(jobStore))
    
def _test():
    import doctest      
    return doctest.testmod()
