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
Reports the state of a Toil workflow
"""

# python 2/3 compatibility imports
from __future__ import absolute_import
from __future__ import print_function
# TODO: change functions to support python 3 str and map
# from builtins import map
# from builtins import str

# standard library
import logging
import sys

# toil imports
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.job import JobException
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    """Reports the state of toil.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################
    
    parser = getBasicOptionParser()
    
    parser.add_argument("jobStore", type=str,
                        help="The location of a job store that holds the information about the "
                             "workflow whose status is to be reported on." + jobStoreLocatorHelp)
    
    parser.add_argument("--failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if toil jobs not all completed. default=%(default)s",
                      default=False)
    
    parser.add_argument("--noAggStats", dest="stats", action="store_false",
                      help="Do not print overall, aggregate status of workflow.",
                      default=True)
    
    parser.add_argument("--printDot", action="store_true",
                      help="Print dot formatted description of the graph. If using --jobs will "
                      "restrict to subgraph including only those jobs. default=%(default)s",
                      default=False)
    
    parser.add_argument("--jobs", nargs='+',
                      help="Restrict reporting to the following jobs (allows subsetting of the report).",
                      default=None)
    
    parser.add_argument("--printPerJobStats", action="store_true",
                      help="Print info about each job. default=%(default)s",
                      default=False)
    
    parser.add_argument("--printLogs", action="store_true",
                      help="Print the log files of jobs (if they exist). default=%(default)s",
                      default=False)
    
    parser.add_argument("--printChildren", action="store_true",
                      help="Print children of each job. default=%(default)s",
                      default=False)
    
    parser.add_argument("--version", action='version', version=version)
    
    options = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    ##########################################
    # Survey the status of the job and report.
    ##########################################  
    
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)

    ##########################################
    # Gather the jobs to report
    ########################################## 
    
    jobsToReport = [] # The jobs to report on

    if options.jobs == None:
        # Report on all jobs in the workflow
        try:
            rootJob = jobStore.loadRootJob()
        except JobException:
            print('The root job of the job store is absent, the workflow may have completed successfully.',
                  file=sys.stderr)
            sys.exit(0)

        def traverseGraph(jobGraph):
            foundJobStoreIDs = set()
            def inner(jobGraph):
                if jobGraph.jobStoreID in foundJobStoreIDs:
                    return
                foundJobStoreIDs.add(jobGraph.jobStoreID)
                jobsToReport.append(jobGraph)
                # Traverse jobs in stack
                for jobs in jobGraph.stack:
                    for successorJobStoreID in [x.jobStoreID for x in jobs]:
                        if (successorJobStoreID not in foundJobStoreIDs and jobStore.exists(successorJobStoreID)):
                            inner(jobStore.load(successorJobStoreID))
    
                # Traverse service jobs
                for jobs in jobGraph.services:
                    for serviceJobStoreID in [x.jobStoreID for x in jobs]:
                        if jobStore.exists(serviceJobStoreID):
                            assert serviceJobStoreID not in foundJobStoreIDs
                            foundJobStoreIDs.add(serviceJobStoreID)
                            jobsToReport.append(jobStore.load(serviceJobStoreID))
            inner(jobGraph)
    
        logger.info('Traversing the job graph to find the jobs. This may take a couple minutes.')
        traverseGraph(rootJob)

    else:
        for jobID in options.jobs:
            try:
                jobsToReport.append(jobStore.load(jobID))
            except JobException:
                print('The job %s could not be found.' % jobID,
                      file=sys.stderr)
                sys.exit(0)
    
    ##########################################
    # Report on the jobs
    ########################################## 
    
    hasChildren = []
    readyToRun = []
    zombies = []
    hasLogFile = []
    hasServices = []
    services = []
    
    
    for job in jobsToReport:
        properties = set()
        if job.logJobStoreFileID is not None:
            hasLogFile.append(job)
        
        childNumber =  reduce(lambda x, y : x + y, map(len, job.stack) + [0])
        if childNumber > 0: # Total number of successors > 0
            hasChildren.append(job)
            properties.add("HAS_CHILDREN")
            
        elif job.command != None:
            #if job.remainingRetryCount == 0 and job.logJobStoreFileID != None 
            # The job has no children and has a command to run. This indicates that the job could be run.
            readyToRun.append(job)
            properties.add("READY_TO_RUN")
        
        else:
            # The job has no successors and no command, so is a zombie job
            zombies.append(job)
            properties.add("IS_ZOMBIE")
            
        if job.services:
            hasServices.append(job)
            properties.append("HAS_SERVICES")
            
        if job.startJobStoreID or job.terminateJobStoreID or job.errorJobStoreID:
            # these attributes are only set in service jobs
            services.append(job)
            properties.append("IS_SERVICE")
            
        if options.printPerJobStats:
            # Print aggregate stats about the jobs
            lf = lambda x : "%s:%s" % (x, str(x in properties))
            print("\t".join(("JOB:%s" % job, "LOG_FILE:%s" % job.logJobStoreFileID, "TRYS_REMAINING:%i" % job.remainingRetryCount, 
                             "CHILD_NUMBER:%s" % childNumber, lf("READY_TO_RUN"), lf("IS_ZOMBIE"), lf("HAS_SERVICES"), lf("IS_SERVICE"))))

        if options.printLogs:
            # Print any log file
            if job.logJobStoreFileID is not None:
                msg = "LOG_FILE_OF_JOB:%s LOG: =======>\n" % job
                with job.getLogFileHandle(jobStore) as fH:
                    msg += fH.read()
                msg += "<========="
            else:
                msg = "LOG_FILE_OF_JOB:%s LOG: Job has no log file" % job
            print(msg)
        
        if options.printChildren:
            # Print the successors of the job
            children = "CHILDREN_OF_JOB:%s " % job
            for jobList, level in zip(job.stack, xrange(len(job.stack))):
                for childJob in jobList:
                    children += "\t(CHILD_JOB:%s,PRECEDENCE:%i)" % (childJob, level)
            print(children)
        
    if options.printDot:
        # Print a dot output graph representing the workflow
        print("digraph toil_graph {")
        print("# This graph was created from job-store: %s" % config.jobStore)
        
        # Make job IDs to node names map
        jobsToNodeNames = dict(zip(map(lambda job : job.jobStoreID, jobsToReport), range(len(jobsToReport))))
        
        # Print the nodes
        for job in set(jobsToReport):
            print('%s [label="%s %s"];' % (jobsToNodeNames[job.jobStoreID], job.jobName, job.jobStoreID))
        
        # Print the edges
        for job in set(jobsToReport):
            for jobList, level in zip(job.stack, xrange(len(job.stack))):
                for childJob in jobList: 
                    if childJob.jobStoreID in jobsToNodeNames: # Check, because successor may be finished / not in the set of considered jobs
                        print('%s -> %s [label="%i"];' % (jobsToNodeNames[job.jobStoreID], jobsToNodeNames[childJob.jobStoreID], level))
        
        print("}")
    
    if options.stats:
        # Print aggregate statistics
        print('Of the %i jobs considered, there are %i jobs with children, %i jobs ready to run, %i zombie jobs, %i jobs with services, %i services, '
                    'and %i jobs with log files currently in %s.' %
                    (len(jobsToReport), len(hasChildren), len(readyToRun), len(zombies), len(hasServices), len(services), len(hasLogFile), config.jobStore))

    if len(jobsToReport) > 0 and options.failIfNotComplete:
        exit(1) # when the workflow is complete, all jobs will have been removed from job store

def _test():
    import doctest      
    return doctest.testmod()
