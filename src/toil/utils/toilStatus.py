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
from __future__ import absolute_import
from __future__ import print_function

import logging
import sys

from toil.lib.bioio import logStream
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.leader import ToilState
from toil.job import JobException
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    """Reports the state of the toil.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser()
    
    parser.add_argument("jobStore", type=str,
                        help="The location of a job store that holds the information about the "
                             "workflow whose status is to be reported on." + jobStoreLocatorHelp)
    
    parser.add_argument("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of \
                      jobs that failed. default=%(default)s",
                      default=False)
    
    parser.add_argument("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if toil jobs not all completed. default=%(default)s",
                      default=False)
    parser.add_argument("--version", action='version', version=version)
    options = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    ##########################################
    #Do some checks.
    ##########################################
    
    logger.info("Checking if we have files for Toil")
    assert options.jobStore is not None
    config = Config()
    config.setOptions(options)
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    jobStore = Toil.resumeJobStore(config.jobStore)
    try:
        rootJob = jobStore.loadRootJob()
    except JobException:
        print('The root job of the job store is absent, the workflow completed successfully.',
              file=sys.stderr)
        sys.exit(0)

    def traverseGraph(jobGraph):
        foundJobStoreIDs = set()
        totalJobs = []
        def inner(jobGraph):
            if jobGraph.jobStoreID in foundJobStoreIDs:
                return
            foundJobStoreIDs.add(jobGraph.jobStoreID)
            totalJobs.append(jobGraph)
            # Traverse jobs in stack
            for jobs in jobGraph.stack:
                for successorJobStoreID in map(lambda x: x.jobStoreID, jobs):
                    if (successorJobStoreID not in foundJobStoreIDs and jobStore.exists(successorJobStoreID)):
                        inner(jobStore.load(successorJobStoreID))

            # Traverse service jobs
            for jobs in jobGraph.services:
                for serviceJobStoreID in map(lambda x: x.jobStoreID, jobs):
                    if jobStore.exists(serviceJobStoreID):
                        assert serviceJobStoreID not in foundJobStoreIDs
                        foundJobStoreIDs.add(serviceJobStoreID)
                        totalJobs.append(jobStore.load(serviceJobStoreID))
        inner(jobGraph)
        return totalJobs

    logger.info('Traversing the job graph. This may take a couple minutes.')
    totalJobs = traverseGraph(rootJob)

    failedJobs = []
    hasChildren = []
    hasServices = []
    services = []
    currentlyRunnning = []

    for job in totalJobs:
        if job.logJobStoreFileID is not None:
            failedJobs.append(job)
        if job.stack:
            hasChildren.append(job)
        elif job.remainingRetryCount != 0 and job.logJobStoreFileID != 0 and job.command:
            # The job has no children, hasn't failed, and has a command to run. This indicates that the job is
            # likely currently running, or at least could be run.
            currentlyRunnning.append(job)
        if job.services:
            hasServices.append(job)
        if job.startJobStoreID or job.terminateJobStoreID or job.errorJobStoreID:
            # these attributes are only set in service jobs
            services.append(job)

    logger.info('There are %i unfinished jobs, %i parent jobs with children, %i jobs with services, %i services, '
                'and %i totally failed jobs currently in %s.' %
                (len(totalJobs), len(hasChildren), len(hasServices), len(services), len(failedJobs), config.jobStore))

    if currentlyRunnning:
        logger.info('These %i jobs are currently active: %s',
                    len(currentlyRunnning), ' \n'.join(map(str, currentlyRunnning)))

    if options.verbose: #Verbose currently means outputting the files that have failed.
        if failedJobs:
            msg = "Outputting logs for the %i failed jobs" % (len(failedJobs))
            msg += ": %s" % ", ".join((str(failedJob) for failedJob in failedJobs))
            for jobNode in failedJobs:
                job = jobStore.load(jobNode.jobStoreID)
                msg += "\n=========> Failed job %s \n" % jobNode
                with job.getLogFileHandle(jobStore) as fH:
                    msg += fH.read()
                msg += "<=========\n"
            print(msg)
        else:
            print('There are no failed jobs to report.', file=sys.stderr)

    if totalJobs and options.failIfNotComplete:
        exit(1) # when the workflow is complete, all jobs will have been removed from job store

def _test():
    import doctest      
    return doctest.testmod()
