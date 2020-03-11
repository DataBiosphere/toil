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

"""Tool for reporting on job status.
"""

# python 2/3 compatibility imports
from __future__ import absolute_import
from __future__ import print_function
from six.moves import xrange
from past.builtins import map
from functools import reduce

# standard library
import logging
import sys
import os

# toil imports
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.jobStores.abstractJobStore import NoSuchJobStoreException, NoSuchFileException
from toil.job import JobException
from toil.statsAndLogging import StatsAndLogging
from toil.version import version

logger = logging.getLogger(__name__)

class ToilStatus():
    """Tool for reporting on job status."""
    def __init__(self, jobStoreName, specifiedJobs=None):
        self.jobStoreName = jobStoreName
        self.jobStore = Toil.resumeJobStore(jobStoreName)

        if specifiedJobs is None:
            rootJob = self.fetchRootJob()
            logger.info('Traversing the job graph gathering jobs. This may take a couple of minutes.')
            self.jobsToReport = self.traverseJobGraph(rootJob)
        else:
            self.jobsToReport = self.fetchUserJobs(specifiedJobs)

    def print_dot_chart(self):
        """Print a dot output graph representing the workflow."""
        print("digraph toil_graph {")
        print("# This graph was created from job-store: %s" % self.jobStoreName)

        # Make job IDs to node names map
        jobsToNodeNames = dict(enumerate(map(lambda job: job.jobStoreID, self.jobsToReport)))

        # Print the nodes
        for job in set(self.jobsToReport):
            print('%s [label="%s %s"];' % (
                jobsToNodeNames[job.jobStoreID], job.jobName, job.jobStoreID))

        # Print the edges
        for job in set(self.jobsToReport):
            for level, jobList  in enumerate(job.stack):
                for childJob in jobList:
                    # Check, b/c successor may be finished / not in the set of jobs
                    if childJob.jobStoreID in jobsToNodeNames:
                        print('%s -> %s [label="%i"];' % (
                            jobsToNodeNames[job.jobStoreID],
                            jobsToNodeNames[childJob.jobStoreID], level))
        print("}")

    def printJobLog(self):
        """Takes a list of jobs, finds their log files, and prints them to the terminal."""
        for job in self.jobsToReport:
            if job.logJobStoreFileID is not None:
                # TODO: This looks intended to be machine-readable, but the format is
                # unspecified and no escaping is done. But keep these tags around.
                msg = "LOG_FILE_OF_JOB:%s LOG:" % job
                with job.getLogFileHandle(self.jobStore) as fH:
                    msg += StatsAndLogging.formatLogStream(fH)
                print(msg)
            else:
                print("LOG_FILE_OF_JOB:%s LOG: Job has no log file" % job)

    def printJobChildren(self):
        """Takes a list of jobs, and prints their successors."""
        for job in self.jobsToReport:
            children = "CHILDREN_OF_JOB:%s " % job
            for level, jobList in enumerate(job.stack):
                for childJob in jobList:
                    children += "\t(CHILD_JOB:%s,PRECEDENCE:%i)" % (childJob, level)
            print(children)

    def printAggregateJobStats(self, properties, childNumber):
        """Prints a job's ID, log file, remaining tries, and other properties."""
        for job in self.jobsToReport:
            lf = lambda x: "%s:%s" % (x, str(x in properties))
            print("\t".join(("JOB:%s" % job,
                             "LOG_FILE:%s" % job.logJobStoreFileID,
                             "TRYS_REMAINING:%i" % job.remainingRetryCount,
                             "CHILD_NUMBER:%s" % childNumber,
                             lf("READY_TO_RUN"), lf("IS_ZOMBIE"),
                             lf("HAS_SERVICES"), lf("IS_SERVICE"))))

    def report_on_jobs(self):
        """
        Gathers information about jobs such as its child jobs and status.

        :returns jobStats: Pairings of a useful category and a list of jobs which fall into it.
        :rtype dict:
        """
        hasChildren = []
        readyToRun = []
        zombies = []
        hasLogFile = []
        hasServices = []
        services = []
        properties = set()

        for job in self.jobsToReport:
            if job.logJobStoreFileID is not None:
                hasLogFile.append(job)

            childNumber = reduce(lambda x, y: x + y, map(len, job.stack) + [0])
            if childNumber > 0:  # Total number of successors > 0
                hasChildren.append(job)
                properties.add("HAS_CHILDREN")
            elif job.command is not None:
                # Job has no children and a command to run. Indicates job could be run.
                readyToRun.append(job)
                properties.add("READY_TO_RUN")
            else:
                # Job has no successors and no command, so is a zombie job.
                zombies.append(job)
                properties.add("IS_ZOMBIE")
            if job.services:
                hasServices.append(job)
                properties.add("HAS_SERVICES")
            if job.startJobStoreID or job.terminateJobStoreID or job.errorJobStoreID:
                # These attributes are only set in service jobs
                services.append(job)
                properties.add("IS_SERVICE")

        jobStats = {'hasChildren': hasChildren,
                    'readyToRun': readyToRun,
                    'zombies': zombies,
                    'hasServices': hasServices,
                    'services': services,
                    'hasLogFile': hasLogFile,
                    'properties': properties,
                    'childNumber': childNumber}
        return jobStats

    @staticmethod
    def getPIDStatus(jobStoreName):
        """
        Determine the status of a process with a particular pid.

        Checks to see if a process exists or not.

        :return: A string indicating the status of the PID of the workflow as stored in the jobstore.
        :rtype: str
        """
        try:
            jobstore = Toil.resumeJobStore(jobStoreName)
        except NoSuchJobStoreException:
            return 'QUEUED'
        except NoSuchFileException:
            return 'QUEUED'

        try:
            with jobstore.readSharedFileStream('pid.log') as pidFile:
                pid = int(pidFile.read())
                try:
                    os.kill(pid, 0)  # Does not kill process when 0 is passed.
                except OSError:  # Process not found, must be done.
                    return 'COMPLETED'
                else:
                    return 'RUNNING'
        except NoSuchFileException:
            pass
        return 'QUEUED'

    @staticmethod
    def getStatus(jobStoreName):
        """
        Determine the status of a workflow.

        If the jobstore does not exist, this returns 'QUEUED', assuming it has not been created yet.

        Checks for the existence of files created in the toil.Leader.run(). In toil.Leader.run(), if a workflow completes
        with failed jobs, 'failed.log' is created, otherwise 'succeeded.log' is written. If neither of these exist,
        the leader is still running jobs.

        :return: A string indicating the status of the workflow. ['COMPLETED', 'RUNNING', 'ERROR', 'QUEUED']
        :rtype: str
        """
        try:
            jobstore = Toil.resumeJobStore(jobStoreName)
        except NoSuchJobStoreException:
            return 'QUEUED'
        except NoSuchFileException:
            return 'QUEUED'

        try:
            with jobstore.readSharedFileStream('succeeded.log') as successful:
                pass
            return 'COMPLETED'
        except NoSuchFileException:
            try:
                with jobstore.readSharedFileStream('failed.log') as failed:
                    pass
                return 'ERROR'
            except NoSuchFileException:
                pass
        return 'RUNNING'

    def fetchRootJob(self):
        """
        Fetches the root job from the jobStore that provides context for all other jobs.

        Exactly the same as the jobStore.loadRootJob() function, but with a different
        exit message if the root job is not found (indicating the workflow ran successfully
        to completion and certain stats cannot be gathered from it meaningfully such
        as which jobs are left to run).

        :raises JobException: if the root job does not exist.
        """
        try:
            return self.jobStore.loadRootJob()
        except JobException:
            print('Root job is absent. The workflow has may have completed successfully.', file=sys.stderr)
            raise

    def fetchUserJobs(self, jobs):
        """
        Takes a user input array of jobs, verifies that they are in the jobStore
        and returns the array of jobsToReport.

        :param list jobs: A list of jobs to be verified.
        :returns jobsToReport: A list of jobs which are verified to be in the jobStore.
        """
        jobsToReport = []
        for jobID in jobs:
            try:
                jobsToReport.append(self.jobStore.load(jobID))
            except JobException:
                print('The job %s could not be found.' % jobID, file=sys.stderr)
                raise
        return jobsToReport

    def traverseJobGraph(self, rootJob, jobsToReport=None, foundJobStoreIDs=None):
        """
        Find all current jobs in the jobStore and return them as an Array.

        :param jobNode rootJob: The root job of the workflow.
        :param list jobsToReport: A list of jobNodes to be added to and returned.
        :param set foundJobStoreIDs: A set of jobStoreIDs used to keep track of jobStoreIDs encountered in traversal.
        :returns jobsToReport: The list of jobs currently in the job graph.
        """
        if jobsToReport is None:
            jobsToReport = []

        if foundJobStoreIDs is None:
            foundJobStoreIDs = set()

        if rootJob.jobStoreID in foundJobStoreIDs:
            return jobsToReport

        foundJobStoreIDs.add(rootJob.jobStoreID)
        jobsToReport.append(rootJob)
        # Traverse jobs in stack
        for jobs in rootJob.stack:
            for successorJobStoreID in [x.jobStoreID for x in jobs]:
                if successorJobStoreID not in foundJobStoreIDs and self.jobStore.exists(successorJobStoreID):
                    self.traverseJobGraph(self.jobStore.load(successorJobStoreID), jobsToReport, foundJobStoreIDs)

        # Traverse service jobs
        for jobs in rootJob.services:
            for serviceJobStoreID in [x.jobStoreID for x in jobs]:
                if self.jobStore.exists(serviceJobStoreID):
                    if serviceJobStoreID in foundJobStoreIDs:
                        raise RuntimeError('Service job was unexpectedly found while traversing ')
                    foundJobStoreIDs.add(serviceJobStoreID)
                    jobsToReport.append(self.jobStore.load(serviceJobStoreID))

        return jobsToReport


def main():
    """Reports the state of a Toil workflow."""
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

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    config = Config()
    config.setOptions(options)

    try:
        status = ToilStatus(config.jobStore, options.jobs)
    except NoSuchJobStoreException:
        print('No job store found.')
        return
    except JobException:  # Workflow likely complete, user informed in ToilStatus()
        return

    jobStats = status.report_on_jobs()

    # Info to be reported.
    hasChildren = jobStats['hasChildren']
    readyToRun = jobStats['readyToRun']
    zombies = jobStats['zombies']
    hasServices = jobStats['hasServices']
    services = jobStats['services']
    hasLogFile = jobStats['hasLogFile']
    properties = jobStats['properties']
    childNumber = jobStats['childNumber']

    if options.printPerJobStats:
        status.printAggregateJobStats(properties, childNumber)
    if options.printLogs:
        status.printJobLog()
    if options.printChildren:
        status.printJobChildren()
    if options.printDot:
        status.print_dot_chart()
    if options.stats:
        print('Of the %i jobs considered, '
           'there are %i jobs with children, '
           '%i jobs ready to run, '
           '%i zombie jobs, '
           '%i jobs with services, '
           '%i services, '
           'and %i jobs with log files currently in %s.' %
            (len(status.jobsToReport), len(hasChildren), len(readyToRun), len(zombies),
             len(hasServices), len(services), len(hasLogFile), status.jobStore))

    if len(status.jobsToReport) > 0 and options.failIfNotComplete:
        # Upon workflow completion, all jobs will have been removed from job store
        exit(1)
