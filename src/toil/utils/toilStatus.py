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
"""Tool for reporting on job status."""
import logging
import os
import sys
from typing import Any, Optional

from toil.bus import replay_message_bus
from toil.common import Toil, parser_with_common_options
from toil.job import JobDescription, JobException, ServiceJobDescription
from toil.jobStores.abstractJobStore import NoSuchFileException, NoSuchJobStoreException
from toil.statsAndLogging import StatsAndLogging, set_logging_from_options

logger = logging.getLogger(__name__)


class ToilStatus:
    """Tool for reporting on job status."""

    def __init__(self, jobStoreName: str, specifiedJobs: Optional[list[str]] = None):
        self.jobStoreName = jobStoreName
        self.jobStore = Toil.resumeJobStore(jobStoreName)

        if specifiedJobs is None:
            rootJob = self.fetchRootJob()
            logger.info(
                "Traversing the job graph gathering jobs. This may take a couple of minutes."
            )
            self.jobsToReport = self.traverseJobGraph(rootJob)
        else:
            self.jobsToReport = self.fetchUserJobs(specifiedJobs)

        self.message_bus_path = self.jobStore.config.write_messages

    def print_dot_chart(self) -> None:
        """Print a dot output graph representing the workflow."""
        print("digraph toil_graph {")
        print("# This graph was created from job-store: %s" % self.jobStoreName)

        # Make job IDs to node names map
        jobsToNodeNames: dict[str, str] = dict(
            map(lambda job: (str(job.jobStoreID), job.jobName), self.jobsToReport)
        )

        # Print the nodes
        for job in set(self.jobsToReport):
            print(
                '{} [label="{} {}"];'.format(
                    jobsToNodeNames[str(job.jobStoreID)], job.jobName, job.jobStoreID
                )
            )

        # Print the edges
        for job in set(self.jobsToReport):
            for level, childJob in job.successors_by_phase():
                # Check, b/c successor may be finished / not in the set of jobs
                if childJob in jobsToNodeNames:
                    print(
                        '%s -> %s [label="%i"];'
                        % (
                            jobsToNodeNames[str(job.jobStoreID)],
                            jobsToNodeNames[childJob],
                            level,
                        )
                    )
        print("}")

    def printJobLog(self) -> None:
        """Takes a list of jobs, finds their log files, and prints them to the terminal."""
        for job in self.jobsToReport:
            if job.logJobStoreFileID is not None:
                with job.getLogFileHandle(self.jobStore) as fH:
                    # TODO: This looks intended to be machine-readable, but the format is
                    #  unspecified and no escaping is done. But keep these tags around.
                    print(
                        StatsAndLogging.formatLogStream(
                            fH, stream_name=f"LOG_FILE_OF_JOB:{job} LOG:"
                        )
                    )
            else:
                print(f"LOG_FILE_OF_JOB: {job} LOG: Job has no log file")

    def printJobChildren(self) -> None:
        """Takes a list of jobs, and prints their successors."""
        for job in self.jobsToReport:
            children = "CHILDREN_OF_JOB:%s " % job
            for level, childJob in job.successors_by_phase():
                children += "\t(CHILD_JOB:%s,PRECEDENCE:%i)" % (childJob, level)
            print(children)

    def printAggregateJobStats(
        self, properties: list[set[str]], childNumber: list[int]
    ) -> None:
        """
        Prints each job's ID, log file, remaining tries, and other properties.

        :param properties: A set of string flag names for each job in self.jobsToReport.
        :param childNumber: A list of child counts for each job in self.jobsToReport.
        """
        for job, job_properties, job_child_number in zip(
            self.jobsToReport, properties, childNumber
        ):

            def lf(x: str) -> str:
                return f"{x}:{str(x in job_properties)}"

            # We use a sort of not-really-machine-readable key:value TSV format here.
            # But we only include important keys to help the humans, and flags
            # don't have a value, just a key.
            parts = [f"JOB:{job}"]
            for flag in [
                "COMPLETELY_FAILED",
                "READY_TO_RUN",
                "IS_ZOMBIE",
                "HAS_SERVICES",
                "IS_SERVICE",
            ]:
                if flag in job_properties:
                    parts.append(flag)
            if job.logJobStoreFileID:
                parts.append(f"LOG_FILE:{job.logJobStoreFileID}")
            if job.remainingTryCount > 0:
                parts.append(f"TRYS_REMAINING:{job.remainingTryCount}")
            if job_child_number > 0:
                parts.append(f"CHILD_NUMBER:{job_child_number}")

            print("\t".join(parts))

    def report_on_jobs(self) -> dict[str, Any]:
        """
        Gathers information about jobs such as its child jobs and status.

        :returns jobStats: Dict containing some lists of jobs by category, and
            some lists of job properties for each job in self.jobsToReport.
        """
        # These are lists of the matching jobs
        hasChildren = []
        readyToRun = []
        zombies = []
        hasLogFile: list[JobDescription] = []
        hasServices = []
        services: list[ServiceJobDescription] = []
        completely_failed = []

        # These are stats for jobs in self.jobsToReport
        child_number: list[int] = []
        properties: list[set[str]] = []

        # TODO: This mix of semantics is confusing and made per-job status be
        # wrong for multiple years because it was not understood. Redesign it!

        for job in self.jobsToReport:
            job_properties: set[str] = set()
            if job.logJobStoreFileID is not None:
                hasLogFile.append(job)

            job_child_number = len(list(job.allSuccessors()))
            child_number.append(job_child_number)
            if job_child_number > 0:  # Total number of successors > 0
                hasChildren.append(job)
                job_properties.add("HAS_CHILDREN")
            elif job.has_body():
                # Job has no children and a body to run. Indicates job could be run.
                readyToRun.append(job)
                job_properties.add("READY_TO_RUN")
            else:
                # Job has no successors and no command, so is a zombie job.
                zombies.append(job)
                job_properties.add("IS_ZOMBIE")
            if job.services:
                hasServices.append(job)
                job_properties.add("HAS_SERVICES")
            if isinstance(job, ServiceJobDescription):
                services.append(job)
                job_properties.add("IS_SERVICE")
            if job.remainingTryCount == 0:
                # Job is out of tries (and thus completely failed)
                job_properties.add("COMPLETELY_FAILED")
                completely_failed.append(job)
            properties.append(job_properties)

        jobStats = {
            # These are lists of the mathcing jobs
            "hasChildren": hasChildren,
            "readyToRun": readyToRun,
            "zombies": zombies,
            "hasServices": hasServices,
            "services": services,
            "hasLogFile": hasLogFile,
            "completelyFailed": completely_failed,
            # These are stats for jobs in self.jobsToReport
            "properties": properties,
            "childNumber": child_number,
        }
        return jobStats

    @staticmethod
    def getPIDStatus(jobStoreName: str) -> str:
        """
        Determine the status of a process with a particular local pid.

        Checks to see if a process exists or not.

        :return: A string indicating the status of the PID of the workflow as stored in the jobstore.
        :rtype: str
        """
        try:
            jobstore = Toil.resumeJobStore(jobStoreName)
        except NoSuchJobStoreException:
            return "QUEUED"
        except NoSuchFileException:
            return "QUEUED"

        try:
            pid = jobstore.read_leader_pid()
            try:
                os.kill(pid, 0)  # Does not kill process when 0 is passed.
            except OSError:  # Process not found, must be done.
                return "COMPLETED"
            else:
                return "RUNNING"
        except NoSuchFileException:
            pass
        return "QUEUED"

    @staticmethod
    def getStatus(jobStoreName: str) -> str:
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
            return "QUEUED"
        except NoSuchFileException:
            return "QUEUED"

        try:
            with jobstore.read_shared_file_stream("succeeded.log") as successful:
                pass
            return "COMPLETED"
        except NoSuchFileException:
            try:
                with jobstore.read_shared_file_stream("failed.log") as failed:
                    pass
                return "ERROR"
            except NoSuchFileException:
                pass
        return "RUNNING"

    def print_running_jobs(self) -> None:
        """
        Prints a list of the currently running jobs
        """

        print("\nMessage bus path: ", self.message_bus_path)
        if self.message_bus_path is not None:
            if os.path.exists(self.message_bus_path):
                all_job_statuses = replay_message_bus(self.message_bus_path)

                for job_status in all_job_statuses.values():
                    if job_status.is_running():
                        status_line = [
                            f"Job ID {job_status.job_store_id} with name {job_status.name} is running"
                        ]
                        if job_status.batch_system != "":
                            # batch system exists
                            status_line.append(
                                f" on {job_status.batch_system} as ID {job_status.external_batch_id}"
                            )
                        status_line.append(".")
                        print("".join(status_line))
            else:
                print("Message bus file is missing!")

        return None

    def fetchRootJob(self) -> JobDescription:
        """
        Fetches the root job from the jobStore that provides context for all other jobs.

        Exactly the same as the jobStore.loadRootJob() function, but with a different
        exit message if the root job is not found (indicating the workflow ran successfully
        to completion and certain stats cannot be gathered from it meaningfully such
        as which jobs are left to run).

        :raises JobException: if the root job does not exist.
        """
        try:
            return self.jobStore.load_root_job()
        except JobException as e:
            logger.info(e)
            print(
                "Root job is absent. The workflow has may have completed successfully."
            )
            raise

    def fetchUserJobs(self, jobs: list[str]) -> list[JobDescription]:
        """
        Takes a user input array of jobs, verifies that they are in the jobStore
        and returns the array of jobsToReport.

        :param list jobs: A list of jobs to be verified.
        :returns jobsToReport: A list of jobs which are verified to be in the jobStore.
        """
        jobsToReport = []
        for jobID in jobs:
            try:
                jobsToReport.append(self.jobStore.load_job(jobID))
            except JobException:
                print("The job %s could not be found." % jobID, file=sys.stderr)
                raise
        return jobsToReport

    def traverseJobGraph(
        self,
        rootJob: JobDescription,
        jobsToReport: Optional[list[JobDescription]] = None,
        foundJobStoreIDs: Optional[set[str]] = None,
    ) -> list[JobDescription]:
        """
        Find all current jobs in the jobStore and return them as an Array.

        :param rootJob: The root job of the workflow.
        :param list jobsToReport: A list of jobNodes to be added to and returned.
        :param set foundJobStoreIDs: A set of jobStoreIDs used to keep track of
            jobStoreIDs encountered in traversal.
        :returns jobsToReport: The list of jobs currently in the job graph.
        """
        if jobsToReport is None:
            jobsToReport = []

        if foundJobStoreIDs is None:
            foundJobStoreIDs = set()

        if str(rootJob.jobStoreID) in foundJobStoreIDs:
            return jobsToReport

        foundJobStoreIDs.add(str(rootJob.jobStoreID))
        jobsToReport.append(rootJob)
        # Traverse jobs in stack
        for successorJobStoreID in rootJob.allSuccessors():
            if (
                successorJobStoreID not in foundJobStoreIDs
                and self.jobStore.job_exists(successorJobStoreID)
            ):
                self.traverseJobGraph(
                    self.jobStore.load_job(successorJobStoreID),
                    jobsToReport,
                    foundJobStoreIDs,
                )

        # Traverse service jobs
        for jobs in rootJob.services:
            for serviceJobStoreID in jobs:
                if self.jobStore.job_exists(serviceJobStoreID):
                    if serviceJobStoreID in foundJobStoreIDs:
                        raise RuntimeError(
                            "Service job was unexpectedly found while traversing "
                        )
                    foundJobStoreIDs.add(serviceJobStoreID)
                    jobsToReport.append(self.jobStore.load_job(serviceJobStoreID))

        return jobsToReport


def main() -> None:
    """Reports the state of a Toil workflow."""
    parser = parser_with_common_options(prog="toil status")
    parser.add_argument(
        "--failIfNotComplete",
        action="store_true",
        help="Return exit value of 1 if toil jobs not all completed. default=%(default)s",
        default=False,
    )

    parser.add_argument(
        "--noAggStats",
        dest="stats",
        action="store_false",
        help="Do not print overall, aggregate status of workflow.",
        default=True,
    )

    parser.add_argument(
        "--dot",
        "--printDot",
        dest="print_dot",
        action="store_true",
        help="Print dot formatted description of the graph. If using --jobs will "
        "restrict to subgraph including only those jobs. default=%(default)s",
        default=False,
    )

    parser.add_argument(
        "--jobs",
        nargs="+",
        help="Restrict reporting to the following jobs (allows subsetting of the report).",
        default=None,
    )

    parser.add_argument(
        "--perJob",
        "--printPerJobStats",
        dest="print_per_job_stats",
        action="store_true",
        help="Print info about each job. default=%(default)s",
        default=False,
    )

    parser.add_argument(
        "--logs",
        "--printLogs",
        dest="print_logs",
        action="store_true",
        help="Print the log files of jobs (if they exist). default=%(default)s",
        default=False,
    )

    parser.add_argument(
        "--children",
        "--printChildren",
        dest="print_children",
        action="store_true",
        help="Print children of each job. default=%(default)s",
        default=False,
    )

    parser.add_argument(
        "--status",
        "--printStatus",
        dest="print_status",
        action="store_true",
        help="Determine which jobs are currently running and the associated batch system ID, if any",
    )

    parser.add_argument(
        "--failed",
        "--printFailed",
        dest="print_failed",
        action="store_true",
        help="List jobs which seem to have failed to run",
    )

    options = parser.parse_args()
    set_logging_from_options(options)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    try:
        status = ToilStatus(options.jobStore, options.jobs)
    except NoSuchJobStoreException:
        print(f"The job store {options.jobStore} was not found.")
        return
    except JobException:  # Workflow likely complete, user informed in ToilStatus()
        return

    jobStats = status.report_on_jobs()

    # Info to be reported.
    # These are lists of matching jobs.
    hasChildren = jobStats["hasChildren"]
    readyToRun = jobStats["readyToRun"]
    zombies = jobStats["zombies"]
    hasServices = jobStats["hasServices"]
    services = jobStats["services"]
    hasLogFile = jobStats["hasLogFile"]
    completely_failed = jobStats["completelyFailed"]
    # These are results for corresponding jobs in status.jobsToReport
    properties = jobStats["properties"]
    childNumber = jobStats["childNumber"]

    if options.print_per_job_stats:
        status.printAggregateJobStats(properties, childNumber)
    if options.print_logs:
        status.printJobLog()
    if options.print_children:
        status.printJobChildren()
    if options.print_dot:
        status.print_dot_chart()
    if options.print_failed:
        print("Failed jobs:")
        for job in completely_failed:
            print(job)
    if options.stats:
        print(
            "Of the %i jobs considered, "
            "there are "
            "%i completely failed jobs, "
            "%i jobs with children, "
            "%i jobs ready to run, "
            "%i zombie jobs, "
            "%i jobs with services, "
            "%i services, "
            "and %i jobs with log files currently in %s."
            % (
                len(status.jobsToReport),
                len(completely_failed),
                len(hasChildren),
                len(readyToRun),
                len(zombies),
                len(hasServices),
                len(services),
                len(hasLogFile),
                status.jobStore,
            )
        )
    if options.print_status:
        status.print_running_jobs()
    if len(status.jobsToReport) > 0 and options.failIfNotComplete:
        # Upon workflow completion, all jobs will have been removed from job store
        exit(1)
