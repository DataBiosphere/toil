"""Neutral place for exceptions, to break import cycles."""

import logging
from typing import TYPE_CHECKING, List

from toil.statsAndLogging import StatsAndLogging

if TYPE_CHECKING:
    from toil.job import JobDescription
    from toil.jobStores.abstractJobStore import AbstractJobStore

logger = logging.getLogger(__name__)


class FailedJobsException(Exception):
    def __init__(
        self,
        job_store: "AbstractJobStore",
        failed_jobs: List["JobDescription"],
        exit_code: int = 1,
    ):
        """
        Make an exception to report failed jobs.

        :param job_store: The job store with the failed jobs in it.
        :param failed_jobs: All the failed jobs.
        :param exit_code: Recommended process exit code.
        """
        self.msg = (
            f"The job store '{job_store.locator}' contains "
            f"{len(failed_jobs)} failed jobs"
        )
        self.exit_code = exit_code
        try:
            self.msg += ": %s" % ", ".join(str(failedJob) for failedJob in failed_jobs)
            for job_desc in failed_jobs:
                if job_desc.logJobStoreFileID:
                    with job_desc.getLogFileHandle(job_store) as f:
                        self.msg += "\n" + StatsAndLogging.formatLogStream(f, job_desc)
        # catch failures to prepare more complex details and only return the basics
        except Exception:
            logger.exception("Exception when compiling information about failed jobs")
        self.msg = self.msg.rstrip("\n")
        super().__init__()

        # Save fields that catchers can look at
        self.jobStoreLocator = job_store.locator
        self.numberOfFailedJobs = len(failed_jobs)

    def __str__(self) -> str:
        """Stringify the exception, including the message."""
        return self.msg
