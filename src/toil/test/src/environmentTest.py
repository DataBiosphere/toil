# Copyright (C) 2015-2024 Regents of the University of California
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
import logging
import os
import time
from argparse import Namespace
from threading import Thread
from unittest.mock import patch

from toil.common import Toil
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.test import ToilTest

logger = logging.getLogger(__name__)


class EnvironmentTest(ToilTest):
    """
    Test to make sure that Toil's environment variable save and restore system
    (environment.pickle) works.

    The environment should be captured once at the start of the workflow and
    should be sent through based on that, not base don the leader's current
    environment when the job is launched.
    """

    def test_environment(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "DEBUG"
        options.retryCount = 0

        main(options)

    def test_scheduler_job_variables_stay_the_workers(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "DEBUG"
        options.retryCount = 0

        with patch.dict(os.environ):
            run_with_scheduler_job(options)


def signal_leader(job):
    """
    Make a file in the file store that the leader can see.
    """
    with job.fileStore.jobStore.write_shared_file_stream(
        "jobstarted.txt", encoding="utf-8"
    ) as stream:
        stream.write("Job has run")


def check_environment(job, try_name: str):
    """
    Fail if the test environment is wrong.
    """

    job.fileStore.log_to_leader(f"Try {try_name} checking environment")
    value = os.environ["MAGIC_ENV_VAR_123"]
    job.fileStore.log_to_leader(f"Try {try_name} got: {value}")
    if value != "Value1":
        raise RuntimeError("Environment variable is wrong!")


def wait_a_bit(job):
    """
    Toil job that waits.
    """
    time.sleep(10)


def check_environment_repeatedly(job):
    """
    Toil job that checks the environment, waits, and checks it again, as
    separate invocations.
    """

    signal = job.addChildJobFn(signal_leader)
    check1 = signal.addFollowOnJobFn(check_environment, "try1")
    waiter = check1.addFollowOnJobFn(wait_a_bit)
    check2 = waiter.addFollowOnJobFn(check_environment, "try2")
    # Add another one to make sure we don't chain
    check3 = waiter.addFollowOnJobFn(check_environment, "try3")


def change_environment_once_started(jobStore, name: str, value: str) -> None:
    """
    Once a job has signalled that the workflow has started, set an environment
    variable in the leader.
    """
    while True:
        time.sleep(5)
        try:
            with jobStore.read_shared_file_stream(
                "jobstarted.txt", encoding="utf-8"
            ) as stream:
                logger.info("Got signal from job: %s", stream.read().strip())
                break
        except NoSuchFileException:
            pass
    logger.info("Changing environment variable %s", name)
    os.environ[name] = value


def check_scheduler_job(job):
    """
    Fail if the job sees the leader's scheduler job ID instead of its own.
    """
    value = os.environ.get("SLURM_JOB_ID")
    job.fileStore.log_to_leader(f"Got SLURM_JOB_ID: {value}")
    if value != "worker-allocation":
        raise RuntimeError(f"SLURM_JOB_ID is {value!r}, not the worker's own")


def check_scheduler_job_after_start(job):
    """
    Check the scheduler job ID in jobs started after the leader's environment
    changed, as separate invocations.
    """
    waiter = job.addChildJobFn(signal_leader).addFollowOnJobFn(wait_a_bit)
    waiter.addFollowOnJobFn(check_scheduler_job)
    waiter.addFollowOnJobFn(check_scheduler_job)


def run_with_scheduler_job(options: Namespace) -> None:
    """
    Run a workflow whose leader starts in one scheduler job and whose later
    workers are started in another.
    """
    os.environ["SLURM_JOB_ID"] = "leader-allocation"
    with Toil(options) as toil:
        changer_thread = Thread(
            target=change_environment_once_started,
            args=(toil._jobStore, "SLURM_JOB_ID", "worker-allocation"),
        )
        changer_thread.start()
        toil.start(Job.wrapJobFn(check_scheduler_job_after_start))


def main(options: Namespace | None = None):
    """
    Run the actual workflow with the given options.
    """
    if not options:
        # deal with command line arguments
        parser = Job.Runner.getDefaultArgumentParser()
        options = parser.parse_args()
        logging.basicConfig()

    # Set something that should be seen by Toil jobs
    os.environ["MAGIC_ENV_VAR_123"] = "Value1"

    with Toil(options) as toil:

        # Get a the job store so we can use shared files.
        jobStore = toil._jobStore

        changer_thread = Thread(
            target=change_environment_once_started,
            args=(jobStore, "MAGIC_ENV_VAR_123", "Value2"),
        )
        changer_thread.start()

        toil.start(Job.wrapJobFn(check_environment_repeatedly))


if __name__ == "__main__":
    main()
