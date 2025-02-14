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
import logging
import os
import subprocess
import tempfile

from toil.lib.resources import glob
from toil.test import ToilTest, get_data, needs_wdl, slow
from toil.version import python

logger = logging.getLogger(__name__)


def workflow_debug_jobstore() -> str:
    job_store_path = os.path.join(tempfile.mkdtemp(), "toilWorkflowRun")
    subprocess.check_call(
        [
            python,
            get_data("test/utils/ABCWorkflowDebug/debugWorkflow.py"),
            job_store_path,
        ]
    )
    return job_store_path


@slow
def testJobStoreContents():
    """
    Test toilDebugFile.printContentsOfJobStore().

    Runs a workflow that imports 'B.txt' and 'mkFile.py' into the
    jobStore.  'A.txt', 'C.txt', 'ABC.txt' are then created.  This checks to
    make sure these contents are found in the jobStore and printed.
    """
    contents = ["A.txt", "B.txt", "C.txt", "ABC.txt", "mkFile.py"]

    subprocess.check_call(
        [
            python,
            get_data("utils/toilDebugFile.py"),
            workflow_debug_jobstore(),
            "--logDebug",
            "--listFilesInJobStore=True",
        ]
    )
    jobstoreFileContents = os.path.abspath("jobstore_files.txt")
    files = []
    match = 0
    with open(jobstoreFileContents) as f:
        for line in f:
            files.append(line.strip())
    for xfile in files:
        for expected_file in contents:
            if xfile.endswith(expected_file):
                match = match + 1
    logger.debug(files)
    logger.debug(contents)
    logger.debug(match)
    # C.txt will match twice (once with 'C.txt', and once with 'ABC.txt')
    assert match == 6
    os.remove(jobstoreFileContents)


def fetchFiles(symLink: bool, jobStoreDir: str, outputDir: str):
    """
    Fn for testFetchJobStoreFiles() and testFetchJobStoreFilesWSymlinks().

    Runs a workflow that imports 'B.txt' and 'mkFile.py' into the
    jobStore.  'A.txt', 'C.txt', 'ABC.txt' are then created.  This test then
    attempts to get a list of these files and copy them over into our
    output diectory from the jobStore, confirm that they are present, and
    then delete them.
    """
    contents = ["A.txt", "B.txt", "C.txt", "ABC.txt", "mkFile.py"]
    cmd = [
        python,
        get_data("utils/toilDebugFile.py"),
        jobStoreDir,
        "--fetch",
        "*A.txt",
        "*B.txt",
        "*C.txt",
        "*ABC.txt",
        "*mkFile.py",
        f"--localFilePath={outputDir}",
        f"--useSymlinks={symLink}",
    ]
    print(cmd)
    subprocess.check_call(cmd)
    for xfile in contents:
        matchingFilesFound = glob(glob_pattern="*" + xfile, directoryname=outputDir)
        assert len(matchingFilesFound) >= 1
        for fileFound in matchingFilesFound:
            assert fileFound.endswith(xfile) and os.path.exists(fileFound)
            if fileFound.endswith("-" + xfile):
                os.remove(fileFound)


# expected run time = 4s
def testFetchJobStoreFiles() -> None:
    """Test toilDebugFile.fetchJobStoreFiles() symlinks."""
    job_store_dir = workflow_debug_jobstore()
    output_dir = os.path.join(os.path.dirname(job_store_dir), "testoutput")
    os.makedirs(output_dir, exist_ok=True)
    for symlink in (True, False):
        fetchFiles(symLink=symlink, jobStoreDir=job_store_dir, outputDir=output_dir)


class DebugJobTest(ToilTest):
    """
    Test the toil debug-job command.
    """

    def _get_job_store_and_job_id(self):
        """
        Get a job store and the ID of a failing job within it.
        """

        # First make a job store.
        job_store = os.path.join(self._createTempDir(), "tree")

        logger.info("Running workflow that always fails")
        try:
            # Run an always-failing workflow
            subprocess.check_call(
                [
                    python,
                    get_data("test/docs/scripts/example_alwaysfail.py"),
                    "--retryCount=0",
                    "--logCritical",
                    "--disableProgress",
                    job_store,
                ],
                stderr=subprocess.DEVNULL,
            )
            raise RuntimeError("Failing workflow succeeded!")
        except subprocess.CalledProcessError:
            # Should fail to run
            logger.info("Task failed successfully")

        # Get the job ID.
        # TODO: This assumes a lot about the FileJobStore. Use the MessageBus instead?
        job_id = (
            "kind-explode/"
            + os.listdir(os.path.join(job_store, "jobs/kind-explode"))[0]
        )

        return job_store, job_id

    def _get_wdl_job_store_and_job_name(self):
        """
        Get a job store and the name of a failed job in it that actually wanted to use some files.
        """

        # First make a job store.
        job_store = os.path.join(self._createTempDir(), "tree")

        logger.info("Running workflow that always fails")
        # Run an always-failing workflow
        wf_result = subprocess.run(
            [
                "toil-wdl-runner",
                get_data("test/docs/scripts/example_alwaysfail_with_files.wdl"),
                "--retryCount=0",
                "--logDebug",
                "--disableProgress",
                "--jobStore",
                job_store,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
        )
        logger.debug("Always-failing workflow output: %s", wf_result.stdout)
        if wf_result.returncode == 0:
            raise RuntimeError("Failing workflow succeeded!")
        else:
            logger.info("Task failed successfully")

        # Make sure that the job store we created actually has its job store
        # root job ID file. If it doesn't, we failed during workflow setup and
        # not because of a real failing job.
        assert os.path.exists(
            os.path.join(job_store, "files/shared/rootJobStoreID")
        ), "Failed workflow still needs a root job"

        # Get a job name for a job that fails
        job_name = "WDLTaskJob"

        return job_store, job_name

    def test_run_job(self):
        """
        Make sure that we can use toil debug-job to try and run a job in-process.
        """

        job_store, job_id = self._get_job_store_and_job_id()

        logger.info("Trying to rerun job %s", job_id)

        # Rerun the job, which should fail again
        output = subprocess.check_output(
            ["toil", "debug-job", "--logDebug", job_store, job_id],
            stderr=subprocess.STDOUT,
        )
        # Even if the job fails, the attempt to run it will succeed.
        log = output.decode("utf-8")
        assert "Boom!" in log, f"Did not find the expected exception message in: {log}"

    def test_print_job_info(self):
        """
        Make sure that we can use --printJobInfo to get information on a job from a job store.
        """

        job_store, job_id = self._get_job_store_and_job_id()

        logger.info("Trying to print job info for job %s", job_id)

        # Print the job info and make sure that doesn't crash.
        subprocess.check_call(
            ["toil", "debug-job", "--logDebug", job_store, "--printJobInfo", job_id]
        )

    @needs_wdl
    def test_retrieve_task_directory(self):
        """
        Make sure that we can use --retrieveTaskDirectory to get the input files for a job.
        """

        job_store, job_name = self._get_wdl_job_store_and_job_name()

        logger.info("Trying to retrieve task dorectory for job %s", job_name)

        dest_dir = os.path.join(self._createTempDir(), "dump")

        # Print the job info and make sure that doesn't crash.
        subprocess.check_call(
            [
                "toil",
                "debug-job",
                "--logDebug",
                job_store,
                job_name,
                "--retrieveTaskDirectory",
                dest_dir,
            ]
        )

        first_file = os.path.join(
            dest_dir,
            "inside/mnt/miniwdl_task_container/work/_miniwdl_inputs/0/test.txt",
        )
        assert os.path.exists(
            first_file
        ), "Input file not found in fake container environment"
        self.assertEqual(open(first_file).read(), "These are the contents\n")
