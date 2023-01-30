"""A set of test cases for toilwdl.py"""
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
from pathlib import Path

from toil.lib.resources import glob
from toil.test import slow
from toil.version import python

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture
def workflow_debug_jobstore(tmp_path: Path) -> str:
    jobStorePath = str(tmp_path / "toilWorkflowRun")
    subprocess.check_call(
        [
            python,
            os.path.abspath("src/toil/test/utils/ABCWorkflowDebug/debugWorkflow.py"),
            jobStorePath,
        ]
    )
    return jobStorePath


@slow
def testJobStoreContents(workflow_debug_jobstore: str):
    """
    Test toilDebugFile.printContentsOfJobStore().

    Runs a workflow that imports 'B.txt' and 'mkFile.py' into the
    jobStore.  'A.txt', 'C.txt', 'ABC.txt' are then created.  This checks to
    make sure these contents are found in the jobStore and printed.
    """
    jobStoreDir = workflow_debug_jobstore
    contents = ["A.txt", "B.txt", "C.txt", "ABC.txt", "mkFile.py"]

    subprocess.check_call(
        [
            python,
            os.path.abspath("src/toil/utils/toilDebugFile.py"),
            jobStoreDir,
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


def fetchFiles(symLink, jobStoreDir: str, outputDir):
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
        os.path.abspath("src/toil/utils/toilDebugFile.py"),
        jobStoreDir,
        "--fetch",
        "*A.txt",
        "*B.txt",
        "*C.txt",
        "*ABC.txt",
        "*mkFile.py",
        "--localFilePath=" + outputDir,
        "--useSymlinks=" + str(symLink),
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
def testFetchJobStoreFiles(tmp_path: Path, workflow_debug_jobstore: str) -> None:
    """Test toilDebugFile.fetchJobStoreFiles() without using symlinks."""
    outputDir = tmp_path / "testoutput"
    outputDir.mkdir()
    fetchFiles(
        symLink=False, jobStoreDir=workflow_debug_jobstore, outputDir=str(outputDir)
    )


# expected run time = 4s
def testFetchJobStoreFilesWSymlinks(
    tmp_path: Path, workflow_debug_jobstore: str
) -> None:
    """Test toilDebugFile.fetchJobStoreFiles() using symlinks."""
    outputDir = tmp_path / "testoutput"
    outputDir.mkdir()
    fetchFiles(
        symLink=True, jobStoreDir=workflow_debug_jobstore, outputDir=str(outputDir)
    )
