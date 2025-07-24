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
from pathlib import Path

from toil.job import Job, Promise
from toil.test.src.jobTest import fn1Test


class TestJobEncapsulation:
    """Tests testing the EncapsulationJob class."""

    def testEncapsulation(self, tmp_path: Path) -> None:
        """
        Tests the Job.encapsulation method, which uses the EncapsulationJob
        class.
        """
        # Temporary file
        rootDir = tmp_path / "out"
        rootDir.mkdir()
        outFile = rootDir / "out"
        # Encapsulate a job graph
        a = Job.wrapJobFn(encapsulatedJobFn, "A", outFile, name="a").encapsulate(
            name="a-encap"
        )
        # Now add children/follow to the encapsulated graph
        d = Job.wrapFn(fn1Test, a.rv(), outFile, name="d")
        e = Job.wrapFn(fn1Test, d.rv(), outFile, name="e")
        a.addChild(d)
        a.addFollowOn(e)
        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        # Run the workflow, the return value being the number of failed jobs
        Job.Runner.startToil(a, options)
        # Check output
        assert outFile.read_text() == "ABCDE"

    def testAddChildEncapsulate(self) -> None:
        """
        Make sure that the encapsulate child does not have two parents
        with unique roots.
        """
        # Temporary file
        a = Job.wrapFn(noOp)
        b = Job.wrapFn(noOp)
        a.addChild(b).encapsulate()
        assert len(a.getRootJobs()) == 1


def noOp() -> None:
    pass


def encapsulatedJobFn(job: Job, string: str, outFile: Path) -> Promise:
    a = job.addChildFn(fn1Test, string, outFile, name="inner-a")
    b = a.addFollowOnFn(fn1Test, a.rv(), outFile, name="inner-b")
    return b.rv()
