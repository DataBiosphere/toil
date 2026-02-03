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
import signal
import time
import uuid
from pathlib import Path
from threading import Thread

import pytest

from docker.errors import ContainerError  # type: ignore[import-not-found]
from toil.common import Toil
from toil.exceptions import FailedJobsException
from toil.job import Job
from toil.lib.docker import (
    FORGO,
    RM,
    STOP,
    apiDockerCall,
    containerIsRunning,
    dockerKill,
)
from toil.test import pneeds_docker as needs_docker
from toil.test import pslow as slow

logger = logging.getLogger(__name__)


@needs_docker
@pytest.mark.docker
@pytest.mark.online
class TestDocker:
    """
    Tests dockerCall and ensures no containers are left around.
    """

    dockerTestLogLevel = "INFO"

    def testDockerClean(
        self,
        tmp_path: Path,
        caching: bool = False,
        detached: bool = True,
        rm: bool = True,
        deferParam: int | None = None,
    ) -> None:
        """
        Run the test container that creates a file in the work dir, and sleeps
        for 5 minutes.
        Ensure that the calling job gets SIGKILLed after a minute, leaving
        behind the spooky/ghost/zombie container. Ensure that the container is
        killed on batch system shutdown (through the deferParam mechanism).
        """
        # We need to test the behaviour of `deferParam` with `rm` and
        # `detached`. We do not look at the case where `rm` and `detached` are
        # both True.  This is the truth table for the different combinations at
        # the end of the test. R = Running, X = Does not exist, E = Exists but
        # not running.
        #              None     FORGO     STOP    RM
        #    rm        X         R         X      X
        # detached     X         R         E      X
        #  Neither     X         R         E      X

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        working_dir = tmp_path / "working"
        working_dir.mkdir()
        test_file = working_dir / "test.txt"

        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = self.dockerTestLogLevel
        options.workDir = str(working_dir)
        options.clean = "always"
        options.retryCount = 0  # we're expecting the job to fail so don't retry!
        options.caching = caching

        # No base64 logic since it might create a name starting with a `-`.
        container_name = uuid.uuid4().hex
        A = Job.wrapJobFn(
            _testDockerCleanFn, working_dir, detached, rm, deferParam, container_name
        )
        try:
            with Toil(options) as toil:
                toil.start(A)
        except FailedJobsException:
            # The file created by spooky_container would remain in the directory
            # and since it was created inside the container, it would have had
            # uid and gid == 0 (root) which may cause problems when docker
            # attempts to clean up the jobstore.
            file_stats = test_file.stat()
            assert file_stats.st_gid != 0
            assert file_stats.st_uid != 0

            if (rm and (deferParam != FORGO)) or deferParam == RM or deferParam is None:
                # These containers should not exist
                assert (
                    containerIsRunning(container_name) is None
                ), "Container was not removed."

            elif deferParam == STOP:
                # These containers should exist but be non-running
                assert (
                    containerIsRunning(container_name) == False
                ), "Container was not stopped."

            else:
                # These containers will be running
                assert (
                    containerIsRunning(container_name) == True
                ), "Container was not running."
        finally:
            # Clean up
            try:
                dockerKill(container_name, remove=True)
                test_file.unlink()
            except:
                pass

    def testDockerClean_CRx_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=True, deferParam=FORGO
        )

    def testDockerClean_CRx_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=True, deferParam=STOP
        )

    def testDockerClean_CRx_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=True, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_CRx_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=True, deferParam=None
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_CxD_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=True, rm=False, deferParam=FORGO
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_CxD_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=True, rm=False, deferParam=STOP
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_CxD_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=True, rm=False, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_CxD_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=True, rm=False, deferParam=None
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_Cxx_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=False, deferParam=FORGO
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_Cxx_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=False, deferParam=STOP
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_Cxx_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=False, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_Cxx_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=False, detached=False, rm=False, deferParam=None
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xRx_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=True, deferParam=FORGO
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xRx_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=True, deferParam=STOP
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xRx_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=True, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xRx_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=True, deferParam=None
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxD_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=True, rm=False, deferParam=FORGO
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxD_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=True, rm=False, deferParam=STOP
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxD_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=True, rm=False, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxD_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=True, rm=False, deferParam=None
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxx_FORGO(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=False, deferParam=FORGO
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxx_STOP(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=False, deferParam=STOP
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxx_RM(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=False, deferParam=RM
        )

    @slow
    @pytest.mark.slow
    def testDockerClean_xxx_None(self, tmp_path: Path) -> None:
        self.testDockerClean(
            tmp_path, caching=True, detached=False, rm=False, deferParam=None
        )

    def testDockerPipeChain(self, tmp_path: Path, caching: bool = False) -> None:
        r"""
        Test for piping API for dockerCall().  Using this API (activated when
        list of argument lists is given as parameters), commands a piped
        together into a chain.
        ex:  ``parameters=[ ['printf', 'x\n y\n'], ['wc', '-l'] ]`` should execute:
        ``printf 'x\n y\n' | wc -l``
        """
        workdir = tmp_path / "workdir"
        workdir.mkdir()
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = self.dockerTestLogLevel
        options.workDir = str(workdir)
        options.clean = "always"
        options.caching = caching
        A = Job.wrapJobFn(_testDockerPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        logger.info("Container pipeline result: %s", repr(rv))
        rv = rv.decode("utf-8")
        assert rv.strip() == "2"

    def testDockerPipeChainErrorDetection(
        self, tmp_path: Path, caching: bool = False
    ) -> None:
        """
        By default, executing cmd1 | cmd2 | ... | cmdN, will only return an
        error if cmdN fails.  This can lead to all manor of errors being
        silently missed.  This tests to make sure that the piping API for
        dockerCall() throws an exception if non-last commands in the chain fail.
        """
        workdir = tmp_path / "workdir"
        workdir.mkdir()
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = self.dockerTestLogLevel
        options.workDir = str(workdir)
        options.clean = "always"
        options.caching = caching
        A = Job.wrapJobFn(_testDockerPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv is True

    def testNonCachingDockerChain(self, tmp_path: Path) -> None:
        self.testDockerPipeChain(tmp_path, caching=True)

    def testNonCachingDockerChainErrorDetection(self, tmp_path: Path) -> None:
        self.testDockerPipeChainErrorDetection(tmp_path, caching=True)

    def testDockerLogs(
        self, tmp_path: Path, stream: bool = False, demux: bool = False
    ) -> None:
        """Test for the different log outputs when deatch=False."""

        working_dir = tmp_path / "working"
        working_dir.mkdir()
        script_file = working_dir / "script.sh"

        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = self.dockerTestLogLevel
        options.workDir = str(working_dir)
        options.clean = "always"
        A = Job.wrapJobFn(
            _testDockerLogsFn,
            working_dir=working_dir,
            script_file=script_file,
            stream=stream,
            demux=demux,
        )

        rv = Job.Runner.startToil(A, options)
        assert rv is True

    def testDockerLogs_Stream(self, tmp_path: Path) -> None:
        self.testDockerLogs(tmp_path, stream=True, demux=False)

    def testDockerLogs_Demux(self, tmp_path: Path) -> None:
        self.testDockerLogs(tmp_path, stream=False, demux=True)

    def testDockerLogs_Demux_Stream(self, tmp_path: Path) -> None:
        self.testDockerLogs(tmp_path, stream=True, demux=True)


def _testDockerCleanFn(
    job: Job,
    working_dir: Path,
    detached: bool = True,
    rm: bool | None = None,
    deferParam: int | None = None,
    containerName: str | None = None,
) -> None:
    """
    Test function for test docker_clean.  Runs a container with given flags and
    then dies leaving behind a zombie container.
    :param job: job
    :param working_dir: See `work_dir=` in :func:`dockerCall`
    :param detached: See `detached=` in :func:`dockerCall`
    :param rm: See `rm=` in :func:`dockerCall`
    :param deferParam: See `deferParam=` in :func:`dockerCall`
    :param containerName: See `container_name=` in :func:`dockerCall`
    """

    def killSelf() -> None:
        test_file = working_dir / "test.txt"
        # Kill the worker once we are sure the docker container is started
        while not test_file.exists():
            logger.debug("Waiting on the file created by spooky_container.")
            time.sleep(1)
        # By the time we reach here, we are sure the container is running.
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

    t = Thread(target=killSelf)
    # Make it a daemon thread so that thread failure doesn't hang tests.
    t.daemon = True
    t.start()
    apiDockerCall(
        job,
        image="quay.io/ucsc_cgl/spooky_test",
        deferParam=deferParam,
        working_dir=str(working_dir),
        containerName=containerName,
        detach=detached,
        remove=rm,
        privileged=True,
    )


def _testDockerPipeChainFn(job: Job) -> str:
    """Return the result of a simple pipe chain.  Should be 2."""
    parameters = [["printf", "x\n y\n"], ["wc", "-l"]]
    return apiDockerCall(
        job,
        image="quay.io/ucsc_cgl/ubuntu:20.04",
        parameters=parameters,
        privileged=True,
    )


def _testDockerPipeChainErrorFn(job: Job) -> bool:
    """Return True if the command exit 1 | wc -l raises a ContainerError."""
    parameters = [["exit", "1"], ["wc", "-l"]]
    try:
        apiDockerCall(job, image="quay.io/ucsc_cgl/spooky_test", parameters=parameters)
    except ContainerError:
        return True
    return False


def _testDockerLogsFn(
    job: Job,
    working_dir: Path,
    script_file: Path,
    stream: bool = False,
    demux: bool = False,
) -> bool:
    """Return True if the test succeeds. Otherwise Exception is raised."""

    # we write a script file because the redirection operator, '>&2', is wrapped
    # in quotes when passed as parameters.
    import textwrap

    bash_script = textwrap.dedent(
        """
    #!/bin/bash
    echo hello stdout ;
    echo hello stderr >&2 ;
    echo hello stdout ;
    echo hello stderr >&2 ;
    echo hello stdout ;
    echo hello stdout ;
    """
    )

    with script_file.open("w") as file:
        file.write(bash_script)

    out = apiDockerCall(
        job,
        image="quay.io/ucsc_cgl/ubuntu:20.04",
        working_dir=str(working_dir),
        parameters=[str(script_file)],
        volumes={str(working_dir): {"bind": str(working_dir), "mode": "rw"}},
        entrypoint="/bin/bash",
        stdout=True,
        stderr=True,
        stream=stream,
        demux=demux,
    )

    # we check the output length because order is not guaranteed.
    if stream:
        if demux:
            # a generator with tuples of (stdout, stderr)
            assert hasattr(out, "__iter__")
            for _ in range(6):
                stdout, stderr = next(out)
                if stdout:
                    # len('hello stdout\n') == 13
                    assert len(stdout) == 13
                elif stderr:
                    assert len(stderr) == 13
                else:
                    assert False
        else:
            # a generator with bytes
            assert hasattr(out, "__iter__")
            for _ in range(6):
                assert len(next(out)) == 13
    else:
        if demux:
            # a tuple of (stdout, stderr)
            stdout, stderr = out
            # len('hello stdout\n' * 4) == 52
            assert len(stdout) == 52
            # len('hello stderr\n' * 2) == 26
            assert len(stderr) == 26
        else:
            # a bytes object
            # len('hello stdout\n' * 4 + 'hello stderr\n' * 2) == 78
            assert len(out) == 78

    return True
