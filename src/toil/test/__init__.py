"""Base testing class for Toil."""

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
import atexit
import datetime
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import threading
import time
import unittest
import uuid
import zoneinfo
from abc import ABCMeta, abstractmethod
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from importlib.resources import as_file, files
from inspect import getsource
from pathlib import Path
from shutil import which
from tempfile import mkstemp
from textwrap import dedent
from typing import Any, Callable, Literal, Optional, TypeVar, Union, cast
from unittest.util import strclass
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from toil import ApplianceImageNotFound, applianceSelf, toilPackageDirPath
from toil.lib.accelerators import (
    have_working_nvidia_docker_runtime,
    have_working_nvidia_smi,
)
from toil.lib.io import mkdtemp
from toil.lib.iterables import concat
from toil.lib.memoize import memoize
from toil.lib.threading import ExceptionalThread, cpu_count
from toil.version import distVersion

logger = logging.getLogger(__name__)


def get_data(filename: str) -> str:
    """Returns an absolute path for a file from this package."""
    # normalizing path depending on OS or else it will cause problem when joining path
    filename = os.path.normpath(filename)
    filepath = None
    # import ipdb; ipdb.set_trace()
    try:
        file_manager = ExitStack()
        atexit.register(file_manager.close)
        traversable = files("toil") / filename
        filepath = file_manager.enter_context(as_file(traversable))
    except ModuleNotFoundError:
        pass
    if not filepath or not os.path.isfile(filepath):
        filepath = Path(os.path.dirname(__file__)) / ".." / ".." / ".." / filename
    return str(filepath.resolve())


class ToilTest(unittest.TestCase):
    """
    A common base class for Toil tests.

    Please have every test case directly or indirectly inherit this one.

    When running tests you may optionally set the TOIL_TEST_TEMP environment variable
    to the path of a directory where you want temporary test files be placed. The
    directory will be created if it doesn't exist. The path may be relative in which
    case it will be assumed to be relative to the project root. If TOIL_TEST_TEMP
    is not defined, temporary files and directories will be created in the system's
    default location for such files and any temporary files or directories left
    over from tests will be removed automatically removed during tear down.
    Otherwise, left-over files will not be removed.
    """

    _tempBaseDir: Optional[str] = None
    _tempDirs: list[str] = []

    def setup_method(self, method: Any) -> None:
        western = zoneinfo.ZoneInfo("America/Los_Angeles")
        california_time = datetime.datetime.now(tz=western)
        timestamp = california_time.strftime("%b %d %Y %H:%M:%S:%f %Z")
        print(
            f"\n\n[TEST] {strclass(self.__class__)}:{self._testMethodName} ({timestamp})\n\n"
        )

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        tempBaseDir = os.environ.get("TOIL_TEST_TEMP", None)
        if tempBaseDir is not None and not os.path.isabs(tempBaseDir):
            tempBaseDir = os.path.abspath(
                os.path.join(cls._projectRootPath(), tempBaseDir)
            )
            os.makedirs(tempBaseDir, exist_ok=True)
        cls._tempBaseDir = tempBaseDir

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._tempBaseDir is None:
            while cls._tempDirs:
                tempDir = cls._tempDirs.pop()
                if os.path.exists(tempDir):
                    shutil.rmtree(tempDir)
        else:
            cls._tempDirs = []
        super().tearDownClass()

    def setUp(self) -> None:
        logger.info("Setting up %s ...", self.id())
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        logger.info("Tore down %s", self.id())

    @classmethod
    def awsRegion(cls) -> str:
        """
        Pick an appropriate AWS region.

        Use us-west-2 unless running on EC2, in which case use the region in which
        the instance is located
        """
        from toil.lib.aws import running_on_ec2

        return cls._region() if running_on_ec2() else "us-west-2"

    @classmethod
    def _availabilityZone(cls) -> str:
        """
        Query this instance's metadata to determine in which availability zone it is running.

        Used only when running on EC2.
        """
        zone = urlopen(
            "http://169.254.169.254/latest/meta-data/placement/availability-zone"
        ).read()
        return zone if not isinstance(zone, bytes) else zone.decode("utf-8")

    @classmethod
    @memoize
    def _region(cls) -> str:
        """
        Determine in what region this instance is running.

        Used only when running on EC2.
        The region will not change over the life of the instance so the result
        is memoized to avoid unnecessary work.
        """
        region = re.match(
            r"^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$", cls._availabilityZone()
        )
        assert region
        return region.group(1)

    @classmethod
    def _getUtilScriptPath(cls, script_name: str) -> str:
        return os.path.join(toilPackageDirPath(), "utils", script_name + ".py")

    @classmethod
    def _projectRootPath(cls) -> str:
        """
        Return the path to the project root.

        i.e. the directory that typically contains the .git and src subdirectories.
        This method has limited utility. It only works if in "develop"
        mode, since it assumes the existence of a src subdirectory which, in a regular install
        wouldn't exist. Then again, in that mode project root has no meaning anyways.
        """
        assert re.search(r"__init__\.pyc?$", __file__)
        projectRootPath = os.path.dirname(os.path.abspath(__file__))
        packageComponents = __name__.split(".")
        expectedSuffix = os.path.join("src", *packageComponents)
        assert projectRootPath.endswith(expectedSuffix)
        projectRootPath = projectRootPath[: -len(expectedSuffix)]
        return projectRootPath

    def _createTempDir(self, purpose: Optional[str] = None) -> str:
        return self._createTempDirEx(self._testMethodName, purpose)

    @classmethod
    def _createTempDirEx(cls, *names: Optional[str]) -> str:
        classname = strclass(cls)
        if classname.startswith("toil.test."):
            classname = classname[len("toil.test.") :]
        prefix = ["toil", "test", classname]
        prefix.extend([_f for _f in names if _f])
        prefix.append("")
        temp_dir_path = os.path.realpath(
            mkdtemp(dir=cls._tempBaseDir, prefix="-".join(prefix))
        )
        cls._tempDirs.append(temp_dir_path)
        return temp_dir_path

    @classmethod
    def _getTestJobStorePath(cls) -> str:
        path = cls._createTempDirEx("jobstore")
        # We only need a unique path, directory shouldn't actually exist. This of course is racy
        # and insecure because another thread could now allocate the same path as a temporary
        # directory. However, the built-in tempfile module randomizes the name temp dir suffixes
        # reasonably well (1 in 63 ^ 6 chance of collision), making this an unlikely scenario.
        os.rmdir(path)
        return path

    @classmethod
    def _getSourceDistribution(cls) -> str:
        """
        Find the sdist tarball for this project and return the path to it.

        Also assert that the sdist is up-to date
        """
        sdistPath = os.path.join(
            cls._projectRootPath(), "dist", "toil-%s.tar.gz" % distVersion
        )
        assert os.path.isfile(sdistPath), (
            "Can't find Toil source distribution at %s. Run 'make sdist'." % sdistPath
        )
        excluded = set(
            cast(
                str,
                cls._run(
                    "git",
                    "ls-files",
                    "--others",
                    "-i",
                    "--exclude-standard",
                    capture=True,
                    cwd=cls._projectRootPath(),
                ),
            ).splitlines()
        )
        dirty = cast(
            str,
            cls._run(
                "find",
                "src",
                "-type",
                "f",
                "-newer",
                sdistPath,
                capture=True,
                cwd=cls._projectRootPath(),
            ),
        ).splitlines()
        assert all(path.startswith("src") for path in dirty)
        dirty_set = set(dirty)
        dirty_set.difference_update(excluded)
        assert (
            not dirty_set
        ), "Run 'make clean_sdist sdist'. Files newer than {}: {!r}".format(
            sdistPath,
            list(dirty_set),
        )
        return sdistPath

    @classmethod
    def _run(cls, command: str, *args: str, **kwargs: Any) -> Optional[str]:
        """
        Run a command.

        Convenience wrapper for subprocess.check_call and subprocess.check_output.

        :param command: The command to be run.

        :param args: Any arguments to be passed to the command.

        :param kwargs: keyword arguments for subprocess.Popen constructor.
            Pass capture=True to have the process' stdout returned.
            Pass input='some string' to feed input to the process' stdin.

        :return: The output of the process' stdout if capture=True was passed, None otherwise.
        """
        argl = list(concat(command, args))
        logger.info("Running %r", argl)
        capture = kwargs.pop("capture", False)
        _input = kwargs.pop("input", None)
        if capture:
            kwargs["stdout"] = subprocess.PIPE
        if _input is not None:
            kwargs["stdin"] = subprocess.PIPE
        popen = subprocess.Popen(args, universal_newlines=True, **kwargs)
        stdout, stderr = popen.communicate(input=_input)
        assert stderr is None
        if popen.returncode != 0:
            raise subprocess.CalledProcessError(popen.returncode, argl)
        if capture:
            return cast(Optional[str], stdout)

    def _getScriptSource(self, callable_: Callable[..., Any]) -> str:
        """
        Return the source code of the body of given callable as a string, dedented.

        This is a naughty but incredibly useful trick that lets you embed user scripts
        as nested functions and expose them to the syntax checker of your IDE.
        """
        return dedent("\n".join(getsource(callable_).split("\n")[1:]))


MT = TypeVar("MT", bound=Callable[..., Any])

try:
    # noinspection PyUnresolvedReferences
    from pytest import mark as pytest_mark
except ImportError:
    # noinspection PyUnusedLocal
    def _mark_test(name: str, test_item: MT) -> MT:
        return test_item

else:

    def _mark_test(name: str, test_item: MT) -> MT:
        return cast(MT, getattr(pytest_mark, name)(test_item))


def get_temp_file(suffix: str = "", rootDir: Optional[str] = None) -> str:
    """Return a string representing a temporary file, that must be manually deleted."""
    if rootDir is None:
        handle, tmp_file = mkstemp(suffix)
        os.close(handle)
        return tmp_file
    else:
        alphanumerics = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        tmp_file = os.path.join(
            rootDir,
            f"tmp_{''.join([random.choice(alphanumerics) for _ in range(0, 10)])}{suffix}",
        )
        open(tmp_file, "w").close()
        os.chmod(tmp_file, 0o777)  # Ensure everyone has access to the file.
        return tmp_file


def needs_env_var(var_name: str, comment: Optional[str] = None) -> Callable[[MT], MT]:
    """
    Use as a decorator before test classes or methods to run only if the given
    environment variable is set.
    Can include a comment saying what the variable should be set to.
    """

    def decorator(test_item: MT) -> MT:
        if not os.getenv(var_name):
            return unittest.skip(
                f"Set {var_name}{' to ' + comment if comment else ''} to include this test."
            )(test_item)
        return test_item

    return decorator


def needs_rsync3(test_item: MT) -> MT:
    """
    Decorate classes or methods that depend on any features from rsync version 3.0.0+.

    Necessary because :meth:`utilsTest.testAWSProvisionerUtils` uses option `--protect-args`
    which is only available in rsync 3
    """
    test_item = _mark_test("rsync", test_item)
    try:
        versionInfo = subprocess.check_output(["rsync", "--version"]).decode("utf-8")
        # output looks like: 'rsync  version 2.6.9 ...'
        if int(versionInfo.split()[2].split(".")[0]) < 3:
            return unittest.skip("This test depends on rsync version 3.0.0+.")(
                test_item
            )
    except subprocess.CalledProcessError:
        return unittest.skip("rsync needs to be installed to run this test.")(test_item)
    return test_item


def needs_online(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if we are meant to talk to the Internet."""
    test_item = _mark_test("online", test_item)
    if os.getenv("TOIL_SKIP_ONLINE", "").lower() == "true":
        return unittest.skip("Skipping online test.")(test_item)
    return test_item


def needs_aws_s3(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if AWS S3 is usable."""
    # TODO: we just check for generic access to the AWS account
    test_item = _mark_test("aws-s3", needs_online(test_item))
    try:
        from boto3 import Session

        session = Session()
        boto3_credentials = session.get_credentials()
    except ImportError:
        return unittest.skip("Install Toil with the 'aws' extra to include this test.")(
            test_item
        )
    from toil.lib.aws import running_on_ec2

    if not (
        boto3_credentials
        or os.path.exists(os.path.expanduser("~/.aws/credentials"))
        or running_on_ec2()
    ):
        return unittest.skip("Configure AWS credentials to include this test.")(
            test_item
        )
    return test_item


def needs_aws_ec2(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if AWS EC2 is usable."""
    # Assume we need S3 as well as EC2
    test_item = _mark_test("aws-ec2", needs_aws_s3(test_item))
    # In addition to S3 we also need an SSH key to deploy with.
    # TODO: We assume that if this is set we have EC2 access.
    test_item = needs_env_var("TOIL_AWS_KEYNAME", "an AWS-stored SSH key")(test_item)
    return test_item


def needs_aws_batch(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to run only if AWS Batch
    is usable.
    """
    # Assume we need S3 as well as Batch
    test_item = _mark_test("aws-batch", needs_aws_s3(test_item))
    # Assume we have Batch if the user has set these variables.
    test_item = needs_env_var("TOIL_AWS_BATCH_QUEUE", "an AWS Batch queue name or ARN")(
        test_item
    )
    test_item = needs_env_var(
        "TOIL_AWS_BATCH_JOB_ROLE_ARN", "an IAM role ARN that grants S3 and SDB access"
    )(test_item)
    try:
        from toil.lib.aws import get_current_aws_region

        if get_current_aws_region() is None:
            # We don't know a region so we need one set.
            # TODO: It always won't be set if we get here.
            test_item = needs_env_var(
                "TOIL_AWS_REGION", "an AWS region to use with AWS batch"
            )(test_item)

    except ImportError:
        return unittest.skip("Install Toil with the 'aws' extra to include this test.")(
            test_item
        )
    return test_item


def needs_google_storage(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to run only if Google
    Cloud is installed and we ought to be able to access public Google Storage
    URIs.
    """
    test_item = _mark_test("google-storage", needs_online(test_item))
    try:
        from google.cloud import storage  # noqa
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'google' extra to include this test."
        )(test_item)

    return test_item


def needs_google_project(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to run only if we have a Google Cloud project set.
    """
    test_item = _mark_test("google-project", needs_online(test_item))
    test_item = needs_env_var("TOIL_GOOGLE_PROJECTID", "a Google project ID")(test_item)
    return test_item


def needs_gridengine(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if GridEngine is installed."""
    test_item = _mark_test("gridengine", test_item)
    if which("qhost"):
        return test_item
    return unittest.skip("Install GridEngine to include this test.")(test_item)


def needs_torque(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if PBS/Torque is installed."""
    test_item = _mark_test("torque", test_item)
    if which("pbsnodes"):
        return test_item
    return unittest.skip("Install PBS/Torque to include this test.")(test_item)


def needs_kubernetes_installed(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if Kubernetes is installed."""
    test_item = _mark_test("kubernetes", test_item)
    try:
        import kubernetes

        str(kubernetes)  # to prevent removal of this import
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'kubernetes' extra to include this test."
        )(test_item)
    return test_item


def needs_kubernetes(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if Kubernetes is installed and configured."""
    test_item = needs_kubernetes_installed(needs_online(test_item))
    try:
        import kubernetes

        try:
            kubernetes.config.load_kube_config()
        except kubernetes.config.ConfigException:
            try:
                kubernetes.config.load_incluster_config()
            except kubernetes.config.ConfigException:
                return unittest.skip(
                    "Configure Kubernetes (~/.kube/config, $KUBECONFIG, "
                    "or current pod) to include this test."
                )(test_item)
    except ImportError:
        # We should already be skipping this test
        pass
    return test_item


def needs_mesos(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if Mesos is installed."""
    test_item = _mark_test("mesos", test_item)
    if not (which("mesos-master") or which("mesos-agent")):
        return unittest.skip(
            "Install Mesos (and Toil with the 'mesos' extra) to include this test."
        )(test_item)
    try:
        import psutil  # noqa
        import pymesos  # noqa
    except ImportError:
        return unittest.skip(
            "Install Mesos (and Toil with the 'mesos' extra) to include this test."
        )(test_item)
    return test_item


def needs_slurm(test_item: MT) -> MT:
    """Use as a decorator before test classes or methods to run only if Slurm is installed."""
    test_item = _mark_test("slurm", test_item)
    if which("squeue"):
        return test_item
    return unittest.skip("Install Slurm to include this test.")(test_item)


def needs_htcondor(test_item: MT) -> MT:
    """Use a decorator before test classes or methods to run only if the HTCondor is installed."""
    test_item = _mark_test("htcondor", test_item)
    try:
        import htcondor

        htcondor.Collector(os.getenv("TOIL_HTCONDOR_COLLECTOR")).query(
            constraint="False"
        )
    except ImportError:
        return unittest.skip(
            "Install the HTCondor Python bindings to include this test."
        )(test_item)
    except OSError:
        return unittest.skip("HTCondor must be running to include this test.")(
            test_item
        )
    except RuntimeError:
        return unittest.skip(
            "HTCondor must be installed and configured to include this test."
        )(test_item)
    else:
        return test_item


def needs_lsf(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if LSF is installed.
    """
    test_item = _mark_test("lsf", test_item)
    if which("bsub"):
        return test_item
    else:
        return unittest.skip("Install LSF to include this test.")(test_item)


def needs_java(test_item: MT) -> MT:
    """Use as a test decorator to run only if java is installed."""
    test_item = _mark_test("java", test_item)
    if which("java"):
        return test_item
    else:
        return unittest.skip("Install java to include this test.")(test_item)


def needs_docker(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    docker is installed and docker-based tests are enabled.
    """
    test_item = _mark_test("docker", needs_online(test_item))
    if os.getenv("TOIL_SKIP_DOCKER", "").lower() == "true":
        return unittest.skip("Skipping docker test.")(test_item)
    if which("docker"):
        return test_item
    else:
        return unittest.skip("Install docker to include this test.")(test_item)


def needs_singularity(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    singularity is installed.
    """
    test_item = _mark_test("singularity", needs_online(test_item))
    if which("singularity"):
        return test_item
    else:
        return unittest.skip("Install singularity to include this test.")(test_item)


def needs_singularity_or_docker(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    docker is installed and docker-based tests are enabled, or if Singularity
    is installed.
    """

    # TODO: Is there a good way to OR decorators?
    if which("singularity"):
        # Singularity is here, say it's a Singularity test
        return needs_singularity(test_item)
    else:
        # Otherwise say it's a Docker test.
        return needs_docker(test_item)


def needs_local_cuda(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    a CUDA setup legible to cwltool (i.e. providing userspace nvidia-smi) is present.
    """
    test_item = _mark_test("local_cuda", test_item)
    if have_working_nvidia_smi():
        return test_item
    else:
        return unittest.skip(
            "Install nvidia-smi, an nvidia proprietary driver, and a CUDA-capable nvidia GPU to include this test."
        )(test_item)


def needs_docker_cuda(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    a CUDA setup is available through Docker.
    """
    test_item = _mark_test("docker_cuda", needs_online(test_item))
    if have_working_nvidia_docker_runtime():
        return test_item
    else:
        return unittest.skip(
            "Install nvidia-container-runtime on your Docker server and configure an 'nvidia' runtime to include this test."
        )(test_item)


def needs_encryption(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if PyNaCl is installed
    and configured.
    """
    test_item = _mark_test("encryption", test_item)
    try:
        # noinspection PyUnresolvedReferences
        import nacl  # noqa
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'encryption' extra to include this test."
        )(test_item)
    else:
        return test_item


def needs_cwl(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if CWLTool is installed
    and configured.
    """
    test_item = _mark_test("cwl", test_item)
    try:
        # noinspection PyUnresolvedReferences
        import cwltool  # noqa
    except ImportError:
        return unittest.skip("Install Toil with the 'cwl' extra to include this test.")(
            test_item
        )
    else:
        return test_item


def needs_wdl(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if miniwdl is installed
    and configured.
    """
    test_item = _mark_test("wdl", test_item)
    try:
        # noinspection PyUnresolvedReferences
        import WDL  # noqa
    except ImportError:
        return unittest.skip("Install Toil with the 'wdl' extra to include this test.")(
            test_item
        )
    else:
        return test_item


def needs_server(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if Connexion is installed.
    """
    test_item = _mark_test("server_mode", test_item)
    try:
        # noinspection PyUnresolvedReferences
        import connexion

        print(connexion.__file__)  # keep this import from being removed.
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'server' extra to include this test."
        )(test_item)
    else:
        return test_item


def needs_celery_broker(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to run only if RabbitMQ is set up to take Celery jobs.
    """
    test_item = _mark_test("celery", needs_online(test_item))
    test_item = needs_env_var(
        "TOIL_WES_BROKER_URL", "a URL to a RabbitMQ broker for Celery"
    )(test_item)
    return test_item


def needs_wes_server(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to run only if a WES
    server is available to run against.
    """
    test_item = _mark_test("wes_server", needs_online(test_item))

    wes_url = os.environ.get("TOIL_WES_ENDPOINT")
    if not wes_url:
        return unittest.skip(f"Set TOIL_WES_ENDPOINT to include this test")(test_item)

    try:
        urlopen(f"{wes_url}/ga4gh/wes/v1/service-info")
    except (HTTPError, URLError) as e:
        return unittest.skip(f"Run a WES server on {wes_url} to include this test")(
            test_item
        )

    return test_item


def needs_local_appliance(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    the Toil appliance Docker image is downloaded.
    """
    test_item = _mark_test("appliance", test_item)
    if os.getenv("TOIL_SKIP_DOCKER", "").lower() == "true":
        return unittest.skip("Skipping docker test.")(test_item)
    if not which("docker"):
        return unittest.skip("Install docker to include this test.")(test_item)

    try:
        image = applianceSelf()
    except ApplianceImageNotFound:
        return unittest.skip(
            "Appliance image is not published. Use 'make test' target to automatically "
            "build appliance, or just run 'make push_docker' prior to running this "
            "test."
        )(test_item)

    try:
        stdout, stderr = subprocess.Popen(
            ["docker", "inspect", '--format="{{json .RepoTags}}"', image],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate()
        if image in stdout.decode("utf-8"):
            return test_item
    except Exception:
        pass

    return unittest.skip(
        f"Cannot find appliance {image} locally. Use 'make test' target to automatically "
        "build appliance, or just run 'make push_docker' prior to running this "
        "test."
    )(test_item)


def needs_fetchable_appliance(test_item: MT) -> MT:
    """
    Use as a decorator before test classes or methods to only run them if
    the Toil appliance Docker image is able to be downloaded from the Internet.
    """

    test_item = _mark_test("fetchable_appliance", needs_online(test_item))
    if os.getenv("TOIL_SKIP_DOCKER", "").lower() == "true":
        return unittest.skip("Skipping docker test.")(test_item)
    try:
        applianceSelf()
    except ApplianceImageNotFound:
        # Not downloadable
        return unittest.skip(
            "Cannot see appliance in registry. Use 'make test' target to automatically "
            "build appliance, or just run 'make push_docker' prior to running this "
            "test."
        )(test_item)
    else:
        return test_item


def integrative(test_item: MT) -> MT:
    """
    Use this to decorate integration tests so as to skip them during regular builds.

    We define integration tests as A) involving other, non-Toil software components
    that we develop and/or B) having a higher cost (time or money).
    """
    test_item = _mark_test("integrative", test_item)
    if os.getenv("TOIL_TEST_INTEGRATIVE", "").lower() == "true":
        return test_item
    else:
        return unittest.skip(
            "Set TOIL_TEST_INTEGRATIVE=True to include this integration test, "
            "or run `make integration_test_local` to run all integration tests."
        )(test_item)


def slow(test_item: MT) -> MT:
    """
    Use this decorator to identify tests that are slow and not critical.
    Skip if TOIL_TEST_QUICK is true.
    """
    test_item = _mark_test("slow", test_item)
    if os.environ.get("TOIL_TEST_QUICK", "").lower() != "true":
        return test_item
    else:
        return unittest.skip('Skipped because TOIL_TEST_QUICK is "True"')(test_item)


methodNamePartRegex = re.compile("^[a-zA-Z_0-9]+$")


@contextmanager
def timeLimit(seconds: int) -> Generator[None, None, None]:
    """
    Use to limit the execution time of a function.

    Raises an exception if the execution of the function takes more than the
    specified amount of time. See <http://stackoverflow.com/a/601168>.

    :param seconds: maximum allowable time, in seconds

    >>> import time
    >>> with timeLimit(2):
    ...    time.sleep(1)
    >>> import time
    >>> with timeLimit(1):
    ...    time.sleep(2)
    Traceback (most recent call last):
        ...
    RuntimeError: Timed out
    """

    # noinspection PyUnusedLocal
    def signal_handler(signum: int, frame: Any) -> None:
        raise RuntimeError("Timed out")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


def make_tests(generalMethod, targetClass, **kwargs):
    """
    This method dynamically generates test methods using the generalMethod as a template. Each
    generated function is the result of a unique combination of parameters applied to the
    generalMethod. Each of the parameters has a corresponding string that will be used to name
    the method. These generated functions are named in the scheme: test_[generalMethodName]___[
    firstParamaterName]_[someValueName]__[secondParamaterName]_...

    The arguments following the generalMethodName should be a series of one or more dictionaries
    of the form {str : type, ...} where the key represents the name of the value. The names will
    be used to represent the permutation of values passed for each parameter in the generalMethod.

    The generated method names will list the parameters in lexicographic order by parameter name.

    :param generalMethod: A method that will be parameterized with values passed as kwargs. Note
           that the generalMethod must be a regular method.

    :param targetClass: This represents the class to which the generated test methods will be
           bound. If no targetClass is specified the class of the generalMethod is assumed the
           target.

    :param kwargs: a series of dictionaries defining values, and their respective names where
           each keyword is the name of a parameter in generalMethod.

    >>> class Foo:
    ...     def has(self, num, letter):
    ...         return num, letter
    ...
    ...     def hasOne(self, num):
    ...         return num

    >>> class Bar(Foo):
    ...     pass

    >>> make_tests(Foo.has, Bar, num={'one':1, 'two':2}, letter={'a':'a', 'b':'b'})

    >>> b = Bar()

    Note that num comes lexicographically before letter and so appears first in
    the generated method names.

    >>> assert b.test_has__letter_a__num_one() == b.has(1, 'a')

    >>> assert b.test_has__letter_b__num_one() == b.has(1, 'b')

    >>> assert b.test_has__letter_a__num_two() == b.has(2, 'a')

    >>> assert b.test_has__letter_b__num_two() == b.has(2, 'b')

    >>> f = Foo()

    >>> hasattr(f, 'test_has__num_one__letter_a')  # should be false because Foo has no test methods
    False

    """

    def permuteIntoLeft(left, rParamName, right):
        """
        Permutes values in right dictionary into each parameter: value dict pair in the left
        dictionary. Such that the left dictionary will contain a new set of keys each of which is
        a combination of one of its original parameter-value names appended with some
        parameter-value name from the right dictionary. Each original key in the left is deleted
        from the left dictionary after the permutation of the key and every parameter-value name
        from the right has been added to the left dictionary.

        For example if left is  {'__PrmOne_ValName':{'ValName':Val}} and right is
        {'rValName1':rVal1, 'rValName2':rVal2} then left will become
        {'__PrmOne_ValName__rParamName_rValName1':{'ValName':Val. 'rValName1':rVal1},
        '__PrmOne_ValName__rParamName_rValName2':{'ValName':Val. 'rValName2':rVal2}}

        :param left: A dictionary pairing each paramNameValue to a nested dictionary that
               contains each ValueName and value pair described in the outer dict's paramNameValue
               key.

        :param rParamName: The name of the parameter that each value in the right dict represents.

        :param right: A dict that pairs 1 or more valueNames and values for the rParamName
               parameter.
        """
        for prmValName, lDict in list(left.items()):
            for rValName, rVal in list(right.items()):
                nextPrmVal = f"__{rParamName}_{rValName.lower()}"
                if methodNamePartRegex.match(nextPrmVal) is None:
                    raise RuntimeError(
                        "The name '%s' cannot be used in a method name" % pvName
                    )
                aggDict = dict(lDict)
                aggDict[rParamName] = rVal
                left[prmValName + nextPrmVal] = aggDict
            left.pop(prmValName)

    def insertMethodToClass():
        """Generate and insert test methods."""

        def fx(self, prms=prms):
            if prms is not None:
                return generalMethod(self, **prms)
            else:
                return generalMethod(self)

        methodName = f"test_{generalMethod.__name__}{prmNames}"

        setattr(targetClass, methodName, fx)

    if len(kwargs) > 0:
        # Define order of kwargs.
        # We keep them in reverse order of how we use them for efficient pop.
        sortedKwargs = sorted(list(kwargs.items()), reverse=True)

        # create first left dict
        left = {}
        prmName, vals = sortedKwargs.pop()
        for valName, val in list(vals.items()):
            pvName = f"__{prmName}_{valName.lower()}"
            if methodNamePartRegex.match(pvName) is None:
                raise RuntimeError(
                    "The name '%s' cannot be used in a method name" % pvName
                )
            left[pvName] = {prmName: val}

        # get cartesian product
        while len(sortedKwargs) > 0:
            permuteIntoLeft(left, *sortedKwargs.pop())

        # set class attributes
        targetClass = targetClass or generalMethod.__class__
        for prmNames, prms in list(left.items()):
            insertMethodToClass()
    else:
        prms = None
        prmNames = ""
        insertMethodToClass()


class ApplianceTestSupport(ToilTest):
    """
    A Toil test that runs a user script on a minimal cluster of appliance containers.

    i.e. one leader container and one worker container.
    """

    @contextmanager
    def _applianceCluster(
        self, mounts: dict[str, str], numCores: Optional[int] = None
    ) -> Generator[
        tuple["ApplianceTestSupport.LeaderThread", "ApplianceTestSupport.WorkerThread"],
        None,
        None,
    ]:
        """
        Context manager for creating and tearing down an appliance cluster.

        :param mounts: Dictionary mapping host paths to container paths. Both the leader
               and the worker container will be started with one -v argument per
               dictionary entry, as in -v KEY:VALUE.

               Beware that if KEY is a path to a directory, its entire content will be deleted
               when the cluster is torn down.

        :param numCores: The number of cores to be offered by the Mesos agent process running
               in the worker container.

        :return: A tuple of the form `(leader, worker)` containing the Appliance instances
                 representing the respective appliance containers
        """
        if numCores is None:
            numCores = cpu_count()
        # The last container to stop (and the first to start) should clean the mounts.
        with self.LeaderThread(self, mounts, cleanMounts=True) as leader:
            with self.WorkerThread(self, mounts, numCores) as worker:
                yield leader, worker

    class Appliance(ExceptionalThread, metaclass=ABCMeta):
        @abstractmethod
        def _getRole(self) -> str:
            return "leader"

        @abstractmethod
        def _containerCommand(self) -> list[str]:
            pass

        @abstractmethod
        def _entryPoint(self) -> str:
            pass

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(
            self,
            outer: "ApplianceTestSupport",
            mounts: dict[str, str],
            cleanMounts: bool = False,
        ) -> None:
            assert all(
                " " not in v for v in mounts.values()
            ), "No spaces allowed in mounts"
            super().__init__()
            self.outer = outer
            self.mounts = mounts
            self.cleanMounts = cleanMounts
            self.containerName = str(uuid.uuid4())
            self.popen: Optional[subprocess.Popen[bytes]] = None

        def __enter__(self) -> "Appliance":
            with self.lock:
                image = applianceSelf()
                # Omitting --rm, it's unreliable, see https://github.com/docker/docker/issues/16575
                args = list(
                    concat(
                        "docker",
                        "run",
                        "--entrypoint=" + self._entryPoint(),
                        "--net=host",
                        "-i",
                        "--name=" + self.containerName,
                        ["--volume=%s:%s" % mount for mount in self.mounts.items()],
                        image,
                        self._containerCommand(),
                    )
                )
                logger.info("Running %r", args)
                self.popen = subprocess.Popen(args)
            self.start()
            self.__wait_running()
            return self

        # noinspection PyUnusedLocal
        def __exit__(
            self, exc_type: type[BaseException], exc_val: Exception, exc_tb: Any
        ) -> Literal[False]:
            try:
                try:
                    self.outer._run("docker", "stop", self.containerName)
                    self.join()
                finally:
                    if self.cleanMounts:
                        self.__cleanMounts()
            finally:
                self.outer._run("docker", "rm", "-f", self.containerName)
            return False  # don't swallow exception

        def __wait_running(self) -> None:
            logger.info(
                "Waiting for %s container process to appear. "
                "Expect to see 'Error: No such image or container'.",
                self._getRole(),
            )
            alive = cast(
                Callable[[], bool], getattr(self, "isAlive", getattr(self, "is_alive"))
            )
            while alive():
                try:
                    running = cast(
                        str,
                        self.outer._run(
                            "docker",
                            "inspect",
                            "--format={{ .State.Running }}",
                            self.containerName,
                            capture=True,
                        ),
                    ).strip()
                except subprocess.CalledProcessError:
                    pass
                else:
                    if "true" == running:
                        break
                time.sleep(1)

        def __cleanMounts(self) -> None:
            """
            Delete all files in every mounted directory.

            Without this step, we risk leaking files owned by root on the host.
            To avoid races, this method should be called after the appliance container
            was stopped, otherwise the running container might still be writing files.
            """
            # Delete all files within each mounted directory, but not the directory itself.
            cmd = "shopt -s dotglob && rm -rf " + " ".join(
                v + "/*" for k, v in self.mounts.items() if os.path.isdir(k)
            )
            self.outer._run(
                "docker",
                "run",
                "--rm",
                "--entrypoint=/bin/bash",
                applianceSelf(),
                "-c",
                cmd,
            )

        def tryRun(self) -> None:
            assert self.popen
            self.popen.wait()
            logger.info("Exiting %s", self.__class__.__name__)

        def runOnAppliance(self, *args: str, **kwargs: Any) -> None:
            # Check if thread is still alive. Note that ExceptionalThread.join raises the
            # exception that occurred in the thread.
            self.join(timeout=0)
            # noinspection PyProtectedMember
            self.outer._run("docker", "exec", "-i", self.containerName, *args, **kwargs)

        def writeToAppliance(self, path: str, contents: Any) -> None:
            self.runOnAppliance("tee", path, input=contents)

        def deployScript(
            self, path: str, packagePath: str, script: Union[str, Callable[..., Any]]
        ) -> None:
            """
            Deploy a Python module on the appliance.

            :param path: the path (absolute or relative to the WORDIR of the appliance container)
                   to the root of the package hierarchy where the given module should be placed.
                   The given directory should be on the Python path.

            :param packagePath: the desired fully qualified module name (dotted form) of the module

            :param str|callable script: the contents of the Python module. If a callable is given,
                   its source code will be extracted. This is a convenience that lets you embed
                   user scripts into test code as nested function.
            """
            if callable(script):
                script = self.outer._getScriptSource(script)
            packagePath_list = packagePath.split(".")
            packages, module = packagePath_list[:-1], packagePath_list[-1]
            for package in packages:
                path += "/" + package
                self.runOnAppliance("mkdir", "-p", path)
                self.writeToAppliance(path + "/__init__.py", "")
            self.writeToAppliance(path + "/" + module + ".py", script)

    class LeaderThread(Appliance):
        def _entryPoint(self) -> str:
            return "mesos-master"

        def _getRole(self) -> str:
            return "leader"

        def _containerCommand(self) -> list[str]:
            return [
                "--registry=in_memory",
                "--ip=127.0.0.1",
                "--port=5050",
                "--allocation_interval=500ms",
            ]

    class WorkerThread(Appliance):
        def __init__(
            self, outer: "ApplianceTestSupport", mounts: dict[str, str], numCores: int
        ) -> None:
            self.numCores = numCores
            super().__init__(outer, mounts)

        def _entryPoint(self) -> str:
            return "mesos-agent"

        def _getRole(self) -> str:
            return "worker"

        def _containerCommand(self) -> list[str]:
            return [
                "--work_dir=/var/lib/mesos",
                "--ip=127.0.0.1",
                "--master=127.0.0.1:5050",
                "--attributes=preemptible:False",
                "--resources=cpus(*):%i" % self.numCores,
                "--no-hostname_lookup",
                "--no-systemd_enable_support",
            ]
