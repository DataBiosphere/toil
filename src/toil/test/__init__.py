# Copyright (C) 2015-2020 Regents of the University of California
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
from __future__ import absolute_import

import datetime
import logging
import os
import re
import shutil
import signal
import tempfile
import threading
import time
import unittest
import uuid
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from inspect import getsource
from shutil import which
from textwrap import dedent

import pytz
from builtins import str
from future.utils import with_metaclass
from six import iteritems, itervalues
from six.moves.urllib.request import urlopen
from unittest.util import strclass

from toil import subprocess
from toil import toilPackageDirPath, applianceSelf, ApplianceImageNotFound
from toil import which
from toil.lib.iterables import concat
from toil.lib.memoize import memoize
from toil.lib.threading import ExceptionalThread, cpu_count
from toil.provisioners.aws import runningOnEC2
from toil.version import distVersion

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class ToilTest(unittest.TestCase):
    """
    A common base class for Toil tests. Please have every test case directly or indirectly
    inherit this one.

    When running tests you may optionally set the TOIL_TEST_TEMP environment variable to the path
    of a directory where you want temporary test files be placed. The directory will be created
    if it doesn't exist. The path may be relative in which case it will be assumed to be relative
    to the project root. If TOIL_TEST_TEMP is not defined, temporary files and directories will
    be created in the system's default location for such files and any temporary files or
    directories left over from tests will be removed automatically removed during tear down.
    Otherwise, left-over files will not be removed.
    """
    _tempBaseDir = None
    _tempDirs = None

    def setup_method(self, method):
        western = pytz.timezone('America/Los_Angeles')
        california_time = western.localize(datetime.datetime.now())
        timestamp = california_time.strftime("%b %d %Y %H:%M:%S:%f %Z")
        print(f"\n\n[TEST] {strclass(self.__class__)}:{self._testMethodName} ({timestamp})\n\n")

    @classmethod
    def setUpClass(cls):
        super(ToilTest, cls).setUpClass()
        cls._tempDirs = []
        tempBaseDir = os.environ.get('TOIL_TEST_TEMP', None)
        if tempBaseDir is not None and not os.path.isabs(tempBaseDir):
            tempBaseDir = os.path.abspath(os.path.join(cls._projectRootPath(), tempBaseDir))
            os.makedirs(tempBaseDir, exist_ok=True)
        cls._tempBaseDir = tempBaseDir

    @classmethod
    def tearDownClass(cls):
        if cls._tempBaseDir is None:
            while cls._tempDirs:
                tempDir = cls._tempDirs.pop()
                if os.path.exists(tempDir):
                    shutil.rmtree(tempDir)
        else:
            cls._tempDirs = []
        super(ToilTest, cls).tearDownClass()

    def setUp(self):
        log.info("Setting up %s ...", self.id())
        super(ToilTest, self).setUp()

    def tearDown(self):
        super(ToilTest, self).tearDown()
        log.info("Tore down %s", self.id())

    @classmethod
    def awsRegion(cls):
        """
        Use us-west-2 unless running on EC2, in which case use the region in which
        the instance is located
        """
        return cls._region() if runningOnEC2() else 'us-west-2'

    @classmethod
    def _availabilityZone(cls):
        """
        Used only when running on EC2. Query this instance's metadata to determine
        in which availability zone it is running.
        """
        zone = urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone').read()
        return zone if not isinstance(zone, bytes) else zone.decode('utf-8')

    @classmethod
    @memoize
    def _region(cls):
        """
        Used only when running on EC2. Determines in what region this instance is running.
        The region will not change over the life of the instance so the result
        is memoized to avoid unnecessary work.
        """
        region = re.match(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$', cls._availabilityZone())
        assert region
        return region.group(1)

    @classmethod
    def _getUtilScriptPath(cls, script_name):
        return os.path.join(toilPackageDirPath(), 'utils', script_name + '.py')

    @classmethod
    def _projectRootPath(cls):
        """
        Returns the path to the project root, i.e. the directory that typically contains the .git
        and src subdirectories. This method has limited utility. It only works if in "develop"
        mode, since it assumes the existence of a src subdirectory which, in a regular install
        wouldn't exist. Then again, in that mode project root has no meaning anyways.
        """
        assert re.search(r'__init__\.pyc?$', __file__)
        projectRootPath = os.path.dirname(os.path.abspath(__file__))
        packageComponents = __name__.split('.')
        expectedSuffix = os.path.join('src', *packageComponents)
        assert projectRootPath.endswith(expectedSuffix)
        projectRootPath = projectRootPath[:-len(expectedSuffix)]
        return projectRootPath

    def _createTempDir(self, purpose=None):
        return self._createTempDirEx(self._testMethodName, purpose)

    @classmethod
    def _createTempDirEx(cls, *names):
        prefix = ['toil', 'test', strclass(cls)]
        prefix.extend([_f for _f in names if _f])
        prefix.append('')
        temp_dir_path = os.path.realpath(tempfile.mkdtemp(dir=cls._tempBaseDir, prefix='-'.join(prefix)))
        cls._tempDirs.append(temp_dir_path)
        return temp_dir_path

    def _getTestJobStorePath(self):
        path = self._createTempDir(purpose='jobstore')
        # We only need a unique path, directory shouldn't actually exist. This of course is racy
        # and insecure because another thread could now allocate the same path as a temporary
        # directory. However, the built-in tempfile module randomizes the name temp dir suffixes
        # reasonably well (1 in 63 ^ 6 chance of collision), making this an unlikely scenario.
        os.rmdir(path)
        return path

    @classmethod
    def _getSourceDistribution(cls):
        """
        Find the sdist tarball for this project, check whether it is up-to date and return the
        path to it.

        :rtype: str
        """
        sdistPath = os.path.join(cls._projectRootPath(), 'dist', 'toil-%s.tar.gz' % distVersion)
        assert os.path.isfile(sdistPath), "Can't find Toil source distribution at %s. Run 'make sdist'." % sdistPath
        excluded = set(cls._run('git', 'ls-files', '--others', '-i', '--exclude-standard',
                                capture=True,
                                cwd=cls._projectRootPath()).splitlines())
        dirty = cls._run('find', 'src', '-type', 'f', '-newer', sdistPath,
                         capture=True,
                         cwd=cls._projectRootPath()).splitlines()
        assert all(path.startswith('src') for path in dirty)
        dirty = set(dirty)
        dirty.difference_update(excluded)
        assert not dirty, "Run 'make clean_sdist sdist'. Files newer than %s: %r" % (sdistPath, list(dirty))
        return sdistPath

    @classmethod
    def _run(cls, command, *args, **kwargs):
        """
        Run a command. Convenience wrapper for subprocess.check_call and subprocess.check_output.

        :param str command: The command to be run.

        :param str args: Any arguments to be passed to the command.

        :param Any kwargs: keyword arguments for subprocess.Popen constructor. Pass capture=True
               to have the process' stdout returned. Pass input='some string' to feed input to the
               process' stdin.

        :rtype: None|str

        :return: The output of the process' stdout if capture=True was passed, None otherwise.
        """
        args = list(concat(command, args))
        log.info('Running %r', args)
        capture = kwargs.pop('capture', False)
        _input = kwargs.pop('input', None)
        if capture:
            kwargs['stdout'] = subprocess.PIPE
        if _input is not None:
            kwargs['stdin'] = subprocess.PIPE
        popen = subprocess.Popen(args, **kwargs)
        stdout, stderr = popen.communicate(input=_input)
        assert stderr is None
        if popen.returncode != 0:
            raise subprocess.CalledProcessError(popen.returncode, args)
        if capture:
            return stdout

    def _getScriptSource(self, callable_):
        """
        Returns the source code of the body of given callable as a string, dedented. This is a
        naught but incredibly useful trick that lets you embed user scripts as nested functions
        and expose them to the syntax checker of your IDE.
        """
        return dedent('\n'.join(getsource(callable_).split('\n')[1:]))


try:
    # noinspection PyUnresolvedReferences
    import pytest.mark
except ImportError:
    # noinspection PyUnusedLocal
    def _mark_test(name, test_item):
        return test_item
else:
    def _mark_test(name, test_item):
        return getattr(pytest.mark, name)(test_item)


def needs_rsync3(test_item):
    """
    Use as a decorator before test classes or methods that depend on any features used in rsync
    version 3.0.0+

    Necessary because :meth:`utilsTest.testAWSProvisionerUtils` uses option `--protect-args` which is only
    available in rsync 3
    """
    test_item = _mark_test('rsync', test_item)
    try:
        versionInfo = subprocess.check_output(['rsync', '--version']).decode('utf-8')
        if int(versionInfo.split()[2].split('.')[0]) < 3:  # output looks like: 'rsync  version 2.6.9 ...'
            return unittest.skip('This test depends on rsync version 3.0.0+.')(test_item)
    except subprocess.CalledProcessError:
        return unittest.skip('rsync needs to be installed to run this test.')(test_item)
    return test_item


def needs_aws_s3(test_item):
    """Use as a decorator before test classes or methods to run only if AWS S3 is usable."""
    # TODO: we just check for generic access to the AWS account
    test_item = _mark_test('aws-s3', test_item)
    try:
        from boto import config
        boto_credentials = config.get('Credentials', 'aws_access_key_id')
    except ImportError:
        return unittest.skip("Install Toil with the 'aws' extra to include this test.")(test_item)

    if not (boto_credentials or os.path.exists(os.path.expanduser('~/.aws/credentials')) or runningOnEC2()):
        return unittest.skip("Configure AWS credentials to include this test.")(test_item)
    return test_item


def needs_aws_ec2(test_item):
    """Use as a decorator before test classes or methods to run only if AWS EC2 is usable."""
    # Assume we need S3 as well as EC2
    test_item = _mark_test('aws-ec2', needs_aws_s3(test_item))
    if not os.getenv('TOIL_AWS_KEYNAME'):
        # In addition to S3 we also need an SSH key to deploy with.
        # TODO: We assume that if this is set we have EC2 access.
        return unittest.skip("Set TOIL_AWS_KEYNAME to an AWS-stored SSH key to include this test.")(test_item)
    return test_item


def travis_test(test_item):
    test_item = _mark_test('travis', test_item)
    if os.environ.get('TRAVIS') != 'true':
        return unittest.skip("Set TRAVIS='true' to include this test.")(test_item)
    return test_item


def needs_google(test_item):
    """Use as a decorator before test classes or methods to run only if Google is usable."""
    test_item = _mark_test('google', test_item)
    try:
        from boto import config
    except ImportError:
        return unittest.skip("Install Toil with the 'google' extra to include this test.")(test_item)

    if not os.getenv('TOIL_GOOGLE_PROJECTID'):
        return unittest.skip("Set TOIL_GOOGLE_PROJECTID to include this test.")(test_item)
    elif not config.get('Credentials'):  # TODO: Deprecate this check?  Needed by only by the ancients.
        return unittest.skip("Configure ~/.boto with Google Cloud credentials to include this test.")(test_item)
    return test_item


def needs_gridengine(test_item):
    """Use as a decorator before test classes or methods to run only if GridEngine is installed."""
    test_item = _mark_test('gridengine', test_item)
    if which('qhost'):
        return test_item
    return unittest.skip("Install GridEngine to include this test.")(test_item)


def needs_torque(test_item):
    """Use as a decorator before test classes or methods to run only ifPBS/Torque is installed."""
    test_item = _mark_test('torque', test_item)
    if which('pbsnodes'):
        return test_item
    return unittest.skip("Install PBS/Torque to include this test.")(test_item)


def needs_kubernetes(test_item):
    """Use as a decorator before test classes or methods to run only if Kubernetes is installed."""
    test_item = _mark_test('kubernetes', test_item)
    try:
        import kubernetes
        kubernetes.config.load_kube_config()
    except ImportError:
        return unittest.skip("Install Toil with the 'kubernetes' extra to include this test.")(test_item)
    except TypeError:
        return unittest.skip("Configure Kubernetes (~/.kube/config) to include this test.")(test_item)
    return test_item


def needs_mesos(test_item):
    """Use as a decorator before test classes or methods to run only if Mesos is installed."""
    test_item = _mark_test('mesos', test_item)
    if not (which('mesos-master') or which('mesos-slave')):
        return unittest.skip("Install Mesos (and Toil with the 'mesos' extra) to include this test.")(test_item)
    try:
        import pymesos
        import psutil
    except ImportError:
        return unittest.skip("Install Mesos (and Toil with the 'mesos' extra) to include this test.")(test_item)
    return test_item


def needs_parasol(test_item):
    """Use as decorator so tests are only run if Parasol is installed."""
    test_item = _mark_test('parasol', test_item)
    if which('parasol'):
        return test_item
    return unittest.skip("Install Parasol to include this test.")(test_item)


def needs_slurm(test_item):
    """Use as a decorator before test classes or methods to run only if Slurm is installed."""
    test_item = _mark_test('slurm', test_item)
    if which('squeue'):
        return test_item
    return unittest.skip("Install Slurm to include this test.")(test_item)


def needs_htcondor(test_item):
    """Use a decorator before test classes or methods to run only if the HTCondor is installed."""
    test_item = _mark_test('htcondor', test_item)
    try:
        import htcondor
        htcondor.Collector(os.getenv('TOIL_HTCONDOR_COLLECTOR')).query(constraint='False')
    except ImportError:
        return unittest.skip("Install the HTCondor Python bindings to include this test.")(test_item)
    except IOError:
        return unittest.skip("HTCondor must be running to include this test.")(test_item)
    except RuntimeError:
        return unittest.skip("HTCondor must be installed and configured to include this test.")(test_item)
    else:
        return test_item


def needs_lsf(test_item):
    """
    Use as a decorator before test classes or methods to only run them if LSF
    is installed.
    """
    test_item = _mark_test('lsf', test_item)
    if which('bsub'):
        return test_item
    else:
        return unittest.skip("Install LSF to include this test.")(test_item)


def needs_docker(test_item):
    """
    Use as a decorator before test classes or methods to only run them if
    docker is installed and docker-based tests are enabled.
    """
    test_item = _mark_test('docker', test_item)
    if os.getenv('TOIL_SKIP_DOCKER', '').lower() == 'true':
        return unittest.skip('Skipping docker test.')(test_item)
    if which('docker'):
        return test_item
    else:
        return unittest.skip("Install docker to include this test.")(test_item)


def needs_encryption(test_item):
    """
    Use as a decorator before test classes or methods to only run them if PyNaCl is installed
    and configured.
    """
    test_item = _mark_test('encryption', test_item)
    try:
        # noinspection PyUnresolvedReferences
        import nacl
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'encryption' extra to include this test.")(test_item)
    else:
        return test_item


def needs_cwl(test_item):
    """
    Use as a decorator before test classes or methods to only run them if CWLTool is installed
    and configured.
    """
    test_item = _mark_test('cwl', test_item)
    try:
        # noinspection PyUnresolvedReferences
        import cwltool
    except ImportError:
        return unittest.skip("Install Toil with the 'cwl' extra to include this test.")(test_item)
    else:
        return test_item


def needs_appliance(test_item):
    """
    Use as a decorator before test classes or methods to only run them if
    the Toil appliance Docker image is downloaded.
    """
    test_item = _mark_test('appliance', test_item)
    if os.getenv('TOIL_SKIP_DOCKER', '').lower() == 'true':
        return unittest.skip('Skipping docker test.')(test_item)
    if not which('docker'):
        return unittest.skip("Install docker to include this test.")(test_item)

    try:
        image = applianceSelf()
        stdout, stderr = subprocess.Popen(['docker', 'inspect', '--format="{{json .RepoTags}}"', image],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if image in stdout.decode("utf-8"):
            return test_item
    except:
        pass

    return unittest.skip(f"Cannot find appliance {image}. Use 'make test' target to automatically build appliance, or "
                         f"just run 'make push_docker' prior to running this test.")(test_item)


def needs_fetchable_appliance(test_item):
    """
    Use as a decorator before test classes or methods to only run them if
    the Toil appliance Docker image is able to be downloaded from the Internet.
    """

    test_item = _mark_test('fetchable_appliance', test_item)
    if os.getenv('TOIL_SKIP_DOCKER', '').lower() == 'true':
        return unittest.skip('Skipping docker test.')(test_item)
    try:
        image = applianceSelf()
    except ApplianceImageNotFound:
        # Not downloadable
        return unittest.skip(f"Cannot see appliance in registry. Use 'make test' target to automatically build appliance, or "
                             f"just run 'make push_docker' prior to running this test.")(test_item)
    else:
        return test_item


def integrative(test_item):
    """
    Use this to decorate integration tests so as to skip them during regular builds. We define
    integration tests as A) involving other, non-Toil software components that we develop and/or
    B) having a higher cost (time or money). Note that brittleness does not qualify a test for
    being integrative. Neither does involvement of external services such as AWS, since that
    would cover most of Toil's test.
    """
    test_item = _mark_test('integrative', test_item)
    if os.getenv('TOIL_TEST_INTEGRATIVE', '').lower() == 'true':
        return test_item
    else:
        return unittest.skip('Set TOIL_TEST_INTEGRATIVE="True" to include this integration test, '
                             'or run `make integration_test_local` to run all integration tests.')(test_item)


def slow(test_item):
    """
    Use this decorator to identify tests that are slow and not critical.
    Skip if TOIL_TEST_QUICK is true.
    """
    test_item = _mark_test('slow', test_item)
    if os.environ.get('TOIL_TEST_QUICK', '').lower() != 'true':
        return test_item
    else:
        return unittest.skip('Skipped because TOIL_TEST_QUICK is "True"')(test_item)


methodNamePartRegex = re.compile('^[a-zA-Z_0-9]+$')


@contextmanager
def timeLimit(seconds):
    """
    http://stackoverflow.com/a/601168
    Use to limit the execution time of a function. Raises an exception if the execution of the
    function takes more than the specified amount of time.

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
    def signal_handler(signum, frame):
        raise RuntimeError('Timed out')

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
                nextPrmVal = ('__%s_%s' % (rParamName, rValName.lower()))
                if methodNamePartRegex.match(nextPrmVal) is None:
                    raise RuntimeError("The name '%s' cannot be used in a method name" % pvName)
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

        methodName = 'test_%s%s' % (generalMethod.__name__, prmNames)

        setattr(targetClass, methodName, fx)

    if len(kwargs) > 0:
        # Define order of kwargs.
        # We keep them in reverse order of how we use them for efficient pop.
        sortedKwargs = sorted(list(kwargs.items()), reverse=True)

        # create first left dict
        left = {}
        prmName, vals = sortedKwargs.pop()
        for valName, val in list(vals.items()):
            pvName = '__%s_%s' % (prmName, valName.lower())
            if methodNamePartRegex.match(pvName) is None:
                raise RuntimeError("The name '%s' cannot be used in a method name" % pvName)
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


@contextmanager
def tempFileContaining(content, suffix=''):
    """
    Write a file with the given contents, and keep it on disk as long as the context is active.
    :param str content: The contents of the file.
    :param str suffix: The extension to use for the temporary file.
    """
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        encoded = content.encode('utf-8')
        assert os.write(fd, encoded) == len(encoded)
    except:
        os.close(fd)
        raise
    else:
        os.close(fd)
        yield path
    finally:
        os.unlink(path)


class ApplianceTestSupport(ToilTest):
    """
    A Toil test that runs a user script on a minimal cluster of appliance containers,
    i.e. one leader container and one worker container.
    """
    @contextmanager
    def _applianceCluster(self, mounts=None, numCores=None):
        """
        A context manager for creating and tearing down an appliance cluster.

        :param dict|None mounts: Dictionary mapping host paths to container paths. Both the leader
               and the worker container will be started with one -v argument per dictionary entry,
               as in -v KEY:VALUE.

               Beware that if KEY is a path to a directory, its entire content will be deleted
               when the cluster is torn down.

        :param int numCores: The number of cores to be offered by the Mesos slave process running
               in the worker container.

        :rtype: (ApplianceTestSupport.Appliance, ApplianceTestSupport.Appliance)

        :return: A tuple of the form `(leader, worker)` containing the Appliance instances
                 representing the respective appliance containers
        """
        if numCores is None:
            numCores = cpu_count()
        # The last container to stop (and the first to start) should clean the mounts.
        with self.LeaderThread(self, mounts, cleanMounts=True) as leader:
            with self.WorkerThread(self, mounts, numCores) as worker:
                yield leader, worker

    class Appliance(with_metaclass(ABCMeta, ExceptionalThread)):
        @abstractmethod
        def _getRole(self):
            return 'leader'

        @abstractmethod
        def _containerCommand(self):
            raise NotImplementedError()

        @abstractmethod
        def _entryPoint(self):
            raise NotImplementedError()

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, outer, mounts, cleanMounts=False):
            """
            :param ApplianceTestSupport outer:
            """
            assert all(' ' not in v for v in itervalues(mounts)), 'No spaces allowed in mounts'
            super(ApplianceTestSupport.Appliance, self).__init__()
            self.outer = outer
            self.mounts = mounts
            self.cleanMounts = cleanMounts
            self.containerName = str(uuid.uuid4())
            self.popen = None

        def __enter__(self):
            with self.lock:
                image = applianceSelf()
                # Omitting --rm, it's unreliable, see https://github.com/docker/docker/issues/16575
                args = list(concat('docker', 'run',
                                   '--entrypoint=' + self._entryPoint(),
                                   '--net=host',
                                   '-i',
                                   '--name=' + self.containerName,
                                   ['--volume=%s:%s' % mount for mount in iteritems(self.mounts)],
                                   image,
                                   self._containerCommand()))
                log.info('Running %r', args)
                self.popen = subprocess.Popen(args)
            self.start()
            self.__wait_running()
            return self

        # noinspection PyUnusedLocal
        def __exit__(self, exc_type, exc_val, exc_tb):
            try:
                try:
                    self.outer._run('docker', 'stop', self.containerName)
                    self.join()
                finally:
                    if self.cleanMounts:
                        self.__cleanMounts()
            finally:
                self.outer._run('docker', 'rm', '-f', self.containerName)
            return False  # don't swallow exception

        def __wait_running(self):
            log.info("Waiting for %s container process to appear. "
                     "Expect to see 'Error: No such image or container'.", self._getRole())
            while self.isAlive():
                try:
                    running = self.outer._run('docker', 'inspect',
                                              '--format={{ .State.Running }}',
                                              self.containerName,
                                              capture=True).strip()
                except subprocess.CalledProcessError:
                    pass
                else:
                    if 'true' == running:
                        break
                time.sleep(1)

        def __cleanMounts(self):
            """
            Deletes all files in every mounted directory. Without this step, we risk leaking
            files owned by root on the host. To avoid races, this method should be called after
            the appliance container was stopped, otherwise the running container might still be
            writing files.
            """
            # Delete all files within each mounted directory, but not the directory itself.
            cmd = 'shopt -s dotglob && rm -rf ' + ' '.join(v + '/*'
                                                           for k, v in iteritems(self.mounts)
                                                           if os.path.isdir(k))
            self.outer._run('docker', 'run',
                            '--rm',
                            '--entrypoint=/bin/bash',
                            applianceSelf(),
                            '-c',
                            cmd)

        def tryRun(self):
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)

        def runOnAppliance(self, *args, **kwargs):
            # Check if thread is still alive. Note that ExceptionalThread.join raises the
            # exception that occurred in the thread.
            self.join(timeout=0)
            # noinspection PyProtectedMember
            self.outer._run('docker', 'exec', '-i', self.containerName, *args, **kwargs)

        def writeToAppliance(self, path, contents):
            self.runOnAppliance('tee', path, input=contents)

        def deployScript(self, path, packagePath, script):
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
            packagePath = packagePath.split('.')
            packages, module = packagePath[:-1], packagePath[-1]
            for package in packages:
                path += '/' + package
                self.runOnAppliance('mkdir', '-p', path)
                self.writeToAppliance(path + '/__init__.py', '')
            self.writeToAppliance(path + '/' + module + '.py', script)

    class LeaderThread(Appliance):
        def _entryPoint(self):
            return 'mesos-master'

        def _getRole(self):
            return 'leader'

        def _containerCommand(self):
            return ['--registry=in_memory',
                    '--ip=127.0.0.1',
                    '--port=5050',
                    '--allocation_interval=500ms']

    class WorkerThread(Appliance):
        def __init__(self, outer, mounts, numCores):
            self.numCores = numCores
            super(ApplianceTestSupport.WorkerThread, self).__init__(outer, mounts)

        def _entryPoint(self):
            return 'mesos-slave'

        def _getRole(self):
            return 'worker'

        def _containerCommand(self):
            return ['--work_dir=/var/lib/mesos',
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--attributes=preemptable:False',
                    '--resources=cpus(*):%i' % self.numCores,
                    '--no-hostname_lookup',
                    '--no-systemd_enable_support']
