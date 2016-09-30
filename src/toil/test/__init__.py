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

from __future__ import absolute_import
import logging
import os
import subprocess
import tempfile
import threading
import unittest
import shutil
import re
import uuid
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from inspect import getsource
from textwrap import dedent
from unittest.util import strclass
from urllib2 import urlopen

import multiprocessing

import time
import signal
from bd2k.util import less_strict_bool, memoize
from bd2k.util.files import mkdir_p
from bd2k.util.iterables import concat
from bd2k.util.processes import which
from bd2k.util.threading import ExceptionalThread

from toil.version import version as toil_version

from toil import toilPackageDirPath

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

    @classmethod
    def setUpClass(cls):
        super(ToilTest, cls).setUpClass()
        cls._tempDirs = []
        tempBaseDir = os.environ.get('TOIL_TEST_TEMP', None)
        if tempBaseDir is not None and not os.path.isabs(tempBaseDir):
            tempBaseDir = os.path.abspath(os.path.join(cls._projectRootPath(), tempBaseDir))
            mkdir_p(tempBaseDir)
        cls._tempBaseDir = tempBaseDir

    @classmethod
    def awsRegion(cls):
        """
        Use us-west-2 unless running on EC2, in which case use the region in which
        the instance is located
        """
        if runningOnEC2():
            return cls._region()
        else:
            return 'us-west-2'


    @classmethod
    def _availabilityZone(cls):
        """
        Used only when running on EC2. Query this instance's metadata to determine
        in which availability zone it is running
        """
        return urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone').read()

    @classmethod
    @memoize
    def _region(cls):
        """
        Used only when running on EC2. Determines in what region this instance is running.
        The region will not change over the life of the instance so the result
        is memoized to avoid unnecessary work.
        """
        m = re.match(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$',cls._availabilityZone())
        assert m
        region = m.group(1)
        return region

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

    def _createTempDir(self, purpose=None):
        return self._createTempDirEx(self._testMethodName, purpose)

    @classmethod
    def _createTempDirEx(cls, *names):
        prefix = ['toil', 'test', strclass(cls)]
        prefix.extend(filter(None, names))
        prefix.append('')
        temp_dir_path = tempfile.mkdtemp(dir=cls._tempBaseDir, prefix='-'.join(prefix))
        cls._tempDirs.append(temp_dir_path)
        return temp_dir_path

    def tearDown(self):
        super(ToilTest, self).tearDown()
        log.info("Tore down %s", self.id())

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
        sdistPath = os.path.join(cls._projectRootPath(), 'dist', 'toil-%s.tar.gz' % toil_version)
        assert os.path.isfile(
            sdistPath), "Can't find Toil source distribution at %s. Run 'make sdist'." % sdistPath
        excluded = set(cls._run('git', 'ls-files', '--others', '-i', '--exclude-standard',
                                capture=True,
                                cwd=cls._projectRootPath()).splitlines())
        dirty = cls._run('find', 'src', '-type', 'f', '-newer', sdistPath,
                         capture=True,
                         cwd=cls._projectRootPath()).splitlines()
        assert all(path.startswith('src') for path in dirty)
        dirty = set(dirty)
        dirty.difference_update(excluded)
        assert not dirty, \
            "Run 'make clean_sdist sdist'. Files newer than %s: %r" % (sdistPath, list(dirty))
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


try:
    # noinspection PyUnresolvedReferences
    from _pytest.mark import MarkDecorator
except ImportError:
    # noinspection PyUnusedLocal
    def _mark_test(name, test_item):
        return test_item
else:
    def _mark_test(name, test_item):
        return MarkDecorator(name)(test_item)


def needs_spark(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Spark is usable.
    """
    test_item = _mark_test('spark', test_item)

    try:
        # noinspection PyUnresolvedReferences
        import pyspark
    except ImportError:
        return unittest.skip("Skipping test. Install PySpark to include this test.")(test_item)
    except:
        raise
    else:
        return test_item


def needs_aws(test_item):
    """
    Use as a decorator before test classes or methods to only run them if AWS usable.
    """
    test_item = _mark_test('aws', test_item)
    try:
        # noinspection PyUnresolvedReferences
        from boto import config
    except ImportError:
        return unittest.skip("Install toil with the 'aws' extra to include this test.")(test_item)
    except:
        raise
    else:
        dot_aws_credentials_path = os.path.expanduser('~/.aws/credentials')
        boto_credentials = config.get('Credentials', 'aws_access_key_id')
        if boto_credentials:
            return test_item
        if os.path.exists(dot_aws_credentials_path) or runningOnEC2():
            # Assume that EC2 machines like the Jenkins slave that we run CI on will have IAM roles
            return test_item
        else:
            return unittest.skip("Configure ~/.aws/credentials with AWS credentials to include "
                                 "this test.")(test_item)


def file_begins_with(path, prefix):
    with open(path) as f:
        return f.read(len(prefix)) == prefix


def runningOnEC2():
    hv_uuid_path = '/sys/hypervisor/uuid'
    return os.path.exists(hv_uuid_path) and file_begins_with(hv_uuid_path, 'ec2')


def needs_google(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Google Storage usable.
    """
    test_item = _mark_test('google', test_item)
    try:
        from boto import config
    except ImportError:
        return unittest.skip(
            "Install Toil with the 'google' extra to include this test.")(test_item)
    else:
        boto_credentials = config.get('Credentials', 'gs_access_key_id')
        if boto_credentials:
            return test_item
        else:
            return unittest.skip(
                "Configure ~/.boto with Google Cloud credentials to include this test.")(test_item)


def needs_azure(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Azure is usable.
    """
    test_item = _mark_test('azure', test_item)
    try:
        # noinspection PyUnresolvedReferences
        import azure.storage
    except ImportError:
        return unittest.skip("Install Toil with the 'azure' extra to include this test.")(test_item)
    except:
        raise
    else:
        from toil.jobStores.azureJobStore import credential_file_path
        full_credential_file_path = os.path.expanduser(credential_file_path)
        if not os.path.exists(full_credential_file_path):
            return unittest.skip("Configure %s with the access key for the 'toiltest' storage "
                                 "account." % credential_file_path)(test_item)
        return test_item


def needs_gridengine(test_item):
    """
    Use as a decorator before test classes or methods to only run them if GridEngine is installed.
    """
    test_item = _mark_test('gridengine', test_item)
    if next(which('qsub'), None):
        return test_item
    else:
        return unittest.skip("Install GridEngine to include this test.")(test_item)


def needs_mesos(test_item):
    """
    Use as a decorator before test classes or methods to only run them if the Mesos is installed
    and configured.
    """
    test_item = _mark_test('mesos', test_item)
    try:
        # noinspection PyUnresolvedReferences
        import mesos.native
    except ImportError:
        return unittest.skip(
            "Install Mesos (and Toil with the 'mesos' extra) to include this test.")(test_item)
    except:
        raise
    else:
        return test_item


def needs_parasol(test_item):
    """
    Use as decorator so tests are only run if Parasol is installed.
    """
    test_item = _mark_test('parasol', test_item)
    if next(which('parasol'), None):
        return test_item
    else:
        return unittest.skip("Install Parasol to include this test.")(test_item)


def needs_slurm(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Slurm is installed.
    """
    test_item = _mark_test('slurm', test_item)
    if next(which('squeue'), None):
        return test_item
    else:
        return unittest.skip("Install Slurm to include this test.")(test_item)


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
    except:
        raise
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
    except:
        raise
    else:
        return test_item


def experimental(test_item):
    """
    Use this to decorate experimental or brittle tests in order to skip them during regular builds.
    """
    # We'll pytest.mark_test the test as experimental but we'll also unittest.skip it via an 
    # environment variable. 
    test_item = _mark_test('experimental', test_item)
    if less_strict_bool(os.getenv('TOIL_TEST_EXPERIMENTAL')):
        return test_item
    else:
        return unittest.skip(
            'Set TOIL_TEST_EXPERIMENTAL="True" to include this experimental test.')(test_item)


def integrative(test_item):
    """
    Use this to decorate integration tests so as to skip them during regular builds. We define
    integration tests as A) involving other, non-Toil software components that we develop and/or
    B) having a higher cost (time or money). Note that brittleness does not qualify a test for
    being integrative. Neither does involvement of external services such as AWS, since that
    would cover most of Toil's test.
    """
    # We'll pytest.mark_test the test as integrative but we'll also unittest.skip it via an
    # environment variable.
    test_item = _mark_test('integrative', test_item)
    if less_strict_bool(os.getenv('TOIL_TEST_INTEGRATIVE')):
        return test_item
    else:
        return unittest.skip(
            'Set TOIL_TEST_INTEGRATIVE="True" to include this integration test.')(test_item)


methodNamePartRegex = re.compile('^[a-zA-Z_0-9]+$')


@contextmanager
def timeLimit(seconds):
    """
    http://stackoverflow.com/a/601168
    Use to limit the execution time of a function. Raises an exception if the execution of the
    function takes more than the specified amount of time.

    :param seconds: maximum allowable time, in seconds
    >>> import time
    >>> with timeLimit(5):
    ...    time.sleep(4)
    >>> import time
    >>> with timeLimit(5):
    ...    time.sleep(6)
    Traceback (most recent call last):
        ...
    RuntimeError: Timed out
    """
    def signal_handler(signum, frame):
        raise RuntimeError('Timed out')

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


# FIXME: move to bd2k-python-lib


def make_tests(generalMethod, targetClass=None, **kwargs):
    """
    This method dynamically generates test methods using the generalMethod as a template. Each generated
    function is the result of a unique combination of parameters applied to the generalMethod. Each of the
    parameters has a corresponding string that will be used to name the method. These generated functions
    are named in the scheme:
    test_[generalMethodName]___[firstParamaterName]_[someValueName]__[secondParamaterName]_...

    The arguments following the generalMethodName should be a series of one or more dictionaries of the form
    {str : type, ...} where the key represents the name of the value. The names will be used to represent the
    permutation of values passed for each parameter in the generalMethod.

    :param generalMethod: A method that will be parametrized with values passed as kwargs. Note that the
        generalMethod must be a regular method.

    :param targetClass: This represents the class to which the generated test methods will be bound. If no
        targetClass is specified the class of the generalMethod is assumed the target.

    :param kwargs: a series of dictionaries defining values, and their respective names where each keyword is
        the name of a parameter in generalMethod.

    >>> class Foo:
    ...     def has(self, num, letter):
    ...         return num, letter
    ...
    ...     def hasOne(self, num):
    ...         return num

    >>> class Bar(Foo):
    ...     pass

    >>> make_tests(Foo.has, targetClass=Bar, num={'one':1, 'two':2}, letter={'a':'a', 'b':'b'})

    >>> b = Bar()

    >>> assert b.test_has__num_one__letter_a() == b.has(1, 'a')

    >>> assert b.test_has__num_one__letter_b() == b.has(1, 'b')

    >>> assert b.test_has__num_two__letter_a() == b.has(2, 'a')

    >>> assert b.test_has__num_two__letter_b() == b.has(2, 'b')

    >>> f = Foo()

    >>> hasattr(f, 'test_has__num_one__letter_a')  # should be false because Foo has no test methods
    False

    >>> make_tests(Foo.has, num={'one':1, 'two':2}, letter={'a':'a', 'b':'b'})

    >>> hasattr(f, 'test_has__num_one__letter_a')
    True

    >>> assert f.test_has__num_one__letter_a() == f.has(1, 'a')

    >>> assert f.test_has__num_one__letter_b() == f.has(1, 'b')

    >>> assert f.test_has__num_two__letter_a() == f.has(2, 'a')

    >>> assert f.test_has__num_two__letter_b() == f.has(2, 'b')

    >>> make_tests(Foo.hasOne, num={'one':1, 'two':2})

    >>> assert f.test_hasOne__num_one() == f.hasOne(1)

    >>> assert f.test_hasOne__num_two() == f.hasOne(2)

    """

    def pop(d):
        """
        Pops an arbitrary key value pair from the dict
        :param d: a dictionary
        :return: the popped key, value tuple
        """
        k, v = next(kwargs.iteritems())
        del d[k]
        return k, v

    def permuteIntoLeft(left, rPrmName, right):
        """
        Permutes values in right dictionary into each parameter: value dict pair in the left dictionary.
        Such that the left dictionary will contain a new set of keys each of which is a combination of one of
        its original parameter-value names appended with some parameter-value name from the right dictionary.
        Each original key in the left is deleted from the left dictionary after the permutation of the key and
        every parameter-value name from the right has been added to the left dictionary.

        For example
        if left is {'__PrmOne_ValName':{'ValName':Val}} and right is {'rValName1':rVal1, 'rValName2':rVal2} then
        left will become
        {'__PrmOne_ValName__rParamName_rValName1':{'ValName':Val. 'rValName1':rVal1},
        '__PrmOne_ValName__rParamName_rValName2':{'ValName':Val. 'rValName2':rVal2}}

        :param left: A dictionary pairing each paramNameValue to a nested dictionary that contains each ValueName
            and value pair described in the outer dict's paramNameValue key.
        :param rParamName: The name of the parameter that each value in the right dict represents.
        :param right: A dict that pairs 1 or more valueNames and values for the rParamName parameter.
        """
        for prmValName, lDict in left.items():
            for rValName, rVal in right.items():
                nextPrmVal = ('__%s_%s' % (rPrmName, rValName.lower()))
                if methodNamePartRegex.match(nextPrmVal) is None:
                    raise RuntimeError("The name '%s' cannot be used in a method name" % pvName)
                aggDict = dict(lDict)
                aggDict[rPrmName] = rVal
                left[prmValName + nextPrmVal] = aggDict
            left.pop(prmValName)

    def insertMethodToClass():
        """
        Generates and inserts test methods.
        """

        def fx(self, prms=prms):
            if prms is not None:
                return generalMethod(self, **prms)
            else:
                return generalMethod(self)

        setattr(targetClass, 'test_%s%s' % (generalMethod.__name__, prmNames), fx)

    if len(kwargs) > 0:
        # create first left dict
        left = {}
        prmName, vals = pop(kwargs)
        for valName, val in vals.items():
            pvName = '__%s_%s' % (prmName, valName.lower())
            if methodNamePartRegex.match(pvName) is None:
                raise RuntimeError("The name '%s' cannot be used in a method name" % pvName)
            left[pvName] = {prmName: val}

        # get cartesian product
        while len(kwargs) > 0:
            permuteIntoLeft(left, *pop(kwargs))

        # set class attributes
        targetClass = targetClass or generalMethod.im_class
        for prmNames, prms in left.items():
            insertMethodToClass()
    else:
        prms = None
        prmNames = ""
        insertMethodToClass()


@contextmanager
def tempFileContaining(content, suffix=''):
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        os.write(fd, content)
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
               as in -v KEY:VALUE

        :param int numCores: The number of cores to be offered by the Mesos slave process running
               in the worker container.

        :rtype: (ApplianceTestSupport.Appliance, ApplianceTestSupport.Appliance)

        :return: A tuple of the form `(leader, worker)` containing the Appliance instances
                 representing the respective appliance containers
        """
        if numCores is None:
            numCores = multiprocessing.cpu_count()
        with self.LeaderThread(self, mounts) as leader:
            with self.WorkerThread(self, mounts, numCores) as worker:
                yield leader, worker

    class Appliance(ExceptionalThread):
        __metaclass__ = ABCMeta

        @abstractmethod
        def _getRole(self):
            return 'leader'

        @abstractmethod
        def _containerCommand(self):
            raise NotImplementedError

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, outer, mounts):
            super(ApplianceTestSupport.Appliance, self).__init__()
            self.outer = outer
            self.mounts = mounts
            self.containerName = str(uuid.uuid4())
            self.popen = None

        def __enter__(self):
            with self.lock:
                image = self._getApplianceImage()
                # Omitting --rm, it's unreliable, see https://github.com/docker/docker/issues/16575
                args = list(concat('docker', 'run',
                                   '--net=host',
                                   '-i',
                                   '--name=' + self.containerName,
                                   ['--volume=%s:%s' % mount for mount in self.mounts.iteritems()],
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
                subprocess.check_call(['docker', 'stop', self.containerName])
                self.join()
            finally:
                subprocess.check_call(['docker', 'rm', '-f', self.containerName])
            return False  # don't swallow exception

        def __wait_running(self):
            log.info("Waiting for %s container process to appear. "
                     "Expect to see 'Error: No such image or container'.", self._getRole())
            while self.isAlive():
                try:
                    running = subprocess.check_output(
                        ['docker', 'inspect', '--format={{ .State.Running }}',
                         self.containerName]).strip()
                except subprocess.CalledProcessError:
                    pass
                else:
                    if 'true' == running:
                        break
                time.sleep(1)

        def _getApplianceImage(self):
            image, tag = os.environ['TOIL_APPLIANCE_SELF'].rsplit(':', 1)
            return image + '-' + self._getRole() + ':' + tag

        def tryRun(self):
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)

        def runOnAppliance(self, *args, **kwargs):
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
                script = dedent('\n'.join(getsource(script).split('\n')[1:]))
            packagePath = packagePath.split('.')
            packages, module = packagePath[:-1], packagePath[-1]
            for package in packages:
                path += '/' + package
                self.runOnAppliance('mkdir', path)
                self.writeToAppliance(path + '/__init__.py', '')
            self.writeToAppliance(path + '/' + module + '.py', script)

    class LeaderThread(Appliance):
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

        def _getRole(self):
            return 'worker'

        def _containerCommand(self):
            return ['--work_dir=/var/lib/mesos',
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--attributes=preemptable:False',
                    '--resources=cpus(*):%i' % self.numCores]
