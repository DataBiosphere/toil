# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import unittest
import shutil
import re
import subprocess

from bd2k.util.files import mkdir_p

from toil.common import toilPackageDirPath

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
        prefix = ['toil', 'test', self.id()]
        if purpose: prefix.append(purpose)
        prefix.append('')
        temp_dir_path = tempfile.mkdtemp(dir=self._tempBaseDir, prefix='-'.join(prefix))
        self._tempDirs.append(temp_dir_path)
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


def needs_aws(test_item):
    """
    Use as a decorator before test classes or methods to only run them if AWS usable.
    """
    try:
        # noinspection PyUnresolvedReferences
        import boto
    except ImportError:
        return unittest.skip("Skipping test. "
                             "Install and configure Boto to include this test.")(test_item)
    except:
        raise
    else:
        dot_boto_path = os.path.expanduser('~/.boto')
        hv_uuid_path = '/sys/hypervisor/uuid'
        if os.path.exists(dot_boto_path) \
                or os.path.exists(hv_uuid_path) \
                        and open(hv_uuid_path).read().startswith('ec2'):
            return test_item
        else:
            return unittest.skip("Skipping test. "
                                 "Create ~/.boto to include this test.")(test_item)


def needs_azure(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Azure is usable.
    """
    try:
        # noinspection PyUnresolvedReferences
        import azure.storage
    except ImportError:
        return unittest.skip("Skipping test. Install and configure the Azure Storage Python SDK to "
                             "include this test.")(test_item)
    except:
        raise
    else:
        from toil.jobStores.azureJobStore import credential_file_path
        full_credential_file_path = os.path.expanduser(credential_file_path)
        if not os.path.exists(full_credential_file_path):
            return unittest.skip("Skipping test. Configure %s with the access key for the "
                                 "'toiltest' storage account." % credential_file_path)(test_item)
        return test_item

def needs_gridengine(test_item):
    """
    Use as a decorator before test classes or methods to only run them if GridEngine is installed.
    """
    try:
        with open('/dev/null','a') as dev_null:
            subprocess.Popen('qsub',stdout=dev_null,stderr=dev_null)
    except OSError:
        return unittest.skip("Skipping test. Install GridEngine to include this test.")(test_item)
    except:
        raise
    else:
        return test_item


def needs_mesos(test_item):
    """
    Use as a decorator before test classes or methods to only run them if the Mesos is installed
    and configured.
    """
    try:
        # noinspection PyUnresolvedReferences
        import mesos.native
    except ImportError:
        return unittest.skip("Skipping test. Install Mesos to include this test.")(test_item)
    except:
        raise
    else:
        return test_item


def needs_parasol(test_item):
    """
    Use as decorator so tests are only run if Parasol is installed.
    """
    try:
        with open('/dev/null','a') as dev_null:
            subprocess.Popen('parasol',stdout=dev_null,stderr=dev_null)
    except OSError:
        return unittest.skip("Skipping test. Install Parasol to include this test.")(test_item)
    except:
        raise
    else:
        return test_item
