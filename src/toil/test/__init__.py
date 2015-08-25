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
import shlex
import tempfile
import unittest
import sys
import uuid

from toil.common import toilPackageDirPath
from toil.lib.bioio import getBasicOptionParser, parseSuiteTestOptions

log = logging.getLogger(__name__)


class ToilTest(unittest.TestCase):
    """
    A common base class for our tests. Please have every test case directly or indirectly inherit
    this one.
    """

    orig_sys_argv = None

    def getScriptPath(self, script_name):
        return os.path.join(toilPackageDirPath(), 'utils', script_name + '.py')

    @classmethod
    def setUpClass(cls):
        super(ToilTest, cls).setUpClass()
        cls.orig_sys_argv = sys.argv[1:]
        sys.argv[1:] = shlex.split(os.environ.get('TOIL_TEST_ARGS', ""))
        parser = getBasicOptionParser()
        options, args = parseSuiteTestOptions(parser)
        sys.argv[1:] = args

    @classmethod
    def tearDownClass(cls):
        sys.argv[1:] = cls.orig_sys_argv
        super(ToilTest, cls).tearDownClass()

    def setUp(self):
        log.info("Setting up %s", self.id())
        super(ToilTest, self).setUp()

    def tearDown(self):
        super(ToilTest, self).tearDown()
        log.info("Tearing down down %s", self.id())

    def _getTestJobStorePath(self):
        return os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))


def needs_aws(test_item):
    """
    Use as a decorator before test classes or methods to only run them if AWS usable.
    """
    try:
        # noinspection PyUnresolvedReferences
        import boto
    except ImportError:
        return unittest.skip("Skipping test. Install and configure Boto to include this test.")(test_item)
    except:
        raise
    else:
        dot_boto_path = os.path.expanduser('~/.boto')
        hv_uuid_path = '/sys/hypervisor/uuid'
        if os.path.exists( dot_boto_path ) \
                or os.path.exists( hv_uuid_path ) \
                        and open( hv_uuid_path ).read( ).startswith( 'ec2' ):
            return test_item
        else:
            return unittest.skip( "Skipping test. Create ~/.boto to include this test." )( test_item)

def needs_azure(test_item):
    """
    Use as a decorator before test classes or methods to only run them if Azure is usable.
    """
    try:
        # noinspection PyUnresolvedReferences
        import azure.storage
    except ImportError:
        return unittest.skip("Skipping test. Install and configure the Azure Storage Python SDK to include this test.")(test_item)
    except:
        raise
    else:
        credentials_path = os.path.expanduser('~/.toilAzureCredentials')
        if not os.path.exists(credentials_path):
            return unittest.skip("Skipping test. Configure .toilAzureCredentials with the account key for 'toiltestaccount'.")(test_item)
        return test_item

def needs_mesos(test_item):
    """
    Use as a decorator before test classes or methods to only run them if the Mesos is installed and configured.
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
