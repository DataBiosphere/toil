import logging
import os
import shlex
import unittest
import sys

from jobTree.common import jobTreePackageDirPath
from jobTree.lib.bioio import getBasicOptionParser, parseSuiteTestOptions

log = logging.getLogger(__name__)


class JobTreeTest(unittest.TestCase):
    """
    A common base class for our tests. Please have every test case directly or indirectly inherit this one.
    """

    orig_sys_argv = None

    def getScriptPath(self, script_name):
        return os.path.join(jobTreePackageDirPath(), 'utils', script_name + '.py')

    @classmethod
    def setUpClass(cls):
        super(JobTreeTest, cls).setUpClass()
        cls.orig_sys_argv = sys.argv[1:]
        sys.argv[1:] = shlex.split(os.environ.get('JOBTREE_TEST_ARGS', ""))
        parser = getBasicOptionParser()
        options, args = parseSuiteTestOptions(parser)
        sys.argv[1:] = args

    @classmethod
    def tearDownClass(cls):
        sys.argv[1:] = cls.orig_sys_argv
        super(JobTreeTest, cls).tearDownClass()

    def setUp(self):
        log.info("Setting up %s", self.id())
        super(JobTreeTest, self).setUp()

    def tearDown(self):
        super(JobTreeTest, self).tearDown()
        log.info("Tearing down down %s", self.id())


