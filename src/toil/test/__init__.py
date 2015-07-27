import logging
import os
import shlex
import unittest
import sys

from toil.common import toilPackageDirPath
from toil.lib.bioio import getBasicOptionParser, parseSuiteTestOptions

log = logging.getLogger(__name__)


class ToilTest(unittest.TestCase):
    """
    A common base class for our tests. Please have every test case directly or indirectly inherit this one.
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


