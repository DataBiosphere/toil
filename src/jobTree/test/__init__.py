import logging
import os
import unittest

from jobTree.common import workflowRootPath


class JobTreeTest(unittest.TestCase):
    """
    A common base class for our tests. Please have every test case directly or indirectly inherit this one.
    """

    def getScriptPath(self, script_name):
        return os.path.join(workflowRootPath(), 'utils', script_name + '.py')
