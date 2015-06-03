import logging
import os
import unittest

from jobTree.common import workflowRootPath


class JobTreeTest(unittest.TestCase):
    def getScriptPath(self, script_name):
        return os.path.join(workflowRootPath(), 'utils', script_name + '.py')

    @classmethod
    def setUpClass(cls):
        logging.getLogger().setLevel(level=logging.DEBUG)
        logging.getLogger('boto').setLevel(logging.INFO)
        super(JobTreeTest, cls).setUpClass()
