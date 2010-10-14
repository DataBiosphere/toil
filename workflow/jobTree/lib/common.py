"""Wrapper functions for running the various programs in the jobTree package.
"""

from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import system

def runJobTreeStats(jobTree, outputFile):
    system("jobTreeStats --jobTree %s --outputFile %s" % (jobTree, outputFile))
    logger.info("Ran the job-tree stats command apparently okay")