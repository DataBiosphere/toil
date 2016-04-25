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

import os
import sys
from subprocess import CalledProcessError, check_call

import toil
import toil.test.sort.sort
from toil import resolveEntryPoint
from toil.job import Job
from toil.lib.bioio import getTempFile
from toil.lib.bioio import system
from toil.test import ToilTest
from toil.test.sort.sortTest import makeFileToSort
from toil.utils.toilStats import getStats, processData
from toil.common import Toil


class UtilsTest(ToilTest):
    """
    Tests the utilities that toil ships with, e.g. stats and status, in conjunction with restart
    functionality.
    """

    def setUp(self):
        super(UtilsTest, self).setUp()
        self.tempDir = self._createTempDir()
        self.tempFile = getTempFile(rootDir=self.tempDir)
        self.outputFile = getTempFile(rootDir=self.tempDir)
        self.toilDir = os.path.join(self.tempDir, "jobstore")
        self.assertFalse(os.path.exists(self.toilDir))
        self.lines = 1000
        self.lineLen = 10
        self.N = 1000
        makeFileToSort(self.tempFile, self.lines, self.lineLen)
        # First make our own sorted version
        with open(self.tempFile, 'r') as fileHandle:
            self.correctSort = fileHandle.readlines()
            self.correctSort.sort()

    def tearDown(self):
        ToilTest.tearDown(self)
        system("rm -rf %s" % self.tempDir)

    @property
    def toilMain(self):
        return resolveEntryPoint('toil')

    @property
    def cleanCommand(self):
        return [self.toilMain, 'clean', self.toilDir]

    @property
    def statsCommand(self):
        return [self.toilMain, 'stats', self.toilDir, '--pretty']

    @property
    def statusCommand(self):
        return [self.toilMain, 'status', self.toilDir, '--failIfNotComplete']

    def testUtilsSort(self):
        """
        Tests the status and stats commands of the toil command line utility using the
        sort example with the --restart flag.
        """
        # Get the sort command to run
        toilCommand = [sys.executable,
                       '-m', toil.test.sort.sort.__name__,
                       self.toilDir,
                       '--logLevel=DEBUG',
                       '--fileToSort', self.tempFile,
                       '--N', str(self.N),
                       '--stats',
                       '--retryCount=2',
                       '--badWorker=0.5',
                       '--badWorkerFailInterval=0.05']
        # Try restarting it to check that a JobStoreException is thrown
        self.assertRaises(CalledProcessError, system, toilCommand + ['--restart'])
        # Check that trying to run it in restart mode does not create the jobStore
        self.assertFalse(os.path.exists(self.toilDir))

        # Status command
        # Run the script for the first time
        try:
            system(toilCommand)
            finished = True
        except CalledProcessError:  # This happens when the script fails due to having unfinished jobs
            self.assertRaises(CalledProcessError, system, self.statusCommand)
            finished = False
        self.assertTrue(os.path.exists(self.toilDir))

        # Try running it without restart and check an exception is thrown
        self.assertRaises(CalledProcessError, system, toilCommand)

        # Now restart it until done
        totalTrys = 1
        while not finished:
            try:
                system(toilCommand + ['--restart'])
                finished = True
            except CalledProcessError:  # This happens when the script fails due to having unfinished jobs
                self.assertRaises(CalledProcessError, system, self.statusCommand)
                if totalTrys > 16:
                    self.fail()  # Exceeded a reasonable number of restarts
                totalTrys += 1

                # Check the toil status command does not issue an exception
        system(self.statusCommand)

        # Check if we try to launch after its finished that we get a JobException
        self.assertRaises(CalledProcessError, system, toilCommand + ['--restart'])

        # Check we can run 'toil stats'
        system(self.statsCommand)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEquals(self.correctSort, l2)

        # Check we can run 'toil clean'
        system(self.cleanCommand)

    def testUtilsStatsSort(self):
        """
        Tests the stats commands on a complete run of the stats test.
        """
        # Get the sort command to run
        toilCommand = [sys.executable,
                       '-m', toil.test.sort.sort.__name__,
                       self.toilDir,
                       '--logLevel=DEBUG',
                       '--fileToSort', self.tempFile,
                       '--N', str(self.N),
                       '--stats',
                       '--retryCount=99',
                       '--badWorker=0.5',
                       '--badWorkerFailInterval=0.01']

        # Run the script for the first time
        system(toilCommand)
        self.assertTrue(os.path.exists(self.toilDir))

        # Check we can run 'toil stats'
        system(self.statsCommand)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEquals(self.correctSort, l2)

    def testUnicodeSupport(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = 'always'
        options.logLevel = 'debug'
        Job.Runner.startToil(Job.wrapFn(printUnicodeCharacter), options)

    def testMultipleJobsPerWorkerStats(self):
        """
        Tests case where multiple jobs are run on 1 worker to insure that all jobs report back their data
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = 'never'
        options.stats = True
        Job.Runner.startToil(RunTwoJobsPerWorker(), options)

        jobStore = Toil.loadOrCreateJobStore(options.jobStore)
        stats = getStats(options)
        collatedStats =  processData(jobStore.config, stats, options)
        self.assertTrue(len(collatedStats.job_types)==2,"Some jobs are not represented in the stats")

def printUnicodeCharacter():
    # We want to get a unicode character to stdout but we can't print it directly because of
    # Python encoding issues. To work around this we print in a separate Python process. See
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    check_call([sys.executable, '-c', "print '\\xc3\\xbc'"])

class RunTwoJobsPerWorker(Job):
    """
    Runs child job with same resources as self in an attempt to chain the jobs on the same worker
    """
    def __init__(self):
        Job.__init__(self)

    def run(self, fileStore):
        self.addChildFn(printUnicodeCharacter)
