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
from __future__ import absolute_import, print_function
import random
import os
import tempfile

from toil.job import Job
from toil.test import ToilTest

class JobCacheEjectionTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """
    def testCacheEjection(self):
        """
        Test cache always always ejects least recently created file
        """
        # Makes three jobs that create an output file each which they write to filestore.  The combined size of any two
        # files is always less that cacheSize but the combined size of all 3 is always more so 1 file always has to be
        # ejected. Test to ensure that A is always ejected regardless of size.
        #  Make a temp directory for the test
        test_dir = self._createTempDir()
        for test in xrange(10):
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "DEBUG"
            options.cacheSize = 100000
            options.retryCount=100
            options.badWorker=0.5
            options.badWorkerFailInterval = 1.0
            # Create a temp file to write teh test results
            handle, logfile = tempfile.mkstemp(dir=test_dir)
            os.close(handle)
            file_sizes = [50000, 40000, 30000]
            # Randomize to (potentially) test all combinations
            random.shuffle(file_sizes)
            # Run the workflow. A, B and C do teh cache operations, and D prints test status to tempFile
            A = Job.wrapJobFn(fileTestJob, file_sizes[0])
            B = Job.wrapJobFn(fileTestJob, file_sizes[0])
            C = Job.wrapJobFn(fileTestJob, file_sizes[0])
            D = Job.wrapJobFn(fileTestCache, A.rv(), B.rv(), C.rv(), logfile)
            A.addChild(B)
            B.addChild(C)
            C.addChild(D)
            Job.Runner.startToil(A, options)
            #  Assert jobs passed by reading test results from tempFile
            with open(logfile, 'r') as outfile:
                for test_status in outfile:
                    assert test_status.strip() == 'True'



def fileTestCache(job, file_a, file_b, file_c, logfile):
    """
    Test job exercises Job.FileStore functions
    """
    with open(logfile, 'w') as lf:
        print(file_a not in job.fileStore._jobStoreFileIDToCacheLocation.items(), file=lf)
        print(file_b not in job.fileStore._jobStoreFileIDToCacheLocation.items(), file=lf)
        print(file_c not in job.fileStore._jobStoreFileIDToCacheLocation.items(), file=lf)

def fileTestJob(job, file_size):
    """
    Test job exercises Job.FileStore functions
    """
    def randomString(str_len):
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        s = "".join(map(lambda i : random.choice(chars), xrange(str_len)))
        return s
    if random.random() > 0.5:
        #Make a local copy of the file
        tempFile = job.fileStore.getLocalTempFile()
    else:
        tempDir = job.fileStore.getLocalTempDir()
        handle, tempFile = tempfile.mkstemp(dir=tempDir)
        os.close(handle)
    with open(tempFile, 'w') as fH:
        fH.write(os.urandom(file_size))
    job.fileStore.writeGlobalFile(tempFile)
    return fH.name

