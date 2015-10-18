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
import sys

from toil.job import Job
from toil.test import ToilTest

PREFIX_LENGTH = 200
CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

class JobFileStoreTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """

    def testCacheGreaterThanFileSize(self):
        """
        Test read/writeGlobalFile if cacheSize >> filesize
        """
        #  This test Creates a chain of jobs, each job
        #       a) accepts a jobStoreID and the string contained in the file held in that jobStoreID
        #       b) reads the file then asserts whether the string and file contents match
        #       c) create a new 100K character string and write to a file in a folder obtained with getLocalTempDir
        #       d) write file to filestore and collect JobStoreID
        #       e) pass jobStoreID and string to next job
        #  Since we (read Arjun) cannot elegantly catch the RuntimeError thrown by worker threads, we write the value
        #  of the test to a file and read the test result from there instead.
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        options.cacheSize = 1000000 # = 10 x filesize
        options.retryCount=0
        options.badWorker=0.5
        options.badWorkerFailInterval = 1.0
        logfile = 'cache_GTFS_logfile.txt'
        # Run the workflow, the return value being the number of failed jobs
        A = Job.wrapJobFn(fileTestJob, (None, None, 10, logfile))
        Job.Runner.startToil(A, options)
        job_passed = True
        with open(logfile, 'r') as lf:
            for line in lf:
                if line.strip() == 'False':
                    job_passed=False
                    break
        os.remove(logfile)
        assert job_passed

    def testCacheLessThanFileSize(self):
        """
        Test read/writeGlobalFile if cacheSize << filesize
        """
        #  This test Creates a chain of jobs, each job
        #       a) accepts a jobStoreID and the string contained in the file held in that jobStoreID
        #       b) reads the file then asserts whether the string and file contents match
        #       c) create a new 100K character string and write to a file in a folder obtained with getLocalTempDir
        #       d) write file to filestore and collect JobStoreID
        #       e) pass jobStoreID and string to next job
        #  Since we (read Arjun) cannot elegantly catch the RuntimeError thrown by worker threads, we write the value
        #  of the test to a file and read the test result from there instead.
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        options.cacheSize = 10000 # = 0.1 x filesize
        options.retryCount=0
        options.badWorker=0.5
        options.badWorkerFailInterval = 1.0
        logfile = 'cache_LTFS_logfile.txt'
        # Run the workflow, the return value being the number of failed jobs
        A = Job.wrapJobFn(fileTestJob, (None, None, 5, logfile))
        Job.Runner.startToil(A, options)
        job_passed = True
        with open(logfile, 'r') as lf:
            for line in lf:
                if line.strip() == 'False':
                    job_passed=False
                    break
        os.remove(logfile)
        assert job_passed


def fileTestJob(job, inargs):
    """
    Test job exercises Job.FileStore functions
    """
    # split inargs
    input_fs_id, test_string, depth, logfile = inargs
    #  Make a local directory to store the file passed by inputFileStoreID
    work_dir = job.fileStore.getLocalTempDir()
    # If an inputFileStoreID is provided, process else nvm
    if input_fs_id:
        #  Store to work_dir
        inputfile = ''.join([work_dir, '/'] +map(lambda i : random.choice(CHARS), xrange(10))+['.txt'])
        inputfile = job.fileStore.readGlobalFile(input_fs_id, inputfile)
        with open(inputfile, 'r') as fH:
            string = fH.readline()
        with open(logfile, 'a') as lf:
            print(test_string == string, file=lf)

    if depth == 0:
        return None
    # Get a new teststring and write it to a file.  Then write the file to the
    # filestore before calling another job that will test this new file check it gets the same results
    test_string = ''.join(map(lambda i : random.choice(CHARS), xrange(100000)))
    outfile_name = ''.join([work_dir, '/'] +map(lambda i : random.choice(CHARS), xrange(10))+['.txt'])
    with open(outfile_name, 'w') as outfile:
        outfile.write(test_string)
    outputfilestoreid = job.fileStore.writeGlobalFile(outfile.name)
    job.addChildJobFn(fileTestJob, (outputfilestoreid, test_string, depth-1, logfile))


        
        