# Copyright (C) 2016 UC Berkeley AMPLab
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
from subprocess import check_output
from unittest import skip

from toil.common import Toil
from toil.job import Job
from toil.lib.spark import spawn_spark_cluster
from toil.test import ToilTest, needs_spark

def _count(job, workers):
    
    # if we are on Mac OS X and using docker-machine to run docker, we need to
    # get the IP of the docker-machine box
    #
    # this is necessary because docker-machine runs docker in a virtualbox
    # vm which has a different IP address from localhost
    ip = None
    if os.uname()[0] == "Darwin":
        # check what machines docker-machine is running
        # strip leading and trailing whitespace, and split lines
        machines = check_output(["docker-machine", "ls"]).strip().rstrip().split("\n")
        
        # we take the first docker-machine environment that is running
        # this means two lines including the header
        if len(machines) != 2:
            raise RuntimeError('Expected a single docker-machine to be running.'
                               'Got %d:\n%r.' % (len(machines) - 1, machines))

        machine = machines[1].split()[0]
        ip = check_output(["docker-machine", "ip", machine]).strip().rstrip()

    # set up cluster
    masterHostname = spawn_spark_cluster(job,
                                   False,
                                   workers,
                                   cores=1,
                                   overrideLeaderIP=ip)

    job.addChildJobFn(_count_child, masterHostname)

def _count_child(job, masterHostname):

    # noinspection PyUnresolvedReferences
    from pyspark import SparkContext

    # start spark context and connect to cluster
    sc = SparkContext(master='spark://%s:7077' % masterHostname,
                      appName='count_test')

    # create an rdd containing 0-9999 split across 10 partitions
    rdd = sc.parallelize(xrange(10000), 10)
    
    # and now, count it
    assert rdd.count() == 10000

repeats = 10
failureRate = 0.1

class SparkTest(ToilTest):

    def wordCount(self,
                  badWorker=0.0,
                  badWorkerFailInterval=0.05,
                  checkpoint = True):

        # wrap _count as a job
        countJob = Job.wrapJobFn(_count, 1, checkpoint = checkpoint)
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.batchSystem = 'singleMachine'
        options.badWorker = badWorker
        options.badWorkerFailInterval = badWorkerFailInterval
        options.clean = 'never'

        Job.Runner.startToil(countJob, options)
    
    @needs_spark
    def testSparkLocal(self):

        self.wordCount()

    @skip('fails due to docker container shutdown issue, see #987')
    def testSparkLocalWithBadWorkerAndCheckpoint(self):

        for i in xrange(repeats):
            self.wordCount(badWorker=failureRate)

    @skip('fails due to docker container shutdown issue, see #987')
    def testSparkLocalWithBadWorkerNoCheckpoint(self):

        # this should fail
        failed = False
        try:
            for i in xrange(repeats):
                self.wordCount(badWorker=failureRate, checkpoint=False)

        except:
            failed = True

        assert failed
        
