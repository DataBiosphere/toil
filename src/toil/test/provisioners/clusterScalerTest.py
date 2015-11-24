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
from toil.test import ToilTest
from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystemInterface
from toil.provisioners.abstractProvisioner import AbstractProvisioner
from toil.leader import ClusterScaler
from toil.common import Config
import time
from threading import Thread, Event
from Queue import Queue, Empty
#from multiprocessing import Process as Thread
#from multiprocessing import Event
#from multiprocessing import Queue
#from Queue import Empty
import logging

logger = logging.getLogger( __name__ )

#Temporary log stuff - needs to be move to test setup
logger.setLevel(logging.DEBUG)
import sys
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)

class ClusterScalerTest(ToilTest):
    def clusterScalerTests(self, config, preemptableJobs, nonPreemptableJobs):
        """
        Creates a simple, dummy scalable batch system interface / provisioner class and 
        uses this to test the ClusterScaler class through a series of 
        tests with different patterns of job creation. Tests ascertain that
        autoscaling occurs and that all the 'jobs' are run.
        """
        
        class Dummy(AbstractScalableBatchSystemInterface, AbstractProvisioner):
            """
            Class that mimics a scalable batch system / provisioner
            """
            def __init__(self, config, secondsPerJob):
                # To mimic parallel preemptable and non-preemptable queues
                # for jobs we create two parallel instances of the following class
                class DummyScalingBatchSystem():
                    """
                    This class implements the methods of AbstractScalableBatchSystemInterface
                    and AbstractProvisioner, but omits the preemptable flag.
                    """
                    def __init__(self):
                        self.jobQueue = Queue()
                        self.totalJobs = 0 #Count of total jobs processed
                        self.totalWorkerTime = 0.0 #Total time spent in worker threads
                        self.workers = [] #Instances of the Worker class
                        self.maxWorkers = 0 #Maximum number of workers
                    
                    def addJob(self):
                        """
                        Add a job to the job queue
                        """
                        self.totalJobs += 1
                        self.jobQueue.put(None)
                    
                    # Methods implementing the AbstractScalableBatchSystemInterface class
            
                    def getIssuedQueueSize(self):
                        return self.jobQueue.qsize()
                    
                    def numberOfRecentJobsStartedPerSecond(self):
                        return (1.0/secondsPerJob) * len(self.workers) #jobs / worker / second = 1.0 / secondsPerJob
                    
                    def getNumberOfEmptyNodes(self):
                        return sum(map(lambda w : 0 if w.busyEvent.is_set() else 1, self.workers))
                    
                    # Methods implementing the AbstractProvisioner class
                    
                    def addNodes(self, nodes=1):
                        class Worker():
                            def __init__(self, jobQueue):
                                self.busyEvent = Event()
                                self.stopEvent = Event()
                                def workerFn():
                                    while True:
                                        if self.stopEvent.is_set():
                                            return
                                        try:
                                            jobQueue.get(timeout=1.0)
                                        except Empty:
                                            continue
                                        self.busyEvent.set()
                                        time.sleep(secondsPerJob)
                                        self.busyEvent.clear()
                                
                                self.startTime = time.time()
                                self.worker = Thread(target=workerFn)
                                self.worker.start()
                                
                            def stop(self):
                                self.stopEvent.set()
                                self.worker.join()
                                return time.time() - self.startTime
                            
                        for i in xrange(nodes):
                            self.workers.append(Worker(self.jobQueue))
                        if len(self.workers) > self.maxWorkers:
                            self.maxWorkers = len(self.workers)
                    
                    def removeNodes(self, nodes=1):
                        while len(self.workers) > 0 and nodes > 0:
                            worker = self.workers.pop()
                            self.totalWorkerTime += worker.stop()
                            nodes -= 1
                            
                    def numberOfWorkers(self):
                        return len(self.workers)
                    
                self.preemptableDummyBatchSystem = DummyScalingBatchSystem()
                self.nonPreemptableDummyBatchSystem = DummyScalingBatchSystem()
                    
            def _pick(self, preemptable=False):
                # Select the preemptable or nonpremptable instance of the the 
                # DummyScalingBatchSystem instance
                return self.preemptableDummyBatchSystem if preemptable else self.nonPreemptableDummyBatchSystem
                    
            def addJob(self, preemptable=False):
                self._pick(preemptable).addJob()
            
            #AbstractScalableBatchSystemInterface methods
                    
            def getIssuedQueueSize(self, preemptable=False):
                return self._pick(preemptable).getIssuedQueueSize()
                    
            def numberOfRecentJobsStartedPerSecond(self, preemptable=False):
                return self._pick(preemptable).numberOfRecentJobsStartedPerSecond()
            
            def getNumberOfEmptyNodes(self, preemptable=False):
                return self._pick(preemptable).getNumberOfEmptyNodes()
                
            #AbstractProvisioner methods
            
            def addNodes(self, nodes=1, preemptable=False):
                self._pick(preemptable).addNodes(nodes=nodes)
            
            def removeNodes(self, nodes=1, preemptable=False):
                self._pick(preemptable).removeNodes(nodes=nodes)
                    
            def numberOfWorkers(self, preemptable=False):
                return self._pick(preemptable).numberOfWorkers()

        # First do simple test of creating 100 preemptable and non-premptable jobs
        # and check the jobs are completed okay, then print the amount of worker
        # time expended and the total number of worker nodes used.
        
        logger.info("Creating dummy batch system and scalar")

        dummy = Dummy(config, secondsPerJob=2.0)
        clusterScaler = ClusterScaler(dummy, dummy, config)
        
        # Add 100 jobs to complete 
        logger.info("Creating test jobs")
        map(lambda x : dummy.addJob(), range(nonPreemptableJobs))
        map(lambda x : dummy.addJob(preemptable=True), range(preemptableJobs))
        
        logger.info("Waiting for jobs to be processed")
        startTime = time.time()
        # Wait while the cluster the process chunks through the jobs
        while (dummy.getIssuedQueueSize(preemptable=False) > 0 or 
               dummy.getIssuedQueueSize(preemptable=True) > 0 or
               dummy.numberOfWorkers() > 0 or dummy.numberOfWorkers(preemptable=True) > 0):
            logger.info("Running, non-preemptable queue size: %s, non-preemptable workers: %s"
                        ", preemptable queue size: %s, preemptable workers: %s" % 
                        (dummy.getIssuedQueueSize(preemptable=False), dummy.numberOfWorkers(preemptable=False),
                         dummy.getIssuedQueueSize(preemptable=True), dummy.numberOfWorkers(preemptable=True)))
            time.sleep(0.5)
        logger.info("We waited %s for cluster to finish" % (time.time() - startTime))
        clusterScaler.shutdown()
        
        #Print some info about the autoscaling
        def report(preemptable, x):
            logger.info("Preemptable: %s, Total-jobs: %s: Max-workers: %s,"
                                    " Total-worker-time: %s, Worker-time-per-job: %s" %
                                    (preemptable, x.totalJobs, x.maxWorkers,
                                     x.totalWorkerTime, x.totalWorkerTime/x.totalJobs if x.totalJobs > 0 else 0.0))
        report(True, dummy.preemptableDummyBatchSystem)
        report(False, dummy.nonPreemptableDummyBatchSystem)

    def testClusterScaler_NoPreemptableJobs(self):
        """
        Test scaling for a batch of non-preemptable jobs and no preemptable jobs 
        (makes debugging easier).
        """
        config = Config()
        #Premptable options
        config.maxPreemptableNodes = 0
        #Non preemptable options
        config.minNonPreemptableNodes = 0
        config.maxNonPreemptableNodes = 100
        config.minNonPreemptableTimeToRun = 10.0
        config.maxNonPreemptableTimeToRun = 20.0
        self.clusterScalerTests(config, preemptableJobs=0, nonPreemptableJobs=100)
        
    def testClusterScaler_PreemptableAndNonPreemptableJobs(self):
        """
        Test scaling simultaneously for a batch of preemptable and non-preemptable jobs.
        """
        config = Config()
        #Premptable options
        config.minPreemptableNodes = 0
        config.maxPreemptableNodes = 100
        config.minPreemptableTimeToRun = 1.0
        config.maxPreemptableTimeToRun = 5.0
        #Non preemptable options
        config.minNonPreemptableNodes = 0
        config.maxNonPreemptableNodes = 20
        config.minNonPreemptableTimeToRun = 10.0
        config.maxNonPreemptableTimeToRun = 20.0
        self.clusterScalerTests(config, preemptableJobs=100, nonPreemptableJobs=100)
