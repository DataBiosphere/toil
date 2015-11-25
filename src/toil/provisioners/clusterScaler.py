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
import logging
import time
import sys
from threading import Thread, Event
#from multiprocessing import Process as Thread
#from multiprocessing import Event
from toil.provisioners.abstractProvisioner import ProvisioningException

logger = logging.getLogger( __name__ )
logger.setLevel(logging.DEBUG)

class ClusterScaler(object):
    def __init__(self, provisioner, batchSystem, config):
        """
        Class manages automatically scaling the number of worker nodes. 
        
        :param toil.batchSystems.abstractBatchSystem.AbstractScalableBatchSystemInterface batchSystem: scalable batch system 
        interface class that implements methods for determining cluster scaling
        :param toil.provisioners.abstractProvisioner.AbstractProvisioner provisioner: 
        
        """
        self.stop = Event() #Event used to indicate that the scaling processes should shutdown
        self.error = Event() #Event used by processes to indicate failure
        
        if config.maxPreemptableNodes + config.maxNonPreemptableNodes == 0:
            raise RuntimeError("Trying to create a cluster that can have no workers!")
        
        #Create scaling process for preemptable nodes
        if config.maxPreemptableNodes > 0:
            args=(provisioner, batchSystem, 
                  config.minPreemptableNodes, config.maxPreemptableNodes,
                  config.minPreemptableTimeToRun, config.maxPreemptableTimeToRun,
                  self.stop, self.error, True)
            self.preemptableScaler = Thread(target=self.scaler, args=args)
            self.preemptableScaler.start()
        else:
            self.preemptableScaler = None
        
        #Create scaling process for non-preemptable nodes
        if config.maxNonPreemptableNodes > 0:
            args=(provisioner, batchSystem, 
                  config.minNonPreemptableNodes, config.maxNonPreemptableNodes,
                  config.minNonPreemptableTimeToRun, config.maxNonPreemptableTimeToRun,
                  self.stop, self.error, False)
            self.nonPreemptableScaler = Thread(target=self.scaler, args=args)
            self.nonPreemptableScaler.start()
        else:
            self.nonPreemptableScaler = None
        
    def shutdown(self):
        """
        Shutdown the cluster.
        """
        self.stop.set()
        if self.preemptableScaler != None:
            self.preemptableScaler.join()
        if self.nonPreemptableScaler != None:
            self.nonPreemptableScaler.join()
    
    @staticmethod
    def scaler(provisioner, batchSystem, 
               minNodes, maxNodes, minTimeToRun, maxTimeToRun,
               stop, error, preemptable):
        """
        Automatically scales the number of worker nodes according to the number
        of jobs queued and the average job queue time.
        
        Jobs are issued to the batch-system and then queue until they are started. 
        Let maxTimeToRun and minTimeToRun be the desired max and min, respectively, 
        amount of time each job should wait in the issued 
        queue before being run. This function, run in a separate process, 
        monitors a running approximation of the wait time, scaling the cluster up 
        and down accordingly so that the avg. wait time of the last N jobs stays 
        approximately between this max and min time.
    
        :param toil.provisioners.abstractProvisioner.AbstractProvisioner provisioner: Provisioner instance to scale.
        :param toil.batchSystem.abstractBatchSysten.AbstractBatchSystemInterface batchSystem: Scalable batchsystem interface to monitor to make scaling decisions.
        :param toil.common.Config config: Config object from which to draw parameters. 
        :param stop multiprocessing.Event: Event used to signify that the function should shut down the cluster and terminate.
        :param error multiprocessing.Event: Event used to signify that the function has terminated unexpectedly..
        :param boolean preemptable:  If True create/cleanup preemptable nodes, else create/cleanup non-preemptable nodes.
        """
        try:
            totalNodes = minNodes
            #Setup a minimal cluster
            if minNodes > 0:
                try:
                    provisioner.addNodes(nodes=minNodes, preemptable=preemptable)
                except ProvisioningException as e:
                    logger.debug("We tried to create a minimal cluster of %s nodes,"
                                 " but got a provisioning exception: %s" % (minNodes, e))
            while True:
                # First check if we've got the cleanup signal       
                if stop.is_set(): #Cleanup logic
                    try:
                        provisioner.removeNodes(totalNodes, preemptable=preemptable)
                    except ProvisioningException as e:
                        logger.debug("We tried to stop the worker nodes (%s total) but got a provisioning exception: %s" % (totalNodes, e))
                        raise
                    logger.debug("Scalar (preemptable=%s) exiting normally" % preemptable)
                    break
                
                queueSize = batchSystem.getIssuedQueueSize(preemptable=preemptable)
                jobsStartedPerSecond = batchSystem.numberOfRecentJobsStartedPerSecond(preemptable=preemptable)
                if queueSize == 0:
                    waitTime = 0.0
                else:
                    waitTime = sys.maxint if jobsStartedPerSecond == 0 else queueSize/jobsStartedPerSecond
                if totalNodes < maxNodes and waitTime > maxTimeToRun:
                    # Try creating a node
                    try:
                        provisioner.addNodes(preemptable=preemptable)
                        totalNodes += 1
                        logger.debug("Added a worker node to the (preemptable: %s) cluster, total worker nodes: %s"
                                     " (estimated wait-time: %s, queue-size: %s, jobs-started-per-second: %s,"
                                     " min wait-time: %s, max wait-time: %s)" % 
                                     (preemptable, totalNodes, waitTime, queueSize, 
                                      jobsStartedPerSecond, minTimeToRun, maxTimeToRun))
                        continue
                    except ProvisioningException as e:
                        logger.debug("We tried to add a worker node but got a provisioning exception: %s" % e)
                elif (totalNodes > minNodes and waitTime < minTimeToRun
                      and batchSystem.getNumberOfEmptyNodes(preemptable=preemptable) > 0):
                    # Try removing a node
                    try:
                        provisioner.removeNodes(preemptable=preemptable)
                        totalNodes -= 1
                        logger.debug("Removing a worker node from the (preemptable: %s) cluster, total worker nodes: %s"
                                     " (estimated wait-time: %s, queue-size: %s, jobs-started-per-second: %s,"
                                     " min wait-time: %s, max wait-time: %s)" % 
                                     (preemptable, totalNodes, waitTime, queueSize, 
                                      jobsStartedPerSecond, minTimeToRun, maxTimeToRun))
                        continue
                    except ProvisioningException as e:
                        logger.debug("We tried to remove a worker node but got a provisioning exception: %s" % e)
                
                #Sleep to avoid thrashing
                time.sleep(1)
        except:
            error.set() #Set the error event
            raise