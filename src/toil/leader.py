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

"""
The leader script (of the leader/worker pair) for running jobs.
"""
from __future__ import absolute_import
import logging
import time
import xml.etree.cElementTree as ET
from multiprocessing import Process
from multiprocessing import JoinableQueue as Queue
import cPickle
from toil.lib.bioio import getTotalCpuTime
from toil.provisioners.clusterScaler import ClusterScaler
from toil.batchSystems.jobDispatcher import JobDispatcher

logger = logging.getLogger( __name__ )

####################################################
##Stats/logging aggregation
####################################################

def statsAndLoggingAggregatorProcess(jobStore, stop):
    """
    The following function is used for collating stats/reporting log messages 
    from the workers.
    Works inside of a separate process, collates as long as the stop flag is 
    not True.
    """
    #Overall timing
    startTime = time.time()
    startClock = getTotalCpuTime()

    #Start off the stats file
    with jobStore.writeSharedFileStream("statsAndLogging.xml") as fileHandle:
        fileHandle.write('<?xml version="1.0" ?><stats>')

        #Call back function
        def statsAndLoggingCallBackFn(fileHandle2):
            node = ET.parse(fileHandle2).getroot()
            nodesNamed = node.find("messages").findall
            for message in nodesNamed("message"):
                logger.log(int(message.attrib["level"]), "Got message from job at time: %s : %s",
                           time.strftime("%m-%d-%Y %H:%M:%S"), message.text)
            for log in nodesNamed("log"):
                logger.info("%s:     %s" %
                                    tuple(log.text.split("!",1)))# the jobID is separated from log by "!"
            ET.ElementTree(node).write(fileHandle)

        #The main loop
        timeSinceOutFileLastFlushed = time.time()
        while True:
            if not stop.empty(): #This is a indirect way of getting a message to
                #the process to exit
                jobStore.readStatsAndLogging(statsAndLoggingCallBackFn)
                break
            if jobStore.readStatsAndLogging(statsAndLoggingCallBackFn) == 0:
                time.sleep(0.5) #Avoid cycling too fast
            if time.time() - timeSinceOutFileLastFlushed > 60: #Flush the
                #results file every minute
                fileHandle.flush()
                timeSinceOutFileLastFlushed = time.time()

        #Finish the stats file
        fileHandle.write("<total_time time='%s' clock='%s'/></stats>" % \
                         (str(time.time() - startTime), str(getTotalCpuTime() - startClock)))

class FailedJobsException( Exception ):
    def __init__( self, jobStoreString, numberOfFailedJobs ):
        super( FailedJobsException, self ).__init__( "The job store '%s' contains %i failed jobs" % (jobStoreString, numberOfFailedJobs))
        self.jobStoreString = jobStoreString
        self.numberOfFailedJobs = numberOfFailedJobs

def mainLoop(config, batchSystem, provisioner, jobStore, rootJobWrapper):
    """
    This is the main loop from which jobs are issued and processed.
    
    :raises: toil.leader.FailedJobsException if at the end of function their remain
    failed jobs
    
    :return: The return value of the root job's run function.
    """

    ##########################################
    #Start the stats/logging aggregation process
    ##########################################

    stopStatsAndLoggingAggregatorProcess = Queue() #When this is s
    worker = Process(target=statsAndLoggingAggregatorProcess,
                     args=(jobStore, stopStatsAndLoggingAggregatorProcess))
    worker.start()
    
    ##########################################
    #Create cluster scaling processes if the provisioner is not None
    ##########################################
     
    if provisioner != None:
        clusterScaler = ClusterScaler(provisioner, batchSystem, config)
    
    ##########################################
    #Create a jobDisbatcher and run the batch
    ##########################################
    
    totalFailedJobs = JobDispatcher(config, batchSystem, jobStore, rootJobWrapper).dispatch()

    logger.info("Finished the main loop")
    
    ##########################################
    #Shutdown worker nodes if using a provisioning instance 
    ##########################################
    
    if provisioner != None:
        logger.info("Waiting for workers to shutdown")
        startTime = time.time()
        clusterScaler.shutdown()
        logger.info("Worker shutdown complete in %s seconds", time.time() - startTime)

    ##########################################
    #Finish up the stats/logging aggregation process
    ##########################################
    
    logger.info("Waiting for stats and logging collator process to finish")
    startTime = time.time()
    stopStatsAndLoggingAggregatorProcess.put(True)
    worker.join()
    logger.info("Stats/logging finished collating in %s seconds", time.time() - startTime)
    # in addition to cleaning on exceptions, onError should clean if there are any failed jobs

    #Parse out the return value from the root job
    with jobStore.readSharedFileStream("rootJobReturnValue") as fH:
        jobStoreFileID = fH.read()
    with jobStore.readFileStream(jobStoreFileID) as fH:
        rootJobReturnValue = cPickle.load(fH)
    
    if totalFailedJobs > 0:
        if config.clean == "onError" or config.clean == "always" :
            jobStore.deleteJobStore()
        raise FailedJobsException( config.jobStore, totalFailedJobs )

    if config.clean == "onSuccess" or config.clean == "always":
        jobStore.deleteJobStore()

    return rootJobReturnValue