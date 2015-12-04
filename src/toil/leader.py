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
import json
from multiprocessing import Process
from multiprocessing import JoinableQueue as Queue
import cPickle
from toil.provisioners.clusterScaler import ClusterScaler
from toil.batchSystems.jobDispatcher import JobDispatcher
from bd2k.util.expando import Expando
from toil.lib.bioio import getTotalCpuTime

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
    #  Overall timing
    startTime = time.time()
    startClock = getTotalCpuTime()

    def callback(fileHandle):
        stats = json.load(fileHandle, object_hook=Expando)
        workers = stats.workers
        try:
            logs = workers.log
        except AttributeError:
            # To be expected if there were no calls to logToMaster()
            pass
        else:
            for message in logs:
                logger.log(int(message.level),
                           "Got message from job at time: %s : %s",
                           time.strftime("%m-%d-%Y %H:%M:%S"), message.text)

        for log in stats.logs:
            logger.info("%s:     %s", log.jobStoreID, log.text)

    while True:
        # This is a indirect way of getting a message to the process to exit
        if not stop.empty():
            jobStore.readStatsAndLogging(callback)
            break
        if jobStore.readStatsAndLogging(callback) == 0:
            time.sleep(0.5)  # Avoid cycling too fast

    # Finish the stats file
    text = json.dumps(dict(total_time=str(time.time() - startTime),
                           total_clock=str(getTotalCpuTime() - startClock)))
    jobStore.writeStatsAndLogging(text)

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

    # Start the stats/logging aggregation process
   
    stopStatsAndLoggingAggregatorProcess = Queue() #When this is s
    worker = Process(target=statsAndLoggingAggregatorProcess,
                     args=(jobStore, stopStatsAndLoggingAggregatorProcess))
    worker.start()
    
    # Create the job dispatcher
   
    jobDispatcher = JobDispatcher(config, batchSystem, jobStore, rootJobWrapper)
     
    # Create cluster scaling processes if the provisioner is not None
     
    if provisioner != None:
        clusterScaler = ClusterScaler(provisioner, jobDispatcher, config)
        jobDispatcher.clusterScaler = clusterScaler
    
    # Run the batch
    
    totalFailedJobs = jobDispatcher.dispatch()

    logger.info("Finished the main loop")
    
    # Shutdown worker nodes if using a provisioning instance 
    
    if provisioner != None:
        logger.info("Waiting for workers to shutdown")
        startTime = time.time()
        clusterScaler.shutdown()
        logger.info("Worker shutdown complete in %s seconds", time.time() - startTime)

    # Finish up the stats/logging aggregation process
    
    logger.info("Waiting for stats and logging collator process to finish")
    startTime = time.time()
    stopStatsAndLoggingAggregatorProcess.put(True)
    worker.join()
    logger.info("Stats/logging finished collating in %s seconds", time.time() - startTime)

    # Parse out the return value from the root job
    
    with jobStore.readSharedFileStream("rootJobReturnValue") as fH:
        jobStoreFileID = fH.read()
    with jobStore.readFileStream(jobStoreFileID) as fH:
        rootJobReturnValue = cPickle.load(fH)
    
    # Decide how to exit
    
    if totalFailedJobs > 0:
        if config.clean == "onError" or config.clean == "always" :
            jobStore.deleteJobStore()
        # in addition to cleaning on exceptions, onError should clean if there are any failed jobs
        raise FailedJobsException( config.jobStore, totalFailedJobs )

    if config.clean == "onSuccess" or config.clean == "always":
        jobStore.deleteJobStore()

    return rootJobReturnValue
