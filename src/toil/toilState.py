# Copyright (C) 2015-2016 Regents of the University of California
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

logger = logging.getLogger( __name__ )

class ToilState( object ):
    """
    Represents a snapshot of the jobs in the jobStore.
    """
    def __init__( self, jobStore, rootJob, jobCache=None):
        """
        Loads the state from the jobStore, using the rootJob 
        as the source of the job graph.
        
        The jobCache is a map from jobStoreIDs to jobWrappers or None. Is used to
        speed up the building of the state.
        
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore 
        :param toil.jobWrapper.JobWrapper rootJob
        """
        # This is a hash of jobs, referenced by jobStoreID, to their predecessor jobs.
        self.successorJobStoreIDToPredecessorJobs = { }
        
        # Hash of jobStoreIDs to counts of numbers of successors issued.
        # There are no entries for jobs
        # without successors in this map.
        self.successorCounts = { }

        # This is a hash of service jobs, referenced by jobStoreID, to their predecessor job
        self.serviceJobStoreIDToPredecessorJob = { }

        # Hash of jobStoreIDs to maps of services issued for the job
        # Each for job, the map is a dictionary of service jobStoreIDs
        # to the flags used to communicate the with service
        self.servicesIssued = { }
        
        # Jobs that are ready to be processed
        self.updatedJobs = set( )
        
        # The set of totally failed jobs - this needs to be filtered at the
        # end to remove jobs that were removed by checkpoints
        self.totalFailedJobs = set()
        
        # Jobs (as jobStoreIDs) with successors that have totally failed
        self.hasFailedSuccessors = set()
        
        # The set of successors of failed jobs as a set of jobStoreIds
        self.failedSuccessors = set()
        
        # Set of jobs that have multiple predecessors that have one or more predecessors
        # finished, but not all of them. This acts as a cache for these jobs.
        # Stored as hash from jobStoreIDs to jobWrappers
        self.jobsToBeScheduledWithMultiplePredecessors = {}
        
        ##Algorithm to build this information
        logger.info("(Re)building internal scheduler state")
        self._buildToilState(rootJob, jobStore, jobCache)

    def _buildToilState(self, jobWrapper, jobStore, jobCache=None):
        """
        Traverses tree of jobs from the root jobWrapper (rootJob) building the
        ToilState class.

        If jobCache is passed, it must be a dict from job ID to JobWrapper
        object. Jobs will be loaded from the cache (which can be downloaded from
        the jobStore in a batch) instead of piecemeal when recursed into.
        """

        def getJob(jobId):
            if jobCache is not None:
                try:
                    return jobCache[jobId]
                except ValueError:
                    return jobStore.load(jobId)
            else:
                return jobStore.load(jobId)

        # If the jobWrapper has a command, is a checkpoint, has services or is ready to be
        # deleted it is ready to be processed
        if (jobWrapper.command is not None
            or jobWrapper.checkpoint is not None
            or len(jobWrapper.services) > 0
            or len(jobWrapper.stack) == 0):
            logger.debug('Found job to run: %s, with command: %s, with checkpoint: %s, '
                         'with  services: %s, with stack: %s', jobWrapper.jobStoreID,
                         jobWrapper.command is not None, jobWrapper.checkpoint is not None,
                         len(jobWrapper.services) > 0, len(jobWrapper.stack) == 0)
            self.updatedJobs.add((jobWrapper, 0))

            if jobWrapper.checkpoint is not None:
                jobWrapper.command = jobWrapper.checkpoint

        else: # There exist successors
            logger.debug("Adding job: %s to the state with %s successors" % (jobWrapper.jobStoreID, len(jobWrapper.stack[-1])))
            
            # Record the number of successors
            self.successorCounts[jobWrapper.jobStoreID] = len(jobWrapper.stack[-1])
            
            def processSuccessorWithMultiplePredecessors(successorJobWrapper):
                # If jobWrapper job is not reported as complete by the successor
                if jobWrapper.jobStoreID not in successorJobWrapper.predecessorsFinished:
                    
                    # Update the sucessor's status to mark the predecessor complete
                    successorJobWrapper.predecessorsFinished.add(jobWrapper.jobStoreID)
            
                # If the successor has no predecessors to finish
                assert len(successorJobWrapper.predecessorsFinished) <= successorJobWrapper.predecessorNumber
                if len(successorJobWrapper.predecessorsFinished) == successorJobWrapper.predecessorNumber:
                    
                    # It is ready to be run, so remove it from the cache
                    self.jobsToBeScheduledWithMultiplePredecessors.pop(successorJobStoreID)
                    
                    # Recursively consider the successor
                    self._buildToilState(successorJobWrapper, jobStore, jobCache=jobCache)
            
            # For each successor
            for successorJobStoreTuple in jobWrapper.stack[-1]:
                successorJobStoreID = successorJobStoreTuple[0]
                
                # If the successor jobWrapper does not yet point back at a
                # predecessor we have not yet considered it
                if successorJobStoreID not in self.successorJobStoreIDToPredecessorJobs:

                    # Add the job as a predecessor
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = [jobWrapper]
                    
                    # If predecessorJobStoreID is not None then the successor has multiple predecessors
                    predecessorJobStoreID = successorJobStoreTuple[-1]
                    if predecessorJobStoreID != None: 
                        
                        # We load the successor job
                        successorJobWrapper =  getJob(successorJobStoreID)
                        
                        # We put the successor job in the cache of successor jobs with multiple predecessors
                        assert successorJobStoreID not in self.jobsToBeScheduledWithMultiplePredecessors
                        self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID] = successorJobWrapper
                        
                        # Process successor
                        processSuccessorWithMultiplePredecessors(successorJobWrapper)
                            
                    else:
                        # The successor has only the jobWrapper job as a predecessor so
                        # recursively consider the successor
                        self._buildToilState(getJob(successorJobStoreID), jobStore, jobCache=jobCache)
                
                else:
                    # We've already seen the successor
                    
                    # Add the job as a predecessor
                    assert jobWrapper not in self.successorJobStoreIDToPredecessorJobs[successorJobStoreID]
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobWrapper) 
                    
                    # If the successor has multiple predecessors
                    if successorJobStoreID in self.jobsToBeScheduledWithMultiplePredecessors:
                        
                        # Get the successor from cache
                        successorJobWrapper = self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID]
                        
                        # Process successor
                        processSuccessorWithMultiplePredecessors(successorJobWrapper)
