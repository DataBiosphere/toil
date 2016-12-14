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
    Represents a snapshot of the jobs in the jobStore. Used by the leader to manage the batch.
    """
    def __init__( self, jobStore, rootJob, jobCache=None):
        """
        Loads the state from the jobStore, using the rootJob 
        as the source of the job graph.
        
        The jobCache is a map from jobStoreIDs to jobGraphs or None. Is used to
        speed up the building of the state.
        
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore 
        :param toil.jobWrapper.JobGraph rootJob
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
        # Stored as hash from jobStoreIDs to job graphs
        self.jobsToBeScheduledWithMultiplePredecessors = {}
        
        ##Algorithm to build this information
        logger.info("(Re)building internal scheduler state")
        self._buildToilState(rootJob, jobStore, jobCache)

    def _buildToilState(self, jobGraph, jobStore, jobCache=None):
        """
        Traverses tree of jobs from the root jobGraph (rootJob) building the
        ToilState class.

        If jobCache is passed, it must be a dict from job ID to JobGraph
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

        # If the jobGraph has a command, is a checkpoint, has services or is ready to be
        # deleted it is ready to be processed
        if (jobGraph.command is not None
            or jobGraph.checkpoint is not None
            or len(jobGraph.services) > 0
            or len(jobGraph.stack) == 0):
            logger.debug('Found job to run: %s, with command: %s, with checkpoint: %s, '
                         'with  services: %s, with stack: %s', jobGraph.jobStoreID,
                         jobGraph.command is not None, jobGraph.checkpoint is not None,
                         len(jobGraph.services) > 0, len(jobGraph.stack) == 0)
            self.updatedJobs.add((jobGraph, 0))

            if jobGraph.checkpoint is not None:
                jobGraph.command = jobGraph.checkpoint

        else: # There exist successors
            logger.debug("Adding job: %s to the state with %s successors" % (jobGraph.jobStoreID, len(jobGraph.stack[-1])))
            
            # Record the number of successors
            self.successorCounts[jobGraph.jobStoreID] = len(jobGraph.stack[-1])
            
            def processSuccessorWithMultiplePredecessors(successorJobGraph):
                # If jobGraph is not reported as complete by the successor
                if jobGraph.jobStoreID not in successorJobGraph.predecessorsFinished:
                    
                    # Update the sucessor's status to mark the predecessor complete
                    successorJobGraph.predecessorsFinished.add(jobGraph.jobStoreID)
            
                # If the successor has no predecessors to finish
                assert len(successorJobGraph.predecessorsFinished) <= successorJobGraph.predecessorNumber
                if len(successorJobGraph.predecessorsFinished) == successorJobGraph.predecessorNumber:
                    
                    # It is ready to be run, so remove it from the cache
                    self.jobsToBeScheduledWithMultiplePredecessors.pop(successorJobStoreID)
                    
                    # Recursively consider the successor
                    self._buildToilState(successorJobGraph, jobStore, jobCache=jobCache)
            
            # For each successor
            for successorJobNode in jobGraph.stack[-1]:
                successorJobStoreID = successorJobNode.jobStoreID
                
                # If the successor jobGraph does not yet point back at a
                # predecessor we have not yet considered it
                if successorJobStoreID not in self.successorJobStoreIDToPredecessorJobs:

                    # Add the job as a predecessor
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = [jobGraph]
                    
                    # If predecessor number > 1 then the successor has multiple predecessors
                    if successorJobNode.predecessorNumber > 1:
                        
                        # We load the successor job
                        successorJobGraph =  getJob(successorJobStoreID)
                        
                        # We put the successor job in the cache of successor jobs with multiple predecessors
                        assert successorJobStoreID not in self.jobsToBeScheduledWithMultiplePredecessors
                        self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID] = successorJobGraph
                        
                        # Process successor
                        processSuccessorWithMultiplePredecessors(successorJobGraph)
                            
                    else:
                        # The successor has only the jobGraph as a predecessor so
                        # recursively consider the successor
                        self._buildToilState(getJob(successorJobStoreID), jobStore, jobCache=jobCache)
                
                else:
                    # We've already seen the successor
                    
                    # Add the job as a predecessor
                    assert jobGraph not in self.successorJobStoreIDToPredecessorJobs[successorJobStoreID]
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobGraph)
                    
                    # If the successor has multiple predecessors
                    if successorJobStoreID in self.jobsToBeScheduledWithMultiplePredecessors:
                        
                        # Get the successor from cache
                        successorJobGraph = self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID]
                        
                        # Process successor
                        processSuccessorWithMultiplePredecessors(successorJobGraph)