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
    def __init__( self, jobStore, rootJob ):
        # This is a hash of jobs, referenced by jobStoreID, to their predecessor jobs.
        self.successorJobStoreIDToPredecessorJobs = { }
        # Hash of jobs to counts of numbers of successors issued.
        # There are no entries for jobs
        # without successors in this map. 
        self.successorCounts = { }
        # Jobs that are ready to be processed
        self.updatedJobs = set( )
        # Issued jobs - jobs that are issued to the batch system
        self.issuedJobs = set( ) 
        ##Algorithm to build this information
        self._buildToilState(rootJob, jobStore)

    def _buildToilState(self, jobWrapper, jobStore):
        """
        Traverses tree of jobs from the root jobWrapper (rootJob) building the
        ToilState class.
        """
        if jobWrapper.command != None or len(jobWrapper.stack) == 0: #If the jobWrapper has a command
            #or is ready to be deleted it is ready to be processed
            self.updatedJobs.add((jobWrapper, 0))
        else: #There exist successors
            self.successorCounts[jobWrapper] = len(jobWrapper.stack[-1])
            for successorJobStoreTuple in jobWrapper.stack[-1]:
                successorJobStoreID = successorJobStoreTuple[0]
                if successorJobStoreID not in self.successorJobStoreIDToPredecessorJobs:
                    #Given that the successor jobWrapper does not yet point back at a
                    #predecessor we have not yet considered it, so we call the function
                    #on the successor
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = [jobWrapper]
                    self._buildToilState(jobStore.load(successorJobStoreID), jobStore)
                else:
                    #We have already looked at the successor, so we don't recurse,
                    #but we add back a predecessor link
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobWrapper)
