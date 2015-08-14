#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

from __future__ import absolute_import
import time

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem

class CombinedBatchSystem(AbstractBatchSystem):
    """Takes two batch systems and a choice function to decide which to issue to.
    """
    def __init__(self, config, batchSystem1, batchSystem2, batchSystemChoiceFn):
        AbstractBatchSystem.__init__(self, config, 0, 0) #Call the parent constructor
        self.batchSystem1 = batchSystem1
        self.batchSystem2 = batchSystem2
        self.batchSystemChoiceFn = batchSystemChoiceFn
        
    def _jobIDForBatchSystem1(self, id):
        return (1, id)
    
    def _isJobIDForBatchSystem1(self, id):
        return id[0] == 1 
    
    def _jobIDForBatchSystem2(self, id):
        return (2, id)
    
    def _isJobIDForBatchSystem2(self, id):
        return id[0] == 2 
    
    def _strip(self, id):
        return id[1]

    def issueBatchJob(self, command, memory, cpu):
        if self.batchSystemChoiceFn(command, memory, cpu):
            return self._jobIDForBatchSystem1(self.batchSystem1.issueBatchJob(command, memory, cpu))
        else:
            return self._jobIDForBatchSystem2(self.batchSystem2.issueBatchJob(command, memory, cpu))
        
    def killBatchJobs(self, jobIDs):
        l, l2 = [], []
        for jobID in jobIDs:
            if self._isJobIDForBatchSystem1(jobID):
                l.append(self._strip(jobID))
            else:
                assert self._isJobIDForBatchSystem2(jobID)
                l2.append(self._strip(jobID))
        self.batchSystem1.killBatchJobs(l)
        self.batchSystem2.killBatchJobs(l2)
    
    def getIssuedBatchJobIDs(self):
        return [ self._jobIDForBatchSystem1(id) for id in self.batchSystem1.getIssuedBatchJobIDs() ] + [ self._jobIDForBatchSystem2(id) for id in self.batchSystem2.getIssuedBatchJobIDs() ]
    
    def getRunningBatchJobIDs(self):
        return [ self._jobIDForBatchSystem1(id) for id in self.batchSystem1.getRunningBatchJobIDs() ] + [ self._jobIDForBatchSystem2(id) for id in self.batchSystem2.getRunningBatchJobIDs() ]
   
    def getUpdatedBatchJob(self, maxWait):
        endTime = time.time() + maxWait
        while 1:
            updatedJob = self.batchSystem2.getUpdatedBatchJob(0) #Small positive values of wait seem to
            if updatedJob != None:
                return (self._jobIDForBatchSystem2(updatedJob[0]), updatedJob[1])
            updatedJob = self.batchSystem1.getUpdatedBatchJob(0)
            if updatedJob != None:
                return (self._jobIDForBatchSystem1(updatedJob[0]), updatedJob[1])
            remaining = endTime - time.time()
            if remaining <= 0:
                return None
            time.sleep(0.01)

    # FIXME: This should be a static method

    def getRescueBatchJobFrequency(self):
        return min(self.batchSystem1.getRescueBatchJobFrequency(), self.batchSystem2.getRescueBatchJobFrequency())
