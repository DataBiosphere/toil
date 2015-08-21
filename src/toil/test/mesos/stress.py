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
import sys
from optparse import OptionParser

from toil.job import Job

def touchFile( fileStore ):
    with fileStore.writeGlobalFileStream() as (f, id):
        f.write( "This is a triumph" )

class LongTestJob(Job):
    def __init__(self, numJobs):
        Job.__init__(self,  memory=100000, cpu=0.01)
        self.numJobs = numJobs

    def run(self, fileStore):
        for i in range(0,self.numJobs):
            self.addChild(HelloWorldJob(i))
        self.addFollowOn(LongTestFollowOn())

class LongTestFollowOn(Job):

    def __init__(self):
        Job.__init__(self,  memory=1000000, cpu=0.01)

    def run(self, fileStore):
        touchFile( fileStore )

class HelloWorldJob(Job):

    def __init__(self,i):
        Job.__init__(self,  memory=100000, cpu=0.01)
        self.i=i


    def run(self, fileStore):
        touchFile( fileStore )
        self.addFollowOn(HelloWorldFollowOn(self.i))

class HelloWorldFollowOn(Job):

    def __init__(self,i):
        Job.__init__(self,  memory=200000, cpu=0.01)
        self.i = i

    def run(self, fileStore):
        touchFile( fileStore)

def main(numJobs):
    args = list( sys.argv )
    args.append('--batchSystem=mesos')
    args.append('--retryCount=3')

    # Boilerplate -- startToil requires options
    parser = OptionParser()
    Job.Runner.addToilOptions(parser)
    options, args = parser.parse_args( args )

    # Launch first toil Job
    i = LongTestJob( numJobs )
    j = Job.Runner.startToil(i,  options )
    assert(j==0) # confirm that no jobs failed

if __name__=="__main__":
    main(numJobs=5)
