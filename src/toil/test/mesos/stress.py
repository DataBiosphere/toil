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

def main(numJobs, useBadExecutor=False):
    args = list( sys.argv )
    args.append("--batchSystem=%s" % ( 'badmesos' if useBadExecutor else 'mesos' ))
    args.append("--retryCount=3")

    # Boilerplate -- startToil requires options
    parser = OptionParser()
    Job.Runner.addToilOptions(parser)
    options, args = parser.parse_args( args )

    # Launch first toil Job
    i = LongTestJob( numJobs )
    j = Job.Runner.startToil(i,  options )
    assert(j==0) # confirm that no jobs failed

if __name__=="__main__":
    main(5, useBadExecutor=False)
