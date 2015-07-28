import sys
from optparse import OptionParser

from toil.target import Target

def touchFile( fileStore ):
    with fileStore.writeGlobalFileStream() as (f, id):
        f.write( "This is a triumph" )

class LongTestTarget(Target):
    def __init__(self, numTargets):
        Target.__init__(self,  memory=100000, cpu=0.01)
        self.numTargets = numTargets

    def run(self, fileStore):
        for i in range(0,self.numTargets):
            self.addChild(HelloWorldTarget(i))
        self.addFollowOn(LongTestFollowOn())

class LongTestFollowOn(Target):

    def __init__(self):
        Target.__init__(self,  memory=1000000, cpu=0.01)

    def run(self, fileStore):
        touchFile( fileStore )

class HelloWorldTarget(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=100000, cpu=0.01)
        self.i=i


    def run(self, fileStore):
        touchFile( fileStore )
        self.addFollowOn(HelloWorldFollowOn(self.i))

class HelloWorldFollowOn(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=200000, cpu=0.01)
        self.i = i

    def run(self, fileStore):
        touchFile( fileStore)

def main(numTargets, useBadExecutor=False):
    args = list( sys.argv )
    args.append("--batchSystem=%s" % ( 'badmesos' if useBadExecutor else 'mesos' ))
    args.append("--retryCount=3")

    # Boilerplate -- startToil requires options
    parser = OptionParser()
    Target.Runner.addToilOptions(parser)
    options, args = parser.parse_args( args )

    # Launch first toil Target
    i = LongTestTarget( numTargets )
    j = Target.Runner.startToil(i,  options )

if __name__=="__main__":
    main(5, useBadExecutor=False)
