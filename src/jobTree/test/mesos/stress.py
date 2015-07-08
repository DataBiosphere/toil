import sys
from optparse import OptionParser

from jobTree.target import Target

def touchFile( name, i='' ):
    with open( 'hello_world_{}_{}.txt'.format( name, i ), 'w' ) as f:
        f.write( 'This is a triumph' )

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
        touchFile( 'parentFollowOn' )

class HelloWorldTarget(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=100000, cpu=0.01)
        self.i=i


    def run(self, fileStore):
        touchFile( 'child', self.i )
        self.addFollowOn(HelloWorldFollowOn(self.i))

class HelloWorldFollowOn(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=200000, cpu=0.01)
        self.i = i

    def run(self, fileStore):
        touchFile( 'followOn', self.i )

def main(numTargets, useBadExecutor=False):
    args = list( sys.argv )
    args.append("--batchSystem=%s" % ( 'badmesos' if useBadExecutor else 'mesos' ))
    args.append("--retryCount=3")
    args.append("--logDebug")

    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Target.Runner.addJobTreeOptions(parser)
    options, args = parser.parse_args( args )

    # Launch first jobTree Target
    i = LongTestTarget( numTargets )
    j = Target.Runner.startJobTree(i,  options )

if __name__=="__main__":
    main(5, useBadExecutor=False)
