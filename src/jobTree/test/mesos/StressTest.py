import sys
from optparse import OptionParser

from jobTree.target import Target
from jobTree.stack import Stack


def touchFile( name, i='' ):
    with open( 'hello_world_{}_{}.txt'.format( name, i ), 'w' ) as f:
        f.write( 'This is a triumph' )

class LongTestTarget(Target):
    def __init__(self, numTargets):
        Target.__init__(self,  memory=100000, cpu=0.01)
        self.numTargets = numTargets

    def run(self):
        for i in range(0,self.numTargets):
            self.addChild(HelloWorldTarget(i))
        self.setFollowOn(LongTestFollowOn())


class LongTestFollowOn(Target):

    def __init__(self):
        Target.__init__(self,  memory=1000000, cpu=0.01)

    def run(self):
        touchFile( 'parentFollowOn' )


class HelloWorldTarget(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=100000, cpu=0.01)
        self.i=i


    def run(self):
        touchFile( 'child', self.i )
        self.setFollowOn(HelloWorldFollowOn(self.i))


class HelloWorldFollowOn(Target):

    def __init__(self,i):
        Target.__init__(self,  memory=200000, cpu=0.01)
        self.i = i

    def run(self):
        touchFile( 'followOn', self.i )

def main(numTargets, useBadExecutor=False):
    args = list( sys.argv )
    args .append("--batchSystem=%s" % ( 'badmesos' if useBadExecutor else 'mesos' ))
    args .append("--retryCount=3")
    args .append("--logDebug")

    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args( args )

    # Setup the job stack and launch jobTree job
    i = Stack( LongTestTarget( numTargets ) ).startJobTree( options )

if __name__=="__main__":
    main(5, useBadExecutor=False)
