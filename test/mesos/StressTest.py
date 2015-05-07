import sys
from time import sleep
from jobTree.src.target import Target
from jobTree.src.stack import Stack
from optparse import OptionParser

class LongTest(Target):
    def __init__(self, x):
        Target.__init__(self, time=1, memory=100000, cpu=0.01)
        self.x = x

    def run(self):
        for i in range(0,self.x):
            self.addChildTarget(HelloWorld(i))
        self.setFollowOnTarget(LongTestFollow())


class HelloWorld(Target):

    def __init__(self,i):
        Target.__init__(self, time=1, memory=100000, cpu=0.01)
        self.i=i

    def run(self):
        raise RuntimeError()
        with open ('hello_world_child{}.txt'.format(self.i), 'w') as file:
            file.write('This is a triumph')
        self.setFollowOnTarget(HelloWorldFollow(self.i))


class LongTestFollow(Target):

    def __init__(self):
        Target.__init__(self, time=1, memory=1000000, cpu=0.01)

    def run(self):
        pass


class HelloWorldFollow(Target):

    def __init__(self,i):
        Target.__init__(self, time=1, memory=200000, cpu=0.01)
        self.i = i

    def run(self):
        with open ('hello_world_follow{}.txt'.format(self.i), 'w') as file:
            file.write('This is a triumph')

def main(tasks):
    sys.argv.append("--batchSystem=mesos")
    sys.argv.append("--retryCount=3")
    sys.argv.append("--logDebug")

    targetsToLaunch=tasks/2
    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(LongTest(targetsToLaunch)).startJobTree(options)

