from time import sleep
from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack
from optparse import OptionParser

class LongTest(Target):
    def __init__(self):
        Target.__init__(self, time=1, memory=1000000, cpu=1)

    def run(self):
        self.addChildTarget(HelloWorld())
        self.setFollowOnTarget(HelloWorldFollow())


class HelloWorld(Target):

    def __init__(self):
        Target.__init__(self, time=1, memory=1000000, cpu=2)

    def run(self):
        with open ('hello_world_child.txt', 'w') as file:
            file.write('This is a triumph')


class HelloWorldFollow(Target):

    def __init__(self):
        Target.__init__(self, time=1, memory=1000000, cpu=1)

    def run(self):
        with open ('hello_world_follow.txt', 'w') as file:
            file.write('This is a triumph')


def main():
    # Boilerplate -- startJobTree requires options
    # sys.argv.append()
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(LongTest()).startJobTree(options)

if __name__ == '__main__':
    from jobTree.test.mesos.LongTest import *
    main()