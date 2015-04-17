# 4-13-15
# John Vivian

"""
'Hello World' script for JobTree
"""

from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack
from optparse import OptionParser


def hello_world(target):
    with open ('hello_world.txt', 'w') as file:
        file.write('This is a triumph')

    target.addChildTargetFn(hello_world_child)

def hello_world_child(target):
    with open ('hello_world_child.txt', 'w') as file:
        file.write('Sorry, the cake is a lie.')

if __name__ == '__main__':

    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(Target.makeTargetFn(hello_world)).startJobTree(options)