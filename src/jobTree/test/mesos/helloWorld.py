# 4-13-15
# John Vivian

"""
'Hello World' script for JobTree
"""

from optparse import OptionParser

from jobTree.target import Target

class HelloWorld(object):
    def __init__(self, target):
        self.foo_bam = target.getEmptyFileStoreID()
        self.bar_bam = target.getEmptyFileStoreID()


def hello_world(target):

    hw = HelloWorld(target)

    with open('foo_bam.txt', 'w') as handle:
        handle.write('\nThis is a triumph...\n')

    # Assign FileStoreID to a given file
    hw.foo_bam = target.writeGlobalFile('foo_bam.txt')

    # Spawn child
    target.addChildTargetFn(hello_world_child, hw)


def hello_world_child(target, hw):

    path = target.readGlobalFile(hw.foo_bam)

    with open(path, 'a') as handle:
        handle.write("\nFileStoreID works!\n")

    # NOTE: path and the udpated file are stored to /tmp
    # If we want to SAVE our changes to this tmp file, we must write it out.
    with open(path, 'r') as r:
        with open('bar_bam.txt', 'w') as handle:
            for line in r.readlines():
                handle.write(line)

    # Assign FileStoreID to a given file
    # can also use:  target.updateGlobalFile() given the FileStoreID instantiation.
    hw.bar_bam = target.writeGlobalFile('bar_bam.txt')


if __name__ == '__main__':

    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Target.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Create object that contains our FileStoreIDs


    # Launch first jobTree Target
    i = Target.wrapTargetFn(hello_world).startJobTree(options)