# 4-13-15
# John Vivian

"""
'Hello World' script for JobTree
"""

from optparse import OptionParser
import os
from jobTree.target import Target

def hello_world(target, memory=100, cpu=0.5):

    with open('foo_bam.txt', 'w') as handle:
        handle.write('\nThis is a triumph...\n')

    # Assign FileStoreID to a given file
    foo_bam = target.fileStore.writeGlobalFile('foo_bam.txt')

    # Spawn child
    target.addChildTargetFn(hello_world_child, foo_bam, memory=100, cpu=0.5, storage=2000)


def hello_world_child(target, hw, memory=100, cpu=0.5):

    path = target.fileStore.readGlobalFile(hw)

    with open(path, 'a') as handle:
        handle.write("\nFileStoreID works!\n")

    # NOTE: path and the udpated file are stored to /tmp
    # If we want to SAVE our changes to this tmp file, we must write it out.
    with open(path, 'r') as r:
        with open('bar_bam.txt', 'w') as handle:
            x = os.getcwd()
            for line in r.readlines():
                handle.write(line)

    # Assign FileStoreID to a given file
    # can also use:  target.updateGlobalFile() given the FileStoreID instantiation.
    bar_bam = target.fileStore.writeGlobalFile('bar_bam.txt')

def main():
    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Target.Runner.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Create object that contains our FileStoreIDs


    # Launch first jobTree Target
    i = Target.wrapTargetFn(hello_world, memory=100, cpu=0.5, storage=2000)
    j = Target.Runner.startJobTree(i, options)


if __name__ == '__main__':
    main()