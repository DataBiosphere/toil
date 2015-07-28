# 4-13-15
# John Vivian

"""
'Hello World' script for Toil
"""

from optparse import OptionParser
import os
from toil.batchJob import Job

def hello_world(job, memory=100, cpu=0.5):

    with open('foo_bam.txt', 'w') as handle:
        handle.write('\nThis is a triumph...\n')

    # Assign FileStoreID to a given file
    foo_bam = job.fileStore.writeGlobalFile('foo_bam.txt')

    # Spawn child
    job.addChildJobFn(hello_world_child, foo_bam, memory=100, cpu=0.5, disk=2000)


def hello_world_child(job, hw, memory=100, cpu=0.5):

    path = job.fileStore.readGlobalFile(hw)

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
    # can also use:  job.updateGlobalFile() given the FileStoreID instantiation.
    bar_bam = job.fileStore.writeGlobalFile('bar_bam.txt')

def main():
    # Boilerplate -- startToil requires options
    parser = OptionParser()
    Job.Runner.addToilOptions(parser)
    options, args = parser.parse_args()

    # Create object that contains our FileStoreIDs


    # Launch first toil Job
    i = Job.wrapJobFn(hello_world, memory=100, cpu=0.5, disk=2000)
    j = Job.Runner.startToil(i, options)


if __name__ == '__main__':
    main()