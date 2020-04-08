#!/usr/bin/env python3
# example_cachingbenchmark.py: Empirically inspect Toil caching

"""
This workflow collects statistics about caching.

Invoke like:

    python examples/example_cachingbenchmark.py ./jobstore --realTimeLogging --logInfo --disableCaching False
    
    python examples/example_cachingbenchmark.py ./jobstore --realTimeLogging --logInfo --disableCaching True
    
    python examples/example_cachingbenchmark.py aws:us-west-2:cachingjobstore --realTimeLogging --logInfo --disableCaching False
    
    python examples/example_cachingbenchmark.py aws:us-west-2:cachingjobstore --realTimeLogging --logInfo --disableCaching True
        
"""

import argparse
import sys
import os
import socket
import time
import random
import collections

from toil.common import Toil
from toil.job import Job
from toil.leader import FailedJobsException
from toil.realtimeLogger import RealtimeLogger

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument('--minSleep', type=int, default=1,
        help="Minimum seconds to sleep")
    
    Job.Runner.addToilOptions(parser)
    
    options = parser.parse_args(sys.argv[1:])

    root_job = Job.wrapJobFn(root, options)

    with Toil(options) as toil:
        results = toil.start(root_job)
        
    print("Caching results:")
    print(results)


def root(job, options):
    # Make a file
    with job.fileStore.writeGlobalFileStream() as (stream, file_id):
        stream.write(('This is a test of the Toil file caching system. ' +
            'Had this been an actual file, its contents would have been more interesting.').encode('utf-8'))
    
    child_rvs = []
    for i in range(100):
        # Make lots of child jobs that read and report on the file
        child_rvs.append(job.addChildJobFn(poll, options, file_id, i).rv())
    
    # Collect all their views into a report
    return job.addFollowOnJobFn(report, child_rvs).rv()
    
def poll(job, options, file_id, number, cores=0.1, disk='200M', memory='512M'):

    # Wait a random amount of time before grabbing the file for others to cache it
    time.sleep(random.randint(options.minSleep, options.minSleep + 10))

    # Read the file. Don't accept a symlink because then we might just have the
    # filestore's copy, even if caching is not happening.
    local_file = job.fileStore.readGlobalFile(file_id, cache=True, mutable=False, symlink=False)
    
    # Wait a random amount of after before grabbing the file for others to use it
    time.sleep(random.randint(options.minSleep, options.minSleep + 10))
    
    # Stat the file (reads through links)
    stats = os.stat(local_file)
    
    # Check what machine we are
    hostname = socket.gethostname()
    
    RealtimeLogger.info('Job {} on host {} sees file at device {} inode {}'.format(number, hostname, stats.st_dev, stats.st_ino))
    
    # Return a tuple representing our view of the file.
    # Drop hostname since hostnames are unique per pod.
    return (stats.st_dev, stats.st_ino)
    

def report(job, views):
    # Count the distinct views
    counts = collections.Counter()
    for v in views:
        counts[v] += 1
        
    report = ['{} distinct views, most frequent:'.format(len(counts))]
        
    for view, count in counts.most_common(10):
        report.append('{}: {}'.format(view, count))
        
    return '\n'.join(report)
    

if __name__=="__main__":
    main()

    

