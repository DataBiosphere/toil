import argparse
import sys

from toil.common import Toil
from toil.job import Job
from toil.leader import FailedJobsException

def main():
    """
    This workflow always fails.
    
    Invoke like:
    
        python examples/example_alwaysfail.py ./jobstore
        
    Then you can inspect the job store with tools like `toil status`:
    
        toil status --printLogs ./jobstore
        
    """
    parser = argparse.ArgumentParser(description=main.__doc__)
    Job.Runner.addToilOptions(parser)
    options = parser.parse_args(sys.argv[1:])

    hello_job = Job.wrapJobFn(explode)

    with Toil(options) as toil:
        toil.start(hello_job)


def explode(job):
    sys.stderr.write('Something somewhere has gone terribly wrong\n')
    raise RuntimeError('Boom!')
    

if __name__=="__main__":
    main()

    
