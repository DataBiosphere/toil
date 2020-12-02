import argparse
import os

from toil.job import Job


def f0(job):
    if 'FAIL' in os.environ:
        raise RuntimeError('failed on purpose')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    Job.Runner.addToilOptions(parser)
    options = parser.parse_args()
    rootJob = Job.wrapJobFn(f0, cores=0.5, memory='50 M', disk='50 M')
    Job.Runner.startToil(rootJob, options)
