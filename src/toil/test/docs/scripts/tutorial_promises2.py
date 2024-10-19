import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


def binaryStrings(job, depth, message=""):
    if depth > 0:
        s = [
            job.addChildJobFn(binaryStrings, depth - 1, message + "0").rv(),
            job.addChildJobFn(binaryStrings, depth - 1, message + "1").rv(),
        ]
        return job.addFollowOnFn(merge, s).rv()
    return [message]


def merge(strings):
    return strings[0] + strings[1]


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_promises2")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.loglevel = "OFF"
    options.clean = "always"

    with Toil(options) as toil:
        print(toil.start(Job.wrapJobFn(binaryStrings, depth=5)))
