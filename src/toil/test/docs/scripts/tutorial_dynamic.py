import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


def binaryStringFn(job, depth, message=""):
    if depth > 0:
        job.addChildJobFn(binaryStringFn, depth - 1, message + "0")
        job.addChildJobFn(binaryStringFn, depth - 1, message + "1")
    else:
        job.log(f"Binary string: {message}")


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_dynamic")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        toil.start(Job.wrapJobFn(binaryStringFn, depth=5))
