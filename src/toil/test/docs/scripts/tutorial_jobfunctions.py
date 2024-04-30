import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


def helloWorld(job, message):
    job.log(f"Hello world, I have a message: {message}")


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_jobfunctions")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    hello_job = Job.wrapJobFn(helloWorld, "Woot!")

    with Toil(options) as toil:
        toil.start(hello_job)
