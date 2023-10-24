import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


def helloWorld(message):
    return f"Hello, world!, here's a message: {message}"


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_quickstart")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "OFF"
    options.clean = "always"

    hello_job = Job.wrapFn(helloWorld, "Woot")

    with Toil(options) as toil:
        print(toil.start(hello_job))  # prints "Hello, world!, ..."
