import os
import tempfile

from toil.common import Toil
from toil.job import Job


def helloWorld(message, memory="2G", cores=2, disk="3G"):
    return f"Hello, world!, here's a message: {message}"


if __name__ == "__main__":
    jobstore: str = tempfile.mkdtemp("tutorial_quickstart")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "OFF"
    options.clean = "always"

    hello_job = Job.wrapFn(helloWorld, "Woot")

    with Toil(options) as toil:
        print(toil.start(hello_job))  # prints "Hello, world!, ..."
