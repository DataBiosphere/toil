import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


class HelloWorld(Job):
    def __init__(self, message):
        Job.__init__(self)
        self.message = message

    def run(self, fileStore):
        return f"Hello, world!, I have a message: {self.message}"


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_invokeworkflow2")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        if not toil.options.restart:
            job = HelloWorld("Woot!")
            output = toil.start(job)
        else:
            output = toil.restart()
    print(output)
