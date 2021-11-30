from toil.common import Toil
from toil.job import Job


class HelloWorld(Job):
    def __init__(self, message):
        Job.__init__(self,  memory="2G", cores=2, disk="3G")
        self.message = message

    def run(self, fileStore):
        return f"Hello, world!, I have a message: {self.message}"


if __name__ == "__main__":
    # options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    # options.logLevel = "INFO"
    # options.clean = "always"
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.clean = "never"

    with Toil(options) as toil:
        if not toil.options.restart:
            job = HelloWorld("Woot!")
            print(toil.start(job))
        else:
            print(toil.restart() + "!!! restart")
