from toil.common import Toil
from toil.job import Job


def binaryStringFn(job, depth, message=""):
    if depth > 0:
        job.addChildJobFn(binaryStringFn, depth-1, message + "0")
        job.addChildJobFn(binaryStringFn, depth-1, message + "1")
    else:
        job.log(f"Binary string: {message}")


if __name__ == "__main__":
    # options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    # options.logLevel = "INFO"
    # options.clean = "always"
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.clean = "never"

    with Toil(options) as toil:
        toil.start(Job.wrapJobFn(binaryStringFn, depth=3))
