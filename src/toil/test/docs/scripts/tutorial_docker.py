import os

from toil.common import Toil
from toil.job import Job
from toil.lib.docker import apiDockerCall
from toil.lib.io import mkdtemp

align = Job.wrapJobFn(
    apiDockerCall, image="ubuntu", working_dir=os.getcwd(), parameters=["ls", "-lha"]
)

if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_docker")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        toil.start(align)
