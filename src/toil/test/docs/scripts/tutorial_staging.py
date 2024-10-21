import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp


class HelloWorld(Job):
    def __init__(self, id):
        Job.__init__(self)
        self.inputFileID = id

    def run(self, fileStore):
        with fileStore.readGlobalFileStream(self.inputFileID, encoding="utf-8") as fi:
            with fileStore.writeGlobalFileStream(encoding="utf-8") as (
                fo,
                outputFileID,
            ):
                fo.write(fi.read() + "World!")
        return outputFileID


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_staging")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        if not toil.options.restart:
            ioFileDirectory = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "stagingExampleFiles"
            )
            inputFileID = toil.importFile(
                "file://" + os.path.abspath(os.path.join(ioFileDirectory, "in.txt"))
            )
            outputFileID = toil.start(HelloWorld(inputFileID))
        else:
            outputFileID = toil.restart()

        toil.exportFile(
            outputFileID,
            "file://" + os.path.abspath(os.path.join(ioFileDirectory, "out.txt")),
        )
