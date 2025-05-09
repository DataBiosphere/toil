import os

from toil.common import Toil
from toil.job import Job
from toil.lib.io import mkdtemp
from toil.test import get_data

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
    tmp: str = mkdtemp("tutorial_staging_tmp")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        if not toil.options.restart:
            with get_data("test/docs/scripts/stagingExampleFiles/in.txt") as path:
                inputFileID = toil.importFile(f"file://{path}")
                outputFileID = toil.start(HelloWorld(inputFileID))
        else:
            outputFileID = toil.restart()

        toil.exportFile(
            outputFileID,
            "file://" + os.path.join(tmp, "out.txt"),
        )
