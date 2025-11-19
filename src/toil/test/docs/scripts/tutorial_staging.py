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
    tmp: str = mkdtemp("tutorial_staging_tmp")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        if not toil.options.restart:
            # Prepare an input file
            path = os.path.join(tmp, "in.txt")
            with open(path, "w") as f:
                f.write("Hello,\n")
            # In a real workflow, you would obtain an input file path from the
            # user.

            # Stage it into the Toil job store.
            #
            # Note: this may create a symlink depending on the value of the
            # --linkImports command line option, in which case the original
            # input file needs to still exist if the workflow is restarted.
            inputFileID = toil.importFile(f"file://{path}")

            # Run the workflow
            outputFileID = toil.start(HelloWorld(inputFileID))
        else:
            outputFileID = toil.restart()

        toil.exportFile(
            outputFileID,
            "file://" + os.path.join(tmp, "out.txt"),
        )
