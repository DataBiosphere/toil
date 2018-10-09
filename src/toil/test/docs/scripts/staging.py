import os
from toil.common import Toil
from toil.job import Job


class HelloWorld(Job):
    def __init__(self, inputFileID):
        Job.__init__(self,  memory="2G", cores=2, disk="3G")
        self.inputFileID = inputFileID

        with fileStore.readGlobalFileStream(self.inputFileID) as fi:
            with fileStore.writeGlobalFileStream() as (fo, outputFileID):
                fo.write(fi.read() + 'World!')
        return outputFileID


if __name__=="__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        if not toil.options.restart:
            inputFileID = toil.importFile(os.path.abspath("/stagingExampleFiles/in.txt"))
            outputFileID = toil.start(HelloWorld(inputFileID))
        else:
            outputFileID = toil.restart()

        toil.exportFile(outputFileID, 'file:///some/other/local/path')
