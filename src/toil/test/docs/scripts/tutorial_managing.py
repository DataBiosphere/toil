from toil.common import Toil
from toil.job import Job

class LocalFileStoreJob(Job):
    def run(self, fileStore):
        # self.TempDir will always contain the name of a directory within the allocated disk space reserved for the job
        scratchDir = self.tempDir

        # Similarly create a temporary file.
        scratchFile = fileStore.getLocalTempFile()

if __name__=="__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"

    # Create an instance of FooJob which will have at least 10 gigabytes of storage space.
    j = LocalFileStoreJob(disk="10G")

    #Run the workflow
    with Toil(options) as toil:
        toil.start(j)