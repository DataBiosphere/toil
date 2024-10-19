import os

from toil.common import Toil
from toil.job import Job, PromisedRequirement
from toil.lib.io import mkdtemp


def parentJob(job):
    downloadJob = Job.wrapJobFn(
        stageFn,
        "file://" + os.path.realpath(__file__),
        cores=0.1,
        memory="32M",
        disk="1M",
    )
    job.addChild(downloadJob)

    analysis = Job.wrapJobFn(
        analysisJob,
        fileStoreID=downloadJob.rv(0),
        disk=PromisedRequirement(downloadJob.rv(1)),
    )
    job.addFollowOn(analysis)


def stageFn(job, url):
    importedFile = job.fileStore.import_file(url)
    return importedFile, importedFile.size


def analysisJob(job, fileStoreID):
    # now do some analysis on the file
    pass


if __name__ == "__main__":
    jobstore: str = mkdtemp("tutorial_requirements")
    os.rmdir(jobstore)
    options = Job.Runner.getDefaultOptions(jobstore)
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
        toil.start(Job.wrapJobFn(parentJob))
