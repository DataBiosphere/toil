from toil.job import Job
from toil.common import Toil
import subprocess
import os


def initialize_jobs(job):
    job.fileStore.logToMaster('initialize_jobs')


def runQC(job, cwl_file, cwl_filename, yml_file, yml_filename, outputs_dir, output_num):
    job.fileStore.logToMaster("runQC")
    tempDir = job.fileStore.getLocalTempDir()

    cwl = job.fileStore.readGlobalFile(cwl_file, userPath=os.path.join(tempDir, cwl_filename))
    yml = job.fileStore.readGlobalFile(yml_file, userPath=os.path.join(tempDir, yml_filename))

    subprocess.check_call(["toil-cwl-runner", cwl, yml])

    output_filename = "output.txt"
    output_file = job.fileStore.writeGlobalFile(output_filename)
    job.fileStore.readGlobalFile(output_file, userPath=os.path.join(outputs_dir, "sample_" + output_num + "_" + output_filename))
    return output_file


if __name__ == "__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"
    with Toil(options) as toil:

        # specify the folder where the cwl and yml files live
        inputs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cwlExampleFiles")
        # specify where you wish the outputs to be written
        outputs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cwlExampleFiles")

        job0 = Job.wrapJobFn(initialize_jobs)

        cwl_filename = "hello.cwl"
        cwl_file = toil.importFile("file://" + os.path.abspath(os.path.join(inputs_dir, cwl_filename)))

        # add list of yml config inputs here or import and construct from file
        yml_files = ["hello1.yml", "hello2.yml", "hello3.yml"]
        i = 0
        for yml in yml_files:
            i = i + 1
            yml_file = toil.importFile("file://" + os.path.abspath(os.path.join(inputs_dir, yml)))
            yml_filename = yml
            job = Job.wrapJobFn(runQC, cwl_file, cwl_filename, yml_file, yml_filename, outputs_dir, output_num=str(i))
            job0.addChild(job)

        toil.start(job0)
