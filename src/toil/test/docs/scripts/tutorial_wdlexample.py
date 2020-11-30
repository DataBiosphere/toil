import os
import subprocess

from toil.common import Toil
from toil.job import Job


def initialize_jobs(job):
    job.fileStore.logToMaster("initialize_jobs")


def runQC(job, wdl_file, wdl_filename, json_file, json_filename, outputs_dir, jar_loc,output_num):
    job.fileStore.logToMaster("runQC")
    tempDir = job.fileStore.getLocalTempDir()

    wdl = job.fileStore.readGlobalFile(wdl_file, userPath=os.path.join(tempDir, wdl_filename))
    json = job.fileStore.readGlobalFile(json_file, userPath=os.path.join(tempDir, json_filename))

    subprocess.check_call(["java","-jar",jar_loc,"run",wdl,"--inputs",json])

    output_filename = "output.txt"
    output_file = job.fileStore.writeGlobalFile(outputs_dir + output_filename)
    job.fileStore.readGlobalFile(output_file, userPath=os.path.join(outputs_dir, "sample_" + output_num + "_" + output_filename))
    return output_file


if __name__ == "__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:

        # specify the folder where the wdl and json files live
        inputs_dir = "wdlExampleFiles/"
        # specify where you wish the outputs to be written
        outputs_dir = "wdlExampleFiles/"
        # specify the location of your cromwell jar
        jar_loc = os.path.abspath("wdlExampleFiles/cromwell-35.jar")

        job0 = Job.wrapJobFn(initialize_jobs)

        wdl_filename = "hello.wdl"
        wdl_file = toil.importFile("file://" + os.path.abspath(os.path.join(inputs_dir, wdl_filename)))


        # add list of yml config inputs here or import and construct from file
        json_files = ["hello1.json", "hello2.json", "hello3.json"]
        i = 0
        for json in json_files:
            i = i + 1
            json_file = toil.importFile("file://" + os.path.join(inputs_dir, json))
            json_filename = json
            job = Job.wrapJobFn(runQC, wdl_file, wdl_filename, json_file, json_filename, outputs_dir, jar_loc, output_num=str(i))
            job0.addChild(job)

        toil.start(job0)
