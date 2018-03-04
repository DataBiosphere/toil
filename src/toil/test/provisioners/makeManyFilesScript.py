from toil.job import Job
from toil.common import Toil
import subprocess
import os
import time

"""
Used to run 1000 small jobs to test toil's autoscaling.

Base command to run: TOIL_APPLIANCE_SELF=quay.io/ucsc_cgl/toil:latest python makeManyFilesScript.py --provisioner aws --nodeType t2.micro --batchSystem mesos --disableCaching --logDebug --logFile logFile_thou aws:us-west-1:thoutest &> thousand_log.txt
"""

def initialize_jobs(job, memory="0.2G", cores=0.2, disk="0.2G"):
    job.fileStore.logToMaster('initialize_jobs')

def make_smallfile(job, uniquename, memory="0.2G", cores=0.2, disk="0.2G"):
    job.fileStore.logToMaster("make_smallfile")
    tempDir = job.fileStore.getLocalTempDir()

    time.sleep(20)

    cmd = 'echo ' + uniquename + ' > ' + os.path.join(tempDir, uniquename) + '.txt'
    this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    this_process.communicate()

    output_filename = os.path.join(tempDir, uniquename) + '.txt'
    output_file = job.fileStore.writeGlobalFile(output_filename)
    return output_file


if __name__ == "__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.logLevel = "DEBUG"
    options.clean = 'never'
    options.cleanWorkDir = True
    with Toil(options) as toil:
        job0 = Job.wrapJobFn(initialize_jobs)

        for i in range(1000):
            uniquename = 'job_' + str(i) + '_j'
            job = Job.wrapJobFn(make_smallfile, uniquename)
            job0.addChild(job)

        toil.start(job0)