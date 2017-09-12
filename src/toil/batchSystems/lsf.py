#Copyright (C) 2013 by Thomas Keane (tk2@sanger.ac.uk)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.
from __future__ import absolute_import
import logging
import subprocess
import time
from datetime import date
import os

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem
from toil.batchSystems.lsfHelper import parse_memory, per_core_reservation

logger = logging.getLogger( __name__ )


class LSFBatchSystem(AbstractGridEngineBatchSystem):
    """
    The interface for running jobs on lsf, runs all the jobs you give it as they come in,
    but in parallel.
    """
    class Worker(AbstractGridEngineBatchSystem.Worker):

        def getRunningJobIDs(self):
            """Gets a map of jobs (as jobIDs) currently running (not just waiting)
            and a how long they have been running for (in seconds).
            """
            times = {}
            currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in
                    self.runningJobs)
            process = subprocess.Popen(["bjobs"], stdout = subprocess.PIPE)

            for curline in process.stdout:
                items = curline.strip().split()
                if (len(items) > 9 and (items[0]) in currentjobs) and items[2] == 'RUN':
                    jobstart = "/".join(items[7:9]) + '/' + str(date.today().year)
                    jobstart = jobstart + ' ' + items[9]
                    try:
                        jobstart = time.mktime(time.strptime(jobstart,"%b/%d/%Y %H:%M"))
                        jobstart = time.mktime(time.strptime(jobstart,"%m/%d/%Y %H:%M:%S"))
                        times[currentjobs[items[0]]] = time.time() - jobstart
                    except TypeError err:
                        logging.error("Got error parsing bjobs output %s: %s",
                                process.stdout, repr(err))
            return times

        def killJob(self, jobID):
            subprocess.check_call(["bkill", self.getBatchSystemID(jobID)])

        def prepareSubmission(self, cpu, memory, jobID, command):
            return self.prepareBsub(cpu, memory, jobID) + [command]

        def submitJob(self, subLine):
            process = subprocess.Popen(" ".join(subLine), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            liney = process.stdout.readline()
            logger.debug("BSUB: " + liney)
            result = int(liney.strip().split()[1].strip('<>'))
            logger.debug("Got the job id: %s" % (str(result)))
            return result

        def getJobExitCode(self, lsfJobID):
            job = lsfJobID

            #first try bjobs to find out job state
            args = ["bjobs", "-l", str(job)]
            logger.debug("Checking job exit code for job via bjobs: " + str(job))
            process = subprocess.Popen(" ".join(args), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            started = 0
            for line in process.stdout:
                if line.find("Done successfully") > -1:
                    logger.debug("bjobs detected job completed for job: " + str(job))
                    return 0
                elif line.find("Completed <exit>") > -1:
                    logger.debug("bjobs detected job failed for job: " + str(job))
                    return 1
                elif line.find("New job is waiting for scheduling") > -1:
                    logger.debug("bjobs detected job pending scheduling for job: " + str(job))
                    return None
                elif line.find("PENDING REASONS") > -1:
                    logger.debug("bjobs detected job pending for job: " + str(job))
                    return None
                elif line.find("Started on ") > -1:
                    started = 1

            if started == 1:
                logger.debug("bjobs detected job started but not completed: " + str(job))
                return None

            #if not found in bjobs, then try bacct (slower than bjobs)
            logger.debug("bjobs failed to detect job - trying bacct: " + str(job))

            args = ["bacct", "-l", str(job)]
            logger.debug("Checking job exit code for job via bacct:" + str(job))
            process = subprocess.Popen(" ".join(args), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            for line in process.stdout:
                if line.find("Completed <done>") > -1:
                    logger.debug("Detected job completed for job: " + str(job))
                    return 0
                elif line.find("Completed <exit>") > -1:
                    logger.debug("Detected job failed for job: " + str(job))
                    return 1
            logger.debug("Cant determine exit code for job or job still running: " + str(job))
            return None

        def prepareBsub(self, cpu, mem, jobID):
            """
            Make a bsub commandline to execute.

            params:
              cpu: number of cores needed
              mem: number of bytes of memory needed
            """
            if mem:
                if per_core_reservation():
                    mem = parse_memory(float(mem)/1024**3/int(cpu))
                else:
                    mem = parse_memory(float(mem)/1024**3)
                bsubMem = '-R "select[type==X86_64 && mem > ' + str(mem) + '] '\
                    'rusage[mem=' + str(mem) + ']" -M' + str(mem)
            else:
                bsubMem = ''
            cpuStr = '' if cpu is None else '-n ' + str(int(cpu))
            bsubline = ["bsub", bsubMem, cpuStr,"-cwd", ".", "-o", "/dev/null",
                    "-e", "/dev/null", '-J', 'toil_job_' + str(jobID)]
            lsfArgs = os.getenv('TOIL_LSF_ARGS')
            if lsfArgs:
                bsubline.extend(lsfArgs.split())
            return bsubline

    @classmethod
    def getWaitDuration(cls):
        """We give LSF a second to catch its breath (in seconds)
        """
        return 15


    @classmethod
    def obtainSystemConstants(self):
        p = subprocess.Popen(["lshosts"], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

        line = p.stdout.readline()
        items = line.strip().split()
        num_columns = len(items)
        cpu_index = None
        mem_index = None
        for i in range(num_columns):
                if items[i] == 'ncpus':
                        cpu_index = i
                elif items[i] == 'maxmem':
                        mem_index = i

        if cpu_index is None or mem_index is None:
                RuntimeError("lshosts command does not return ncpus or maxmem columns")

        p.stdout.readline()

        maxCPU = 0
        maxMEM = MemoryString("0")
        for line in p.stdout:
                items = line.strip().split()
                if len(items) < num_columns:
                        RuntimeError("lshosts output has a varying number of columns")
                if items[cpu_index] != '-' and items[cpu_index] > maxCPU:
                        maxCPU = items[cpu_index]
                if items[mem_index] != '-' and MemoryString(items[mem_index]) > maxMEM:
                        maxMEM = MemoryString(items[mem_index])

        if maxCPU is 0 or maxMEM is 0:
                RuntimeError("lshosts returns null ncpus or maxmem info")
        logger.debug("Got the maxCPU: %s" % (maxMEM))
        return maxCPU, maxMEM
