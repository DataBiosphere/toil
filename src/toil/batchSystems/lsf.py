# Copyright (C) 2013 by Thomas Keane (tk2@sanger.ac.uk)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
import json
import logging
import math
import os
import re
import subprocess
from datetime import datetime
from random import randint
from typing import List

from dateutil.parser import parse
from dateutil.tz import tzlocal

from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
from toil.batchSystems.abstractGridEngineBatchSystem import \
    AbstractGridEngineBatchSystem
from toil.batchSystems.lsfHelper import (check_lsf_json_output_supported,
                                         parse_memory_limit,
                                         parse_memory_resource,
                                         per_core_reservation,
                                         parse_mem_and_cmd_from_output)
from toil.lib.misc import call_command

logger = logging.getLogger(__name__)


class LSFBatchSystem(AbstractGridEngineBatchSystem):

    class Worker(AbstractGridEngineBatchSystem.Worker):
        """LSF specific AbstractGridEngineWorker methods."""

        def getRunningJobIDs(self):
            times = {}
            with self.runningJobsLock:
                currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in
                                   self.runningJobs)

            if check_lsf_json_output_supported:
                stdout = call_command(["bjobs","-json","-o", "jobid stat start_time"])

                bjobs_records = self.parseBjobs(stdout)
                if bjobs_records:
                    for single_item in bjobs_records:
                        if single_item['STAT'] == 'RUN' and single_item['JOBID'] in currentjobs:
                            jobstart = parse(single_item['START_TIME'], default=datetime.now(tzlocal()))
                            times[currentjobs[single_item['JOBID']]] = datetime.now(tzlocal()) \
                            - jobstart
            else:
                times = self.fallbackRunningJobIDs(currentjobs)
            return times

        def fallbackRunningJobIDs(self, currentjobs):
            times = {}
            stdout = call_command(["bjobs", "-o", "jobid stat start_time delimiter='|'"])
            for curline in stdout.split('\n'):
                items = curline.strip().split('|')
                if items[0] in currentjobs and items[1] == 'RUN':
                    jobstart = parse(items[2], default=datetime.now(tzlocal()))
                    times[currentjobs[items[0]]] = datetime.now(tzlocal()) \
                        - jobstart
            return times

        def killJob(self, jobID):
            call_command(['bkill', self.getBatchSystemID(jobID)])

        def prepareSubmission(self, cpu, memory, jobID, command, jobName):
            return self.prepareBsub(cpu, memory, jobID) + [command]

        def submitJob(self, subLine):
            combinedEnv = self.boss.environment
            combinedEnv.update(os.environ)
            stdout = call_command(subLine, env=combinedEnv)
            # Example success: Job <39605914> is submitted to default queue <general>.
            # Example fail: Service class does not exist. Job not submitted.
            result_search = re.search('Job <(.*)> is submitted', stdout)

            if result_search:
                result = int(result_search.group(1))
                logger.debug("Got the job id: {}".format(result))
            else:
                logger.error("Could not submit job\nReason: {}".format(stdout))
                temp_id = randint(10000000, 99999999)
                #Flag this job to be handled by getJobExitCode
                result = "NOT_SUBMITTED_{}".format(temp_id)
            return result

        def getJobExitCode(self, lsfJobID):
            # the task is set as part of the job ID if using getBatchSystemID()
            if "NOT_SUBMITTED" in lsfJobID:
                logger.error("bjobs detected job failed to submit")
                return 1

            job, task = (lsfJobID, None)
            if '.' in lsfJobID:
                job, task = lsfJobID.split('.', 1)

            self.parseMaxMem(job)
            # first try bjobs to find out job state
            if check_lsf_json_output_supported:
                args = ["bjobs", "-json", "-o",
                        "user exit_code stat exit_reason pend_reason", str(job)]
                logger.debug("Checking job exit code for job via bjobs: "
                             "{}".format(job))
                stdout = call_command(args)
                bjobs_records = self.parseBjobs(stdout)
                if bjobs_records:
                    process_output = bjobs_records[0]
                    if 'STAT' in process_output:
                        process_status = process_output['STAT']
                        if process_status == 'DONE':
                            logger.debug(
                                "bjobs detected job completed for job: {}".format(job))
                            return 0
                        if process_status == 'PEND':
                            pending_info = ""
                            if 'PEND_REASON' in process_output:
                                if process_output['PEND_REASON']:
                                    pending_info = "\n" + \
                                        process_output['PEND_REASON']
                            logger.debug(
                                "bjobs detected job pending with: {}\nfor job: {}".format(pending_info, job))
                            return None
                        if process_status == 'EXIT':
                            exit_code = 1
                            exit_reason = ""
                            if 'EXIT_CODE' in process_output:
                                exit_code_str = process_output['EXIT_CODE']
                                if exit_code_str:
                                    exit_code = int(exit_code_str)
                            if 'EXIT_REASON' in process_output:
                                exit_reason = process_output['EXIT_REASON']
                            exit_info = ""
                            if exit_code:
                                exit_info = "\nexit code: {}".format(exit_code)
                            if exit_reason:
                                exit_info += "\nexit reason: {}".format(exit_reason)
                            logger.error(
                                "bjobs detected job failed with: {}\nfor job: {}".format(exit_info, job))
                            if "TERM_MEMLIMIT" in exit_reason:
                                return BatchJobExitReason.MEMLIMIT
                            return exit_code
                        if process_status == 'RUN':
                            logger.debug(
                                "bjobs detected job started but not completed for job: {}".format(job))
                            return None
                        if process_status in {'PSUSP', 'USUSP', 'SSUSP'}:
                            logger.debug(
                                "bjobs detected job suspended for job: {}".format(job))
                            return None

                        return self.getJobExitCodeBACCT(job)
            else:
                return self.fallbackGetJobExitCode(job)

        def getJobExitCodeBACCT(self,job):
            # if not found in bjobs, then try bacct (slower than bjobs)
            logger.debug("bjobs failed to detect job - trying bacct: "
                         "{}".format(job))

            args = ["bacct", "-l", str(job)]
            stdout = call_command(args)
            process_output = stdout.split('\n')
            for line in process_output:
                if line.find("Completed <done>") > -1 or line.find("<DONE>") > -1:
                    logger.debug("Detected job completed for job: "
                                 "{}".format(job))
                    return 0
                elif line.find("Completed <exit>") > -1 or line.find("<EXIT>") > -1:
                    logger.error("Detected job failed for job: "
                                 "{}".format(job))
                    return 1
            logger.debug("Can't determine exit code for job or job still "
                         "running: {}".format(job))
            return None

        def fallbackGetJobExitCode(self, job):
            args = ["bjobs", "-l", str(job)]
            logger.debug(f"Checking job exit code for job via bjobs (fallback): {job}")
            stdout = call_command(args)
            output = stdout.replace("\n                     ", "")
            process_output = output.split('\n')
            started = 0
            for line in process_output:
                if "Done successfully" in line or "Status <DONE>" in line:
                    logger.debug(f"bjobs detected job completed for job: {job}")
                    return 0
                elif "New job is waiting for scheduling" in line:
                    logger.debug(f"bjobs detected job pending scheduling for job: {job}")
                    return None
                elif "PENDING REASONS" in line or "Status <PEND>" in line:
                    logger.debug(f"bjobs detected job pending for job: {job}")
                    return None
                elif "Exited with exit code" in line:
                    exit = int(line[line.find("Exited with exit code ")+22:].split('.')[0])
                    logger.error(f"bjobs detected job exit code {exit} for job {job}")
                    return exit
                elif "Completed <exit>" in line:
                    logger.error(f"bjobs detected job failed for job: {job}")
                    return 1
                elif line.find("Started on ") > -1 or "Status <RUN>" in line:
                    started = 1
            if started == 1:
                logger.debug(f"bjobs detected job started but not completed: {job}")
                return None

            return self.getJobExitCodeBACCT(job)

        """
        Implementation-specific helper methods
        """
        def prepareBsub(self, cpu: int, mem: int, jobID: int) -> List[str]:
            """
            Make a bsub commandline to execute.

            params:
              cpu: number of cores needed
              mem: number of bytes of memory needed
              jobID: ID number of the job
            """
            bsubMem = []
            if mem:
                mem = float(mem) / 1024 ** 3
                if per_core_reservation() and cpu:
                    mem = mem / math.ceil(cpu)
                mem_resource = parse_memory_resource(mem)
                mem_limit = parse_memory_limit(mem)
                bsubMem = ['-R',
                           f'select[mem>{mem_resource}] '
                           f'rusage[mem={mem_resource}]',
                           '-M', mem_limit]
            bsubCpu = [] if cpu is None else ['-n', str(math.ceil(cpu))]
            bsubline = ["bsub", "-cwd", ".", "-J", f"toil_job_{jobID}"]
            bsubline.extend(bsubMem)
            bsubline.extend(bsubCpu)
            stdoutfile: str = self.boss.formatStdOutErrPath(jobID, '%J', 'out')
            stderrfile: str = self.boss.formatStdOutErrPath(jobID, '%J', 'err')
            bsubline.extend(['-o', stdoutfile, '-e', stderrfile])
            lsfArgs = os.getenv('TOIL_LSF_ARGS')
            if lsfArgs:
                bsubline.extend(lsfArgs.split())
            return bsubline

        def parseBjobs(self,bjobs_output_str):
            """
            Parse records from bjobs json type output
            params:
                bjobs_output_str: stdout of bjobs json type output
            """
            bjobs_dict = None
            bjobs_records = None
            # Handle Cannot connect to LSF. Please wait ... type messages
            dict_start = bjobs_output_str.find('{')
            dict_end = bjobs_output_str.rfind('}')
            if dict_start != -1 and dict_end != -1:
                bjobs_output = bjobs_output_str[dict_start:(dict_end+1)]
                try:
                    bjobs_dict = json.loads(bjobs_output)
                except json.decoder.JSONDecodeError:
                    logger.error("Could not parse bjobs output: {}".format(bjobs_output_str))
                if 'RECORDS' in bjobs_dict:
                    bjobs_records = bjobs_dict['RECORDS']
            if bjobs_records is None:
                logger.error("Could not find bjobs output json in: {}".format(bjobs_output_str))

            return bjobs_records

        def parseMaxMem(self, jobID):
            """
            Parse the maximum memory from job.

            :param jobID: ID number of the job
            """
            try:
                output = subprocess.check_output(["bjobs", "-l", str(jobID)], universal_newlines=True)
                max_mem, command = parse_mem_and_cmd_from_output(output=output)
                if not max_mem:
                    logger.warning(f"[job ID {jobID}] Unable to Collect Maximum Memory Usage: {output}")
                    return

                if not command:
                    logger.warning(f"[job ID {jobID}] Cannot Parse Max Memory Due to Missing Command String: {output}")
                else:
                    logger.info(f"[job ID {jobID}, Command {command.group(1)}] Max Memory Used: {max_mem.group(1)}")
                return max_mem
            except subprocess.CalledProcessError as e:
                logger.warning(f"[job ID {jobID}] Unable to Collect Maximum Memory Usage: {e}")

    def getWaitDuration(self):
        """We give LSF a second to catch its breath (in seconds)"""
        return 60
