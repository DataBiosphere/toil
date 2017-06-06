# Copyright (c) 2016 Duke Center for Genomic and Computational Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import logging
import os
from pipes import quote
import subprocess
import time
import math

# Python 3 compatibility imports
from six.moves.queue import Empty, Queue
from six import iteritems

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem

logger = logging.getLogger(__name__)

class SlurmBatchSystem(AbstractGridEngineBatchSystem):

    class Worker(AbstractGridEngineBatchSystem.Worker):

        def getRunningJobIDs(self):
            # Should return a dictionary of Job IDs and number of seconds
            times = {}
            currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in self.runningJobs)
            # currentjobs is a dictionary that maps a slurm job id (string) to our own internal job id
            # squeue arguments:
            # -h for no header
            # --format to get jobid i, state %t and time days-hours:minutes:seconds

            lines = subprocess.check_output(['squeue', '-h', '--format', '%i %t %M']).split('\n')
            for line in lines:
                values = line.split()
                if len(values) < 3:
                    continue
                slurm_jobid, state, elapsed_time = values
                if slurm_jobid in currentjobs and state == 'R':
                    seconds_running = self.parse_elapsed(elapsed_time)
                    times[currentjobs[slurm_jobid]] = seconds_running

            return times

        def killJob(self, jobID):
            subprocess.check_call(['scancel', self.getBatchSystemID(jobID)])

        def prepareSubmission(self, cpu, memory, jobID, command):
            return self.prepareSbatch(cpu, memory, jobID) + ['--wrap={}'.format(command)]

        def submitJob(self, subLine):
            try:
                output = subprocess.check_output(subLine, stderr=subprocess.STDOUT)
                # sbatch prints a line like 'Submitted batch job 2954103'
                result = int(output.strip().split()[-1])
                logger.debug("sbatch submitted job %d", result)
                return result
            except subprocess.CalledProcessError as e:
                logger.error("sbatch command failed with code %d: %s", e.returncode, e.output)
                raise e
            except OSError as e:
                logger.error("sbatch command failed")
                raise e

        def getJobExitCode(self, slurmJobID):
            logger.debug("Getting exit code for slurm job %d", int(slurmJobID))
            
            state, rc = self._getJobDetailsFromSacct(slurmJobID)
            
            if rc == -999:
                state, rc = self._getJobDetailsFromScontrol(slurmJobID)
            
            logger.debug("s job state is %s", state)
            # If Job is in a running state, return None to indicate we don't have an update                                 
            if state in ('PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'RESIZING', 'SUSPENDED'):
                return None
            
            return rc
            
        def _getJobDetailsFromSacct(self, slurmJobID):
            # SLURM job exit codes are obtained by running sacct.
            args = ['sacct',
                    '-n', # no header
                    '-j', str(slurmJobID), # job
                    '--format', 'State,ExitCode', # specify output columns
                    '-P', # separate columns with pipes
                    '-S', '1970-01-01'] # override start time limit
            
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            rc = process.returncode
            
            if rc != 0:
                # no accounting system or some other error
                return (None, -999)
            
            for line in process.stdout:
                values = line.strip().split('|')
                if len(values) < 2:
                    continue
                state, exitcode = values
                logger.debug("sacct job state is %s", state)
                # If Job is in a running state, return None to indicate we don't have an update
                status, _ = exitcode.split(':')
                logger.debug("sacct exit code is %s, returning status %s", exitcode, status)
                return (state, int(status))
            logger.debug("Did not find exit code for job in sacct output")
            return None

        def _getJobDetailsFromScontrol(self, slurmJobID):
            args = ['scontrol',
                    'show',
                    'job',
                    str(slurmJobID)]
    
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
            job = dict()
            for line in process.stdout:
                values = line.strip().split()
    
                # If job information is not available an error is issued:
                # slurm_load_jobs error: Invalid job id specified
                # There is no job information, so exit.
                if len(values)>0 and values[0] == 'slurm_load_jobs':
                    return (None, None)
                
                # Output is in the form of many key=value pairs, multiple pairs on each line
                # and multiple lines in the output. Each pair is pulled out of each line and
                # added to a dictionary
                for v in values:
                    bits = v.split('=')
                    job[bits[0]] = bits[1]
    
            state = job['JobState']
            try:
                exitcode = job['ExitCode']
                if exitcode is not None:
                    status, _ = exitcode.split(':')
                    logger.debug("scontrol exit code is %s, returning status %s", exitcode, status)
                    rc = int(status)
                else:
                    rc = None
            except KeyError:
                rc = None
            
            return (state, rc)

        """
        Implementation-specific helper methods
        """

        def prepareSbatch(self, cpu, mem, jobID):
            #  Returns the sbatch command line before the script to run
            sbatch_line = ['sbatch', '-Q', '-J', 'toil_job_{}'.format(jobID)]

            if self.boss.environment:
                argList = []
                
                for k, v in self.boss.environment.iteritems():
                    quoted_value = quote(os.environ[k] if v is None else v)
                    argList.append('{}={}'.format(k, quoted_value))
                    
                sbatch_line.append('--export=' + ','.join(argList))
            
            if mem is not None:
                # memory passed in is in bytes, but slurm expects megabytes
                sbatch_line.append('--mem={}'.format(int(mem) / 2 ** 20))
            if cpu is not None:
                sbatch_line.append('--cpus-per-task={}'.format(int(math.ceil(cpu))))

            # "Native extensions" for SLURM (see DRMAA or SAGA)
            nativeConfig = os.getenv('TOIL_SLURM_ARGS')
            if nativeConfig is not None:
                logger.debug("Native SLURM options appended to sbatch from TOIL_SLURM_RESOURCES env. variable: {}".format(nativeConfig))
                if ("--mem" in nativeConfig) or ("--cpus-per-task" in nativeConfig):
                    raise ValueError("Some resource arguments are incompatible: {}".format(nativeConfig))

                sbatch_line.extend(nativeConfig.split())

            return sbatch_line

        def parse_elapsed(self, elapsed):
            # slurm returns elapsed time in days-hours:minutes:seconds format
            # Sometimes it will only return minutes:seconds, so days may be omitted
            # For ease of calculating, we'll make sure all the delimeters are ':'
            # Then reverse the list so that we're always counting up from seconds -> minutes -> hours -> days
            total_seconds = 0
            try:
                elapsed = elapsed.replace('-', ':').split(':')
                elapsed.reverse()
                seconds_per_unit = [1, 60, 3600, 86400]
                for index, multiplier in enumerate(seconds_per_unit):
                    if index < len(elapsed):
                        total_seconds += multiplier * int(elapsed[index])
            except ValueError:
                pass  # slurm may return INVALID instead of a time
            return total_seconds

    """
    The interface for SLURM
    """

    @classmethod
    def getWaitDuration(cls):
        return 1.0

    @classmethod
    def obtainSystemConstants(cls):
        # sinfo -Ne --format '%m,%c'
        # sinfo arguments:
        # -N for node-oriented
        # -h for no header
        # -e for exact values (e.g. don't return 32+)
        # --format to get memory, cpu
        max_cpu = 0
        max_mem = MemoryString('0')
        lines = subprocess.check_output(['sinfo', '-Nhe', '--format', '%m %c']).split('\n')
        for line in lines:
            values = line.split()
            if len(values) < 2:
                continue
            mem, cpu = values
            max_cpu = max(max_cpu, int(cpu))
            max_mem = max(max_mem, MemoryString(mem + 'M'))
        if max_cpu == 0 or max_mem.byteVal() == 0:
            RuntimeError('sinfo did not return memory or cpu info')
        return max_cpu, max_mem
