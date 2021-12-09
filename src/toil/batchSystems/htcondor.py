# Copyright (C) 2018, HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import math
import os
import time
from typing import Any, Dict, Optional

import htcondor

from toil.batchSystems.abstractGridEngineBatchSystem import (
    AbstractGridEngineBatchSystem,
)

logger = logging.getLogger(__name__)


class HTCondorBatchSystem(AbstractGridEngineBatchSystem):
    # When using HTCondor, the Schedd handles scheduling

    class Worker(AbstractGridEngineBatchSystem.Worker):

        # Override the createJobs method so that we can use htcondor.Submit objects
        # and so that we can get disk allocation requests and ceil the CPU request.
        def createJobs(self, newJob: Any) -> bool:
            activity = False

            if newJob is not None:
                self.waitingJobs.append(newJob)

            # Queue jobs as necessary:
            while len(self.waitingJobs) > 0 and len(self.runningJobs) < int(self.boss.config.maxLocalJobs):
                activity = True
                jobID, cpu, memory, disk, jobName, command = self.waitingJobs.pop(0)

                # Prepare the htcondor.Submit object
                submitObj: htcondor.Submit = self.prepareHTSubmission(cpu, memory, disk, jobID, jobName, command)
                logger.debug("Submitting %r", submitObj)

                # Submit job and get batch system ID (i.e. the ClusterId)
                batchJobID = self.submitJob(submitObj)
                logger.debug("Submitted job %s", str(batchJobID))

                # Store dict for mapping Toil job ID to batch job ID
                # TODO: Note that this currently stores a tuple of (batch system
                # ID, Task), but the second value is None by default and doesn't
                # seem to be used
                self.batchJobIDs[jobID] = (batchJobID, None)

                # Add to queue of queued ("running") jobs
                self.runningJobs.add(jobID)

            return activity

        def prepareHTSubmission(self, cpu: int, memory: int, disk: int, jobID: int, jobName: str, command: str) -> htcondor.Submit:

            # Convert resource requests
            cpu = int(math.ceil(cpu)) # integer CPUs
            ht_memory = float(memory)/1024 # memory in KB
            ht_disk = float(disk)/1024 # disk in KB

            # NOTE: formatStdOutErrPath() puts files in the Toil workflow directory, which defaults
            # to being in the system temporary directory ($TMPDIR, /tmp) which is unlikely to be on
            # a shared filesystem. So to make this work we need to set should_transfer_files = Yes
            # in the submit file, so that HTCondor will write the standard output/error files on the
            # compute node, then transfer back once the job has completed.
            stdoutfile: str = self.boss.formatStdOutErrPath(jobID, '$(cluster)', 'out')
            stderrfile: str = self.boss.formatStdOutErrPath(jobID, '$(cluster)', 'err')
            condorlogfile: str = self.boss.formatStdOutErrPath(jobID, '$(cluster)', 'events')

            # Execute the entire command as /bin/sh -c "command"
            # TODO: Transfer the jobStore directory if using a local file store with a relative path.
            submit_parameters = {
                'executable': '/bin/sh',
                'transfer_executable': 'False',
                'arguments': f'''"-c '{command}'"'''.encode('utf-8'),    # Workaround for HTCondor Python bindings Unicode conversion bug
                'environment': self.getEnvString(),
                'getenv': 'True',
                'should_transfer_files': 'Yes',   # See note above for stdoutfile, stderrfile
                'output': stdoutfile,
                'error': stderrfile,
                'log': condorlogfile,
                'request_cpus': f'{cpu}',
                'request_memory': f'{ht_memory:.3f}KB',
                'request_disk': f'{ht_disk:.3f}KB',
                'leave_in_queue': '(JobStatus == 4)',
                '+IsToilJob': 'True',
                '+ToilJobID': f'{jobID}',
                '+ToilJobName': f'"{jobName}"',
                '+ToilJobKilled': 'False',
            }

            # Extra parameters for HTCondor
            extra_parameters = os.getenv('TOIL_HTCONDOR_PARAMS')
            if extra_parameters is not None:
                logger.debug(f"Extra HTCondor parameters added to submit file from TOIL_HTCONDOR_PARAMS env. variable: {extra_parameters}")
                for parameter, value in [parameter_value.split('=', 1) for parameter_value in extra_parameters.split(';')]:
                    parameter = parameter.strip()
                    value = value.strip()
                    if parameter in submit_parameters:
                        raise ValueError(f"Some extra parameters are incompatible: {extra_parameters}")

                    submit_parameters[parameter] = value

            # Return the Submit object
            return htcondor.Submit(submit_parameters)

        def submitJob(self, submitObj):

            # Queue the job using a Schedd transaction
            schedd = self.connectSchedd()
            with schedd.transaction() as txn:
                batchJobID = submitObj.queue(txn)

            # Return the ClusterId
            return batchJobID

        def getRunningJobIDs(self):

            # Get all Toil jobs that are running
            requirements = '(JobStatus == 2) && (IsToilJob)'
            projection = ['ClusterId', 'ToilJobID', 'EnteredCurrentStatus']
            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,
                                    projection = projection)

            # Only consider the Toil jobs that are part of this workflow
            batchJobIDs = [batchJobID for (batchJobID, task) in self.batchJobIDs.values()]
            job_runtimes = {}
            for ad in ads:
                batchJobID = int(ad['ClusterId'])
                jobID = int(ad['ToilJobID'])
                if not (batchJobID in batchJobIDs):
                    continue

                # HTCondor stores the start of the runtime as a Unix timestamp
                runtime = time.time() - ad['EnteredCurrentStatus']
                job_runtimes[jobID] = runtime

            return job_runtimes

        def killJob(self, jobID):
            batchJobID = self.batchJobIDs[jobID][0]
            logger.debug(f"Killing HTCondor job {batchJobID}")

            # Set the job to be killed when its exit status is checked
            schedd = self.connectSchedd()
            job_spec = f'(ClusterId == {batchJobID})'
            schedd.edit(job_spec, 'ToilJobKilled', 'True')

        def getJobExitCode(self, batchJobID):
            logger.debug(f"Getting exit code for HTCondor job {batchJobID}")

            status = {
                1: 'Idle',
                2: 'Running',
                3: 'Removed',
                4: 'Completed',
                5: 'Held',
                6: 'Transferring Output',
                7: 'Suspended'
            }

            requirements = f'(ClusterId == {batchJobID})'
            projection = ['JobStatus', 'ToilJobKilled', 'ExitCode',
                              'HoldReason', 'HoldReasonSubCode']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,  projection = projection)

            # Make sure a ClassAd was returned
            try:
                try:
                    ad = next(ads)
                except TypeError:
                    ad = ads.next()
            except StopIteration:
                logger.error(
                    f"No HTCondor ads returned using constraint: {requirements}")
                raise

            # Make sure only one ClassAd was returned
            try:
                try:
                    next(ads)
                except TypeError:
                    ads.next()
            except StopIteration:
                pass
            else:
                logger.warning(
                    f"Multiple HTCondor ads returned using constraint: {requirements}")

            if ad['ToilJobKilled']:
                logger.debug(f"HTCondor job {batchJobID} was killed by Toil")

                # Remove the job from the Schedd and return 1
                job_spec = f'ClusterId == {batchJobID}'
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            elif status[ad['JobStatus']] == 'Completed':
                logger.debug("HTCondor job {} completed with exit code {}".format(
                    batchJobID, ad['ExitCode']))

                # Remove the job from the Schedd and return its exit code
                job_spec = f'ClusterId == {batchJobID}'
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return int(ad['ExitCode'])

            elif status[ad['JobStatus']] == 'Held':
                logger.error("HTCondor job {} was held: '{} (sub code {})'".format(
                    batchJobID, ad['HoldReason'], ad['HoldReasonSubCode']))

                # Remove the job from the Schedd and return 1
                job_spec = f'ClusterId == {batchJobID}'
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            else: # Job still running or idle or doing something else
                logger.debug("HTCondor job {} has not completed (Status: {})".format(
                    batchJobID, status[ad['JobStatus']]))
                return None


        """
        Implementation-specific helper methods
        """

        def connectSchedd(self):
            '''Connect to HTCondor Schedd and return a Schedd object'''

            condor_host = os.getenv('TOIL_HTCONDOR_COLLECTOR')
            schedd_name = os.getenv('TOIL_HTCONDOR_SCHEDD')

            # If TOIL_HTCONDOR_ variables are set, use them to find the Schedd
            if condor_host and schedd_name:
                logger.debug(
                    "Connecting to HTCondor Schedd {} using Collector at {}".format(
                        schedd_name, condor_host))
                try:
                    schedd_ad = htcondor.Collector(condor_host).locate(
                        htcondor.DaemonTypes.Schedd, schedd_name)
                except OSError:
                    logger.error(
                        f"Could not connect to HTCondor Collector at {condor_host}")
                    raise
                except ValueError:
                    logger.error(
                        f"Could not find HTCondor Schedd with name {schedd_name}")
                    raise
                else:
                    schedd = htcondor.Schedd(schedd_ad)

            # Otherwise assume the Schedd is on the local machine
            else:
                logger.debug("Connecting to HTCondor Schedd on local machine")
                schedd = htcondor.Schedd()

            # Ping the Schedd to make sure it's there and responding
            try:
                schedd.xquery(limit = 0)
            except RuntimeError:
                logger.error("Could not connect to HTCondor Schedd")
                raise

            return schedd

        def getEnvString(self):
            """
            Build an environment string that a HTCondor Submit object can use.

            For examples of valid strings, see:
            http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html#man-condor-submit-environment
            """

            env_items = []
            if self.boss.environment:
                for key, value in self.boss.environment.items():

                    # Each variable should be in the form of <key>='<value>'
                    env_string = key + "="

                    # The entire value should be encapsulated in single quotes
                    # Quote marks (single or double) that are part of the value should be duplicated
                    env_string += "'" + value.replace("'", "''").replace('"', '""') + "'"

                    env_items.append(env_string)

            # The entire string should be encapsulated in double quotes
            # Each variable should be separated by a single space
            return '"' + ' '.join(env_items) + '"'

    # Override the issueBatchJob method so HTCondor can be given the disk request
    def issueBatchJob(self, jobNode, job_environment: Optional[Dict[str, str]] = None):
        # Avoid submitting internal jobs to the batch queue, handle locally
        localID = self.handleLocalJob(jobNode)
        if localID:
            return localID
        else:
            self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
            jobID = self.getNextJobID()
            self.currentJobs.add(jobID)

            # Add the jobNode.disk and jobNode.jobName to the job tuple
            self.newJobsQueue.put((jobID, jobNode.cores, jobNode.memory, jobNode.disk, jobNode.jobName, jobNode.command,
                                   job_environment))
            logger.debug("Issued the job command: %s with job id: %s ", jobNode.command, str(jobID))
        return jobID
