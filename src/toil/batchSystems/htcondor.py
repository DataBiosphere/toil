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

from __future__ import absolute_import
from builtins import str

import sys
import os
import logging
import time
import math

from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem

import htcondor
import classad

logger = logging.getLogger(__name__)

class HTCondorBatchSystem(AbstractGridEngineBatchSystem):
    # When using HTCondor, the Schedd handles scheduling

    class Worker(AbstractGridEngineBatchSystem.Worker):

        # Override the createJobs method so that we can use htcondor.Submit objects
        # and so that we can get disk allocation requests and ceil the CPU request.
        def createJobs(self, newJob):
            activity = False

            if newJob is not None:
                self.waitingJobs.append(newJob)

            # Queue jobs as necessary:
            while len(self.waitingJobs) > 0:
                activity = True
                jobID, cpu, memory, disk, jobName, command = self.waitingJobs.pop(0)

                # Prepare the htcondor.Submit object
                submitObj = self.prepareSubmission(cpu, memory, disk, jobID, jobName, command)
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

                # Add to allocated resources
                self.allocatedCpus[jobID] = int(math.ceil(cpu))

            return activity

        def prepareSubmission(self, cpu, memory, disk, jobID, jobName, command):

            # Convert resource requests
            cpu = int(math.ceil(cpu)) # integer CPUs
            memory = float(memory)/1024 # memory in KB
            disk = float(disk)/1024 # disk in KB

            # Workaround for HTCondor Python bindings Unicode conversion bug
            command = command.encode('utf8')

            # Execute the entire command as /bin/sh -c "command"
            # TODO: Transfer the jobStore directory if using a local file store with a relative path.
            submit_parameters = {
                'executable': '/bin/sh',
                'transfer_executable': 'False',
                'arguments': '''"-c '{0}'"'''.format(command),
                'environment': self.getEnvString(),
                'request_cpus': '{0}'.format(cpu),
                'request_memory': '{0:.3f}KB'.format(memory),
                'request_disk': '{0:.3f}KB'.format(disk),
                'leave_in_queue': '(JobStatus == 4)',
                '+IsToilJob': 'True',
                '+ToilJobID': '{0}'.format(jobID),
                '+ToilJobName': '"{0}"'.format(jobName),
                '+ToilJobKilled': 'False',
            }

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
            logger.debug("Killing HTCondor job {0}".format(batchJobID))

            # Set the job to be killed when its exit status is checked
            schedd = self.connectSchedd()
            job_spec = '(ClusterId == {0})'.format(batchJobID)
            schedd.edit(job_spec, 'ToilJobKilled', 'True')

        def getJobExitCode(self, batchJobID):
            logger.debug("Getting exit code for HTCondor job {0}".format(batchJobID))

            status = {
                1: 'Idle',
                2: 'Running',
                3: 'Removed',
                4: 'Completed',
                5: 'Held',
                6: 'Transferring Output',
                7: 'Suspended'
            }

            requirements = '(ClusterId == {0})'.format(batchJobID)
            projection = ['JobStatus', 'ToilJobKilled', 'ExitCode',
                              'HoldReason', 'HoldReasonSubCode']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,  projection = projection)

            # Make sure a ClassAd was returned
            try:
                ad = ads.next()
            except StopIteration:
                logger.error(
                    "No HTCondor ads returned using constraint: {0}".format(requirements))
                raise

            # Make sure only one ClassAd was returned
            try:
                ads.next()
            except StopIteration:
                pass
            else:
                logger.warning(
                    "Multiple HTCondor ads returned using constraint: {0}".format(requirements))

            if ad['ToilJobKilled']:
                logger.debug("HTCondor job {0} was killed by Toil".format(batchJobID))

                # Remove the job from the Schedd and return 1
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            elif status[ad['JobStatus']] == 'Completed':
                logger.debug("HTCondor job {0} completed with exit code {1}".format(
                    batchJobID, ad['ExitCode']))

                # Remove the job from the Schedd and return its exit code
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return int(ad['ExitCode'])

            elif status[ad['JobStatus']] == 'Held':
                logger.error("HTCondor job {0} was held: '{1} (sub code {2})'".format(
                    batchJobID, ad['HoldReason'], ad['HoldReasonSubCode']))

                # Remove the job from the Schedd and return 1
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            else: # Job still running or idle or doing something else
                logger.debug("HTCondor job {0} has not completed (Status: {1})".format(
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
                    "Connecting to HTCondor Schedd {0} using Collector at {1}".format(
                        schedd_name, condor_host))
                try:
                    schedd_ad = htcondor.Collector(condor_host).locate(
                        htcondor.DaemonTypes.Schedd, schedd_name)
                except IOError:
                    logger.error(
                        "Could not connect to HTCondor Collector at {0}".format(condor_host))
                    raise
                except ValueError:
                    logger.error(
                        "Could not find HTCondor Schedd with name {0}".format(schedd_name))
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
            '''Build an environment string that a HTCondor Submit object can use.

            For examples of valid strings, see:
            http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html#man-condor-submit-environment

            '''

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
    def issueBatchJob(self, jobNode):
        # Avoid submitting internal jobs to the batch queue, handle locally
        localID = self.handleLocalJob(jobNode)
        if localID:
            return localID
        else:
            self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
            jobID = self.getNextJobID()
            self.currentJobs.add(jobID)

            # Add the jobNode.disk and jobNode.jobName to the job tuple
            self.newJobsQueue.put((jobID, jobNode.cores, jobNode.memory, jobNode.disk, jobNode.jobName, jobNode.command))
            logger.debug("Issued the job command: %s with job id: %s ", jobNode.command, str(jobID))
        return jobID

    @classmethod
    def obtainSystemConstants(cls):

        # Since it's not always clear what the max cpus and max memory available
        # in an HTCondor slot might be, use some reasonable constants for now.
        # TODO: Use a htcondor.Collector().query() to determine reasonable values.
        max_cpu = 4
        max_mem = 4e9
        return max_cpu, max_mem
