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
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING

import htcondor

if TYPE_CHECKING:
    import classad

from toil.batchSystems.abstractGridEngineBatchSystem import (
    AbstractGridEngineBatchSystem,
) 
from toil.lib.retry import retry

logger = logging.getLogger(__name__)

# We have one global lock to control access to the HTCondor scheduler
schedd_lock = Lock()


class HTCondorBatchSystem(AbstractGridEngineBatchSystem):
    # When using HTCondor, the Schedd handles scheduling

    class GridEngineThread(AbstractGridEngineBatchSystem.GridEngineThread[htcondor.Submit]):

        def prepareSubmission(
            self,
            cpu: float,
            memory: int,
            disk: int,
            walltime: int,
            jobID: int,
            command: str,
            jobName: str,
            job_environment: dict[str, str] | None = None,
            gpus: int | None = None,
        ) -> htcondor.Submit:
            if gpus:
                logger.warning(
                    "Scheduling job %s without enforcing accelerator requirement",
                    jobID,
                )

            # Convert resource requests
            cpu = int(math.ceil(cpu))  # integer CPUs
            ht_memory = float(memory) / 1024  # memory in KB
            ht_disk = float(disk) / 1024  # disk in KB

            # NOTE: format_std_out_err_path() by default puts files in the Toil
            # work directory, which defaults to being in the system temporary
            # directory ($TMPDIR, often /tmp) which is unlikely to be on a
            # shared filesystem. So to make this work more often without the
            # user setting --batchLogsDir we need to set should_transfer_files
            # = Yes in the submit file, so that HTCondor will write the
            # standard output/error files on the compute node, then transfer
            # back once the job has completed.
            stdoutfile: str = self.boss.format_std_out_err_path(
                jobID, "$(cluster)", "out"
            )
            stderrfile: str = self.boss.format_std_out_err_path(
                jobID, "$(cluster)", "err"
            )
            condorlogfile: str = self.boss.format_std_out_err_path(
                jobID, "$(cluster)", "events"
            )

            # Execute the entire command as /bin/sh -c "command"
            # TODO: Transfer the jobStore directory if using a local file store with a relative path.
            submit_parameters = {
                "executable": "/bin/sh",
                "transfer_executable": "False",
                "arguments": f'''"-c '{self.duplicate_quotes(command)}'"'''.encode(),  # Workaround for HTCondor Python bindings Unicode conversion bug
                "environment": self.getEnvString(job_environment or {}),
                "getenv": "True",
                "should_transfer_files": "Yes",  # See note above for stdoutfile, stderrfile
                "output": stdoutfile,
                "error": stderrfile,
                "log": condorlogfile,
                "request_cpus": f"{cpu}",
                "request_memory": f"{ht_memory:.3f}KB",
                "request_disk": f"{ht_disk:.3f}KB",
                "leave_in_queue": "(JobStatus == 4)",
                "+IsToilJob": "True",
                "+ToilJobID": f"{jobID}",
                "+ToilJobName": f'"{jobName}"',
                "+ToilJobKilled": "False",
            }

            # Extra parameters for HTCondor
            extra_parameters = os.getenv("TOIL_HTCONDOR_PARAMS")
            if extra_parameters is not None:
                logger.debug(
                    f"Extra HTCondor parameters added to submit file from TOIL_HTCONDOR_PARAMS env. variable: {extra_parameters}"
                )
                for parameter, value in [
                    parameter_value.split("=", 1)
                    for parameter_value in extra_parameters.split(";")
                ]:
                    parameter = parameter.strip()
                    value = value.strip()
                    if parameter in submit_parameters:
                        raise ValueError(
                            f"Some extra parameters are incompatible: {extra_parameters}"
                        )

                    submit_parameters[parameter] = value

            # Return the Submit object
            return htcondor.Submit(submit_parameters)

        @retry(errors=[htcondor.HTCondorIOError])
        def submitJob(self, submitObj: htcondor.Submit) -> int:

            # Queue the job using a Schedd transaction
            with self.connectSchedd() as schedd:
                with schedd.transaction() as txn:
                    batchJobID = submitObj.queue(txn)

            # Return the ClusterId
            return batchJobID

        def getRunningJobIDs(self) -> dict[int, float]:

            # Get all Toil jobs that are running
            constraint = "(JobStatus == 2) && (IsToilJob)"
            projection = ["ClusterId", "ToilJobID", "EnteredCurrentStatus"]
            with self.connectSchedd() as schedd:
                ads = schedd.xquery(constraint=constraint, projection=projection)

            # Only consider the Toil jobs that are part of this workflow
            batchJobIDs = [
                batchJobID for (batchJobID, task) in self.batchJobIDs.values()
            ]
            job_runtimes: dict[int, float] = {}
            for ad in ads:
                batchJobID = int(ad["ClusterId"])
                jobID = int(ad["ToilJobID"])
                if not (batchJobID in batchJobIDs):
                    continue

                # HTCondor stores the start of the runtime as a Unix timestamp
                runtime = time.time() - ad["EnteredCurrentStatus"]
                job_runtimes[jobID] = runtime

            return job_runtimes

        def killJob(self, jobID: int) -> None:
            batchJobID = self.batchJobIDs[jobID][0]
            logger.debug(f"Killing HTCondor job {batchJobID}")

            # Set the job to be killed when its exit status is checked
            with self.connectSchedd() as schedd:
                job_spec = f"(ClusterId == {batchJobID})"
                schedd.edit(job_spec, "ToilJobKilled", "True")

        def getJobExitCode(self, batchJobID: str) -> int | None:
            logger.debug(f"Getting exit code for HTCondor job {batchJobID}")

            status = {
                1: "Idle",
                2: "Running",
                3: "Removed",
                4: "Completed",
                5: "Held",
                6: "Transferring Output",
                7: "Suspended",
            }

            constraint = f"(ClusterId == {batchJobID})"
            projection = [
                "JobStatus",
                "ToilJobKilled",
                "ExitCode",
                "HoldReason",
                "HoldReasonSubCode",
            ]

            with self.connectSchedd() as schedd:
                ads = schedd.xquery(constraint=constraint, projection=projection)

                # Make sure a ClassAd was returned
                try:
                    ad = next(ads)
                except StopIteration:
                    logger.error(
                        f"No HTCondor ads returned using constraint: {constraint}"
                    )
                    raise

                # Make sure only one ClassAd was returned
                try:
                    next(ads)
                except StopIteration:
                    pass
                else:
                    logger.warning(
                        f"Multiple HTCondor ads returned using constraint: {constraint}"
                    )

                if ad["ToilJobKilled"]:
                    logger.debug(f"HTCondor job {batchJobID} was killed by Toil")

                    # Remove the job from the Schedd and return 1
                    job_spec = f"ClusterId == {batchJobID}"
                    schedd.act(htcondor.JobAction.Remove, job_spec)
                    return 1

                elif status[ad["JobStatus"]] == "Completed":
                    logger.debug(
                        "HTCondor job {} completed with exit code {}".format(
                            batchJobID, ad["ExitCode"]
                        )
                    )

                    # Remove the job from the Schedd and return its exit code
                    job_spec = f"ClusterId == {batchJobID}"
                    schedd.act(htcondor.JobAction.Remove, job_spec)
                    return int(ad["ExitCode"])

                elif status[ad["JobStatus"]] == "Held":
                    logger.error(
                        "HTCondor job {} was held: '{} (sub code {})'".format(
                            batchJobID, ad["HoldReason"], ad["HoldReasonSubCode"]
                        )
                    )

                    # Remove the job from the Schedd and return 1
                    job_spec = f"ClusterId == {batchJobID}"
                    schedd.act(htcondor.JobAction.Remove, job_spec)
                    return 1

                else:  # Job still running or idle or doing something else
                    logger.debug(
                        "HTCondor job {} has not completed (Status: {})".format(
                            batchJobID, status[ad["JobStatus"]]
                        )
                    )
                    return None

        """
        Implementation-specific helper methods
        """

        @contextmanager
        def connectSchedd(self) -> Iterator[htcondor.Schedd]:
            """
            Connect to HTCondor Schedd and yield a Schedd object.

            You can only use it inside the context.
            Handles locking to make sure that only one thread is trying to do this at a time.
            """

            # Find the scheduler
            schedd_ad = self._get_schedd_address()

            with schedd_lock:
                schedd = self._open_schedd_connection(schedd_ad)

                # Ping the Schedd to make sure it's there and responding
                try:
                    self._ping_scheduler(schedd)
                except RuntimeError:
                    logger.error("Could not connect to HTCondor Schedd")
                    raise

                yield schedd

        @retry(errors=[RuntimeError])
        def _ping_scheduler(self, schedd: htcondor.Schedd) -> None:
            """
            Ping the scheduler, or fail if it persistently cannot be contacted.
            """
            schedd.xquery(limit=0)

        @retry(errors=[htcondor.HTCondorIOError])
        def _get_schedd_address(self) -> "classad.ClassAd | None":
            """
            Get the HTCondor scheduler to connect to, or None for the local machine.

            Retries if necessary.
            """
            # TODO: Memoize? Or is the collector meant to field every request?

            condor_host = os.getenv("TOIL_HTCONDOR_COLLECTOR")
            schedd_name = os.getenv("TOIL_HTCONDOR_SCHEDD")

            # Get the scheduler's address, if not local
            schedd_ad: "classad.ClassAd | None" = None

            # If TOIL_HTCONDOR_ variables are set, use them to find the Schedd
            if condor_host and schedd_name:
                logger.debug(
                    "Connecting to HTCondor Schedd {} using Collector at {}".format(
                        schedd_name, condor_host
                    )
                )
                try:
                    schedd_ad = htcondor.Collector(condor_host).locate(
                        htcondor.DaemonTypes.Schedd, schedd_name
                    )
                except OSError:
                    logger.error(
                        f"Could not connect to HTCondor Collector at {condor_host}"
                    )
                    raise
                except ValueError:
                    logger.error(
                        f"Could not find HTCondor Schedd with name {schedd_name}"
                    )
                    raise
            else:
                # Otherwise assume the Schedd is on the local machine
                logger.debug("Connecting to HTCondor Schedd on local machine")

            return schedd_ad

        @retry(errors=[htcondor.HTCondorIOError])
        def _open_schedd_connection(
            self, schedd_ad: "classad.ClassAd | None" = None
        ) -> htcondor.Schedd:
            """
            Open a connection to the htcondor schedd.

            Assumes that the necessary lock is already held. Retries if needed.

            :param schedd_ad: Where the scheduler is. If not set, use the local machine.
            """

            # TODO: Don't hold the lock while retrying?

            if schedd_ad is not None:
                return htcondor.Schedd(schedd_ad)
            else:
                return htcondor.Schedd()

        def duplicate_quotes(self, value: str) -> str:
            """
            Escape a string by doubling up all single and double quotes.

            This is used for arguments we pass to htcondor that need to be
            inside both double and single quote enclosures.
            """
            return value.replace("'", "''").replace('"', '""')

        def getEnvString(self, overrides: dict[str, str]) -> str:
            """
            Build an environment string that a HTCondor Submit object can use.

            For examples of valid strings, see:
            http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html#man-condor-submit-environment
            """

            env_items = []
            if self.boss.environment or overrides:
                combined = dict(self.boss.environment)
                combined.update(overrides)
                for key, value in combined.items():

                    # Each variable should be in the form of <key>='<value>'
                    env_string = key + "="

                    # The entire value should be encapsulated in single quotes
                    # Quote marks (single or double) that are part of the value should be duplicated
                    env_string += "'" + self.duplicate_quotes(value) + "'"

                    env_items.append(env_string)

            # The entire string should be encapsulated in double quotes
            # Each variable should be separated by a single space
            return '"' + " ".join(env_items) + '"'

