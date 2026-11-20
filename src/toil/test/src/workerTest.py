# Copyright (C) 2015-2021 Regents of the University of California
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

import os
from unittest.mock import patch

from toil.batchSystems.abstractBatchSystem import JobEnvironmentVariables
from toil.common import Config
from toil.job import CheckpointJobDescription, JobDescription
from toil.jobStores.fileJobStore import FileJobStore
from toil.test import ToilTest
from toil.worker import apply_leader_environment, nextChainable


class WorkerTests(ToilTest):
    """Test miscellaneous units of the worker."""

    def setUp(self):
        super().setUp()
        path = self._getTestJobStorePath()
        self.jobStore = FileJobStore(path)
        self.config = Config()
        self.config.jobStore = "file:%s" % path
        self.jobStore.initialize(self.config)
        self.jobNumber = 0

    def testNextChainable(self):
        """Make sure chainable/non-chainable jobs are identified correctly."""

        def createTestJobDesc(
            memory,
            cores,
            disk,
            preemptible: bool = True,
            checkpoint: bool = False,
            local: bool | None = None,
        ):
            """
            Create a JobDescription with no command (representing a Job that
            has already run) and return the JobDescription.
            """
            name = "job%d" % self.jobNumber
            self.jobNumber += 1

            descClass = CheckpointJobDescription if checkpoint else JobDescription
            jobDesc = descClass(
                requirements={
                    "memory": memory,
                    "cores": cores,
                    "disk": disk,
                    "preemptible": preemptible,
                },
                jobName=name,
                local=local,
            )

            # Assign an ID
            self.jobStore.assign_job_id(jobDesc)

            # Save and return the JobDescription
            return self.jobStore.create_job(jobDesc)

        for successorType in ["addChild", "addFollowOn"]:
            # Try with the branch point at both child and follow-on stages

            # Identical non-checkpoint jobs should be chainable.
            jobDesc1 = createTestJobDesc(1, 2, 3)
            jobDesc2 = createTestJobDesc(1, 2, 3)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            chainable = nextChainable(jobDesc1, self.jobStore, self.config)
            self.assertNotEqual(chainable, None)
            self.assertEqual(chainable.jobStoreID, jobDesc2.jobStoreID)

            # Identical checkpoint jobs should not be chainable.
            jobDesc1 = createTestJobDesc(1, 2, 3, checkpoint=True)
            jobDesc2 = createTestJobDesc(1, 2, 3, checkpoint=True)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # Changing checkpoint from false to true should make it not chainable.
            jobDesc1 = createTestJobDesc(1, 2, 3, checkpoint=False)
            jobDesc2 = createTestJobDesc(1, 2, 3, checkpoint=True)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # If there is no child we should get nothing to chain.
            jobDesc1 = createTestJobDesc(1, 2, 3)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # If there are 2 or more children we should get nothing to chain.
            jobDesc1 = createTestJobDesc(1, 2, 3)
            jobDesc2 = createTestJobDesc(1, 2, 3)
            jobDesc3 = createTestJobDesc(1, 2, 3)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            getattr(jobDesc1, successorType)(jobDesc3.jobStoreID)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # If there is an increase in resource requirements we should get nothing to chain.
            base_reqs = {
                "memory": 1,
                "cores": 2,
                "disk": 3,
                "preemptible": True,
                "checkpoint": False,
            }
            for increased_attribute in ("memory", "cores", "disk"):
                reqs = dict(base_reqs)
                jobDesc1 = createTestJobDesc(**reqs)
                reqs[increased_attribute] += 1
                jobDesc2 = createTestJobDesc(**reqs)
                getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
                self.assertEqual(
                    nextChainable(jobDesc1, self.jobStore, self.config), None
                )

            # A change in preemptability from True to False should be disallowed.
            jobDesc1 = createTestJobDesc(1, 2, 3, preemptible=True)
            jobDesc2 = createTestJobDesc(1, 2, 3, preemptible=False)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # A change in local-ness from True to False should be disallowed.
            jobDesc1 = createTestJobDesc(1, 2, 3, local=True)
            jobDesc2 = createTestJobDesc(1, 2, 3, local=False)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            self.assertEqual(nextChainable(jobDesc1, self.jobStore, self.config), None)

            # A change in local-ness from False to True should be allowed,
            # since running locally is an optional optimization.
            jobDesc1 = createTestJobDesc(1, 2, 3, local=False)
            jobDesc2 = createTestJobDesc(1, 2, 3, local=True)
            getattr(jobDesc1, successorType)(jobDesc2.jobStoreID)
            chainable = nextChainable(jobDesc1, self.jobStore, self.config)
            self.assertNotEqual(chainable, None)
            self.assertEqual(chainable.jobStoreID, jobDesc2.jobStoreID)


class ApplyLeaderEnvironmentTests(ToilTest):
    """Test which of the leader's environment variables a worker takes."""

    def test_takes_leader_value_for_other_variables(self) -> None:
        """A variable the worker doesn't keep gets the leader's value."""
        with patch.dict(os.environ, {"MAGIC_ENV_VAR_123": "worker"}):
            apply_leader_environment({"MAGIC_ENV_VAR_123": "leader"})
            self.assertEqual(os.environ["MAGIC_ENV_VAR_123"], "leader")

    def test_keeps_own_batch_system_job_variables(self) -> None:
        """
        Variables a batch system sets for each job keep the worker's value,
        whether matched by prefix or by name.
        """
        names = ["SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID", "_CONDOR_SLOT", "JOB_ID"]
        with patch.dict(os.environ, {name: "worker" for name in names}):
            apply_leader_environment({name: "leader" for name in names})
            for name in names:
                self.assertEqual(os.environ[name], "worker", name)

    def test_does_not_add_batch_system_job_variables(self) -> None:
        """A batch system job variable the worker lacks stays unset."""
        with patch.dict(os.environ):
            os.environ.pop("SLURM_JOB_ID", None)
            apply_leader_environment({"SLURM_JOB_ID": "leader"})
            self.assertNotIn("SLURM_JOB_ID", os.environ)

    def test_merges_path(self) -> None:
        """PATH has the leader's entries, then the worker's."""
        with patch.dict(os.environ, {"PATH": "/worker/bin"}):
            apply_leader_environment({"PATH": "/leader/bin"})
            self.assertEqual(os.environ["PATH"], "/leader/bin:/worker/bin")

    def test_keeps_variables_the_batch_system_declares(self) -> None:
        """
        Variables the batch system declares, by name or prefix, keep the
        worker's value.
        """
        declared = JobEnvironmentVariables(
            names=frozenset({"NODE_RANK"}), prefixes=("PLUGIN_",)
        )
        names = ["NODE_RANK", "PLUGIN_NODELIST"]
        with patch.dict(os.environ, {name: "worker" for name in names}):
            apply_leader_environment({name: "leader" for name in names}, declared)
            for name in names:
                self.assertEqual(os.environ[name], "worker", name)
