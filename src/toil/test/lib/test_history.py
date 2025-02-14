# Copyright (C) 2015-2025 Regents of the University of California
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
import logging
import pytest
import time
from toil.test import ToilTest

from toil.lib.history import HistoryManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class HistoryTest(ToilTest):
    """
    Tests for Toil history tracking.

    Each test gets its own history database.
    """

    def setUp(self) -> None:
        super().setUp()
        
        # Apply a temp dir override to history tracking
        temp_dir = self._createTempDir()
        HistoryManager.database_path_override = os.path.join(temp_dir, "test-db.sqlite")

        # Flag on job history tracking
        self.original_flag = HistoryManager.JOB_HISTORY_ENABLED
        HistoryManager.JOB_HISTORY_ENABLED = True
        

    def tearDown(self) -> None:
        # Remove the temp dir override from history tracking
        HistoryManager.database_path_override = None

        # Restore job history tracking flag
        HistoryManager.JOB_HISTORY_ENABLED = self.original_flag

        super().tearDown()

    def make_fake_workflow(self, workflow_id: str) -> None:
        # Make a fake workflow
        workflow_jobstore_spec = "file:/tmp/tree"
        HistoryManager.record_workflow_creation(workflow_id, workflow_jobstore_spec)
        workflow_name = "SuperCoolWF"
        workflow_trs_spec = "#wf:v1"
        HistoryManager.record_workflow_metadata(workflow_id, workflow_name, workflow_trs_spec)
        
        # Give it a job
        workflow_attempt_number = 1
        job_name = "DoThing"
        succeeded = True
        start_time = time.time()
        runtime = 0.1
        HistoryManager.record_job_attempt( 
            workflow_id,
            workflow_attempt_number,
            job_name,
            succeeded,
            start_time,
            runtime,
        )

        # Give it a workflow attempt with the same details.
        HistoryManager.record_workflow_attempt(
            workflow_id,
            workflow_attempt_number,
            succeeded,
            start_time,
            runtime,
        )

    def test_history_submittable_detection(self) -> None:
        """
        Make sure that a submittable workflow shows up as such before
        submission and doesn't afterward.
        """
        workflow_id = "123"
        self.make_fake_workflow(workflow_id)
        workflow_attempt_number = 1

        # Make sure we have data
        self.assertEqual(HistoryManager.count_workflows(), 1)
        self.assertEqual(HistoryManager.count_workflow_attempts(), 1)
        self.assertEqual(HistoryManager.count_job_attempts(), 1)

        # Make sure we see it as submittable
        submittable_workflow_attempts = HistoryManager.get_submittable_workflow_attempts()
        self.assertEqual(len(submittable_workflow_attempts), 1)

        # Make sure we see its jobs as submittable
        with_submittable_job_attempts = HistoryManager.get_workflow_attempts_with_submittable_job_attempts()
        self.assertEqual(len(with_submittable_job_attempts), 1)

        # Make sure we actually see the job
        submittable_job_attempts = HistoryManager.get_unsubmitted_job_attempts(workflow_id, workflow_attempt_number)
        self.assertEqual(len(submittable_job_attempts), 1)

        # Pretend we submitted them.
        HistoryManager.mark_job_attempts_submitted([j.id for j in submittable_job_attempts])
        HistoryManager.mark_workflow_attempt_submitted(workflow_id, workflow_attempt_number)

        # Make sure they are no longer matching
        self.assertEqual(len(HistoryManager.get_submittable_workflow_attempts()), 0)
        self.assertEqual(len(HistoryManager.get_workflow_attempts_with_submittable_job_attempts()), 0)
        self.assertEqual(len(HistoryManager.get_unsubmitted_job_attempts(workflow_id, workflow_attempt_number)), 0)

        # Make sure we still have data
        self.assertEqual(HistoryManager.count_workflows(), 1)
        self.assertEqual(HistoryManager.count_workflow_attempts(), 1)
        self.assertEqual(HistoryManager.count_job_attempts(), 1)

    def test_history_deletion(self) -> None:
        workflow_id = "123"
        self.make_fake_workflow(workflow_id)
        workflow_attempt_number = 1
        
        # Make sure we can see the workflow for deletion by age but not by done-ness
        self.assertEqual(len(HistoryManager.get_oldest_workflow_ids()), 1)
        self.assertEqual(len(HistoryManager.get_fully_submitted_workflow_ids()), 0)

        # Pretend we submitted the workflow.
        HistoryManager.mark_job_attempts_submitted([j.id for j in HistoryManager.get_unsubmitted_job_attempts(workflow_id, workflow_attempt_number)])
        HistoryManager.mark_workflow_attempt_submitted(workflow_id, workflow_attempt_number)

        # Make sure we can see the workflow for deletion by done-ness
        self.assertEqual(len(HistoryManager.get_fully_submitted_workflow_ids()), 1)

        # Add a new workflow
        other_workflow_id = "456"
        self.make_fake_workflow(other_workflow_id)

        # Make sure we can see the both for deletion by age but only one by done-ness
        self.assertEqual(len(HistoryManager.get_oldest_workflow_ids()), 2)
        self.assertEqual(len(HistoryManager.get_fully_submitted_workflow_ids()), 1)

        # Make sure the older workflow is first.
        self.assertEqual(HistoryManager.get_oldest_workflow_ids(), [workflow_id, other_workflow_id])

        # Delete the new workflow
        HistoryManager.delete_workflow(other_workflow_id)

        # Make sure we can see the old one
        self.assertEqual(HistoryManager.get_oldest_workflow_ids(), [workflow_id])
        self.assertEqual(HistoryManager.get_fully_submitted_workflow_ids(), [workflow_id])

        # Delete the old workflow
        HistoryManager.delete_workflow(workflow_id)

        # Make sure we have no data
        self.assertEqual(HistoryManager.count_workflows(), 0)
        self.assertEqual(HistoryManager.count_workflow_attempts(), 0)
        self.assertEqual(HistoryManager.count_job_attempts(), 0)


    def test_history_size_limit(self) -> None:
        """
        Make sure the database size can be controlled.
        """

        for workflow_id in ("WorkflowThatTakesUpSomeSpace,ActuallyMoreThanTheLaterOnesTake" + str(i) for i in range(10)):
            self.make_fake_workflow(workflow_id)

        # We should see the workflows.
        self.assertEqual(HistoryManager.count_workflows(), 10)
        # And they take up space.
        small_size = HistoryManager.get_database_byte_size()
        self.assertGreater(small_size, 0)

        # Add a bunch more
        for workflow_id in ("WorkflowThatTakesUpSpace" + str(i) for i in range(50)):
            self.make_fake_workflow(workflow_id)

        # We should see that this is now a much larger database
        large_size = HistoryManager.get_database_byte_size()
        logger.info("Increased database size from %s to %s", small_size, large_size)
        self.assertGreater(large_size, small_size)
        
        # We should be able to shrink it back down
        HistoryManager.enforce_byte_size_limit(small_size)
        
        reduced_size = HistoryManager.get_database_byte_size()
        logger.info("Decreased database size from %s to %s", large_size, reduced_size)
        # The database should be small enough
        self.assertLessEqual(reduced_size, small_size)
        # There should still be some workflow attempts left in the smaller database (though probably not the first ones)
        remaining_workflows = HistoryManager.count_workflows()
        logger.info("Still have %s workflows", remaining_workflows)
        self.assertGreater(remaining_workflows, 0)
        




       



        
