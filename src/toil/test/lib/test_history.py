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

from collections.abc import Generator
from pathlib import Path
import logging
import time

from toil.lib.history import HistoryManager

import pytest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class TestHistory:
    """
    Tests for Toil history tracking.

    Each test gets its own history database.
    """

    @pytest.fixture(autouse=True, scope="function")
    def private_history_manager(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Generator[None]:
        try:
            with monkeypatch.context() as m:
                m.setattr(
                    HistoryManager,
                    "database_path_override",
                    str(tmp_path / "test-db.sqlite"),
                )
                m.setattr(HistoryManager, "enabled", lambda: True)
                m.setattr(HistoryManager, "enabled_job", lambda: True)
                yield
        finally:
            pass  # no cleanup needed

    def make_fake_workflow(self, workflow_id: str) -> None:
        # Make a fake workflow
        workflow_jobstore_spec = "file:/tmp/tree"
        HistoryManager.record_workflow_creation(workflow_id, workflow_jobstore_spec)
        workflow_name = "SuperCoolWF"
        workflow_trs_spec = "#wf:v1"
        HistoryManager.record_workflow_metadata(
            workflow_id, workflow_name, workflow_trs_spec
        )

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
        assert HistoryManager.count_workflows() == 1
        assert HistoryManager.count_workflow_attempts() == 1
        assert HistoryManager.count_job_attempts() == 1

        # Make sure we see it as submittable
        submittable_workflow_attempts = (
            HistoryManager.get_submittable_workflow_attempts()
        )
        assert len(submittable_workflow_attempts) == 1

        # Make sure we see its jobs as submittable
        with_submittable_job_attempts = (
            HistoryManager.get_workflow_attempts_with_submittable_job_attempts()
        )
        assert len(with_submittable_job_attempts) == 1

        # Make sure we actually see the job
        submittable_job_attempts = HistoryManager.get_unsubmitted_job_attempts(
            workflow_id, workflow_attempt_number
        )
        assert len(submittable_job_attempts) == 1

        # Pretend we submitted them.
        HistoryManager.mark_job_attempts_submitted(
            [j.id for j in submittable_job_attempts]
        )
        HistoryManager.mark_workflow_attempt_submitted(
            workflow_id, workflow_attempt_number
        )

        # Make sure they are no longer matching
        assert len(HistoryManager.get_submittable_workflow_attempts()) == 0
        assert (
            len(HistoryManager.get_workflow_attempts_with_submittable_job_attempts())
            == 0
        )
        assert (
            len(
                HistoryManager.get_unsubmitted_job_attempts(
                    workflow_id, workflow_attempt_number
                )
            )
            == 0
        )

        # Make sure we still have data
        assert HistoryManager.count_workflows() == 1
        assert HistoryManager.count_workflow_attempts() == 1
        assert HistoryManager.count_job_attempts() == 1

    def test_history_deletion(self) -> None:
        workflow_id = "123"
        self.make_fake_workflow(workflow_id)
        workflow_attempt_number = 1

        # Make sure we can see the workflow for deletion by age but not by done-ness
        assert len(HistoryManager.get_oldest_workflow_ids()) == 1
        assert len(HistoryManager.get_fully_submitted_workflow_ids()) == 0

        # Pretend we submitted the workflow.
        HistoryManager.mark_job_attempts_submitted(
            [
                j.id
                for j in HistoryManager.get_unsubmitted_job_attempts(
                    workflow_id, workflow_attempt_number
                )
            ]
        )
        HistoryManager.mark_workflow_attempt_submitted(
            workflow_id, workflow_attempt_number
        )

        # Make sure we can see the workflow for deletion by done-ness
        assert len(HistoryManager.get_fully_submitted_workflow_ids()) == 1

        # Add a new workflow
        other_workflow_id = "456"
        self.make_fake_workflow(other_workflow_id)

        # Make sure we can see the both for deletion by age but only one by done-ness
        assert len(HistoryManager.get_oldest_workflow_ids()) == 2
        assert len(HistoryManager.get_fully_submitted_workflow_ids()) == 1

        # Make sure the older workflow is first.
        assert HistoryManager.get_oldest_workflow_ids() == [
            workflow_id,
            other_workflow_id,
        ]

        # Delete the new workflow
        HistoryManager.delete_workflow(other_workflow_id)

        # Make sure we can see the old one
        assert HistoryManager.get_oldest_workflow_ids() == [workflow_id]
        assert HistoryManager.get_fully_submitted_workflow_ids() == [workflow_id]

        # Delete the old workflow
        HistoryManager.delete_workflow(workflow_id)

        # Make sure we have no data
        assert HistoryManager.count_workflows() == 0
        assert HistoryManager.count_workflow_attempts() == 0
        assert HistoryManager.count_job_attempts() == 0

    def test_history_size_limit(self) -> None:
        """
        Make sure the database size can be controlled.
        """

        for workflow_id in (
            "WorkflowThatTakesUpSomeSpace,ActuallyMoreThanTheLaterOnesTake" + str(i)
            for i in range(10)
        ):
            self.make_fake_workflow(workflow_id)

        # We should see the workflows.
        assert HistoryManager.count_workflows() == 10
        # And they take up space.
        small_size = HistoryManager.get_database_byte_size()
        assert small_size > 0

        # Add a bunch more
        for workflow_id in ("WorkflowThatTakesUpSpace" + str(i) for i in range(50)):
            self.make_fake_workflow(workflow_id)

        # We should see that this is now a much larger database
        large_size = HistoryManager.get_database_byte_size()
        logger.info("Increased database size from %s to %s", small_size, large_size)
        large_size > small_size

        # We should be able to shrink it back down
        HistoryManager.enforce_byte_size_limit(small_size)

        reduced_size = HistoryManager.get_database_byte_size()
        logger.info("Decreased database size from %s to %s", large_size, reduced_size)
        # The database should be small enough
        reduced_size <= small_size
        # There should still be some workflow attempts left in the smaller database (though probably not the first ones)
        remaining_workflows = HistoryManager.count_workflows()
        logger.info("Still have %s workflows", remaining_workflows)
        assert remaining_workflows > 0
