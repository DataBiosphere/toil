# Copyright (C) 2024 Regents of the University of California
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

"""
Contains tools for tracking history.
"""

import logging
import os
from typing import Any, Literal, Optional, Union, TypedDict, cast

from toil.lib.io import get_toil_home

logger = logging.getLogger(__name__)

class HistoryManager:
    """
    Class responsible for managing the history of Toil runs.
    """

    @staticmethod
    def record_workflow_creation(workflow_id: str, job_store_spec: str) -> None:
        """
        Record that a workflow is being run.

        Takes the Toil config's workflow ID and the location of the job store.

        Should only be called on the *first* attempt on a job store, not on a
        restart.
        """
        
        logger.info("Recording workflow creation of %s in %s", workflow_id, job_store_spec)
        pass

    @staticmethod
    def record_workflow_metadata(workflow_id: str, workflow_name: str) -> None:
        """
        Associate a name (possibly a TRS ID and version string) with a workflow run.
        """

        logger.info("Workflow %s is a run of %s", workflow_id, workflow_name)

        pass

    @staticmethod
    def record_job_attempt(workflow_id: str, workflow_attempt_number: int, job_name: str, succeeed: bool, start_time: float, runtime: float) -> None:
        """
        Record that a job ran in a workflow.

        Thread safe.
        """

        logger.info("Workflow %s ran job %s", workflow_id, job_name)

        pass

    @staticmethod
    def record_workflow_attempt(workflow_id: str, workflow_attempt_number: int, succeeded: bool, start_time: float, runtime: float) -> None:
        """
        Record a workflow attempt (start or restart) having finished or failed.
        """

        logger.info("Workflow %s stopped. Success: %s", workflow_id, succeeded)

        pass

   
