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
    def record_workflow_start(workflow_id: str, job_store_spec: str, workflow_spec: str, trs_spec: Optional[str]) -> None:
        """
        Record that a workflow is being run.

        Takes the Toil config's workflow ID, the location of the job store,
        some kind of name for the workflow (like a filename or Python script
        name), and a TRS ID:version string, if available.

        Should only be called on the *first* attempt on a job store, not on a
        restart.
        """

        pass

    @staticmethod
    def record_job_attempt(workflow_id: str, job_name: str, succeeed: bool, start_time: float, runtime: float) -> None:
        """
        Record that a job ran in a workflow.
        """

        pass

    @staticmethod
    def record_workflow_attempt(workflow_id: str, attempt_number: int, succeeded: bool, start_time: float, runtime: float) -> None:
        """
        Record a workflow attempt (start or restart) having finished or failed.
        """

        pass

   
