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
Contains functions for integrating Toil with GA4GH Tool Registry Service
servers, for fetching workflows.
"""

import datetime
import hashlib
import logging
import os
import shutil
import sys
import tempfile
import uuid
import zipfile
from typing import Any, Literal, Optional, Union, TypedDict, cast

from urllib.parse import urlparse, unquote, quote
import requests

from toil.lib.retry import retry
from toil.lib.web import session
from toil.version import baseVersion

logger = logging.getLogger(__name__)

# TODO: This is a https://schema.org/CompletedActionStatus
# But what are the possible values?
ExecutionStatus = Union[Literal["SUCCESSFUL"], Literal["FAILED"]]

class RunExecution(TypedDict):
    """
    Dockstore metrics data for a workflow run.
    """
    executionId: str
    """
    Executor-generated unique execution ID.
    """
    dateExecuted: str
    """
    ISO 8601 UTC timestamp whr when the execution happend.

    TODO: Is this start or end?
    """

    executionStatus: ExecutionStatus
    """
    Did the execution work?
    """

    # TODO: additionalProperties


def send_workflow_metrics_to_dockstore(trs_workflow_id: str, trs_version: str, execution_id: uuid.UUID, succeeded: bool) -> None:
    """
    Send the status of a workflow execution to Dockstore.
    
    Assumes the workflow was executed now.
    """

    # Pack up into a RunExecution
    execution = RunExecution(
        executionId=str(execution_id),
        dateExecuted=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        executionStatus="SUCCESSFUL" if succeeded else "FAILED"
    )

    # Aggregate into a submission
    to_post = {
        "runExecutions": [execution],
        "taskExecutions": [],
        "validationExecutions": []
    }

    # Set the submission query string metadata
    submission_params = {
        "platform": "OTHER", # TODO: Should this be TOIL?
        "description": "Workflow status from Toil"
    }
    
    # TODO: Point at QA
    endpoint_url = f"https://dockstore.org/api/ga4gh/v2/extended/{quote(trs_workflow_id, safe='')}/versions/{quote(trs_version, safe='')}/executions"

    session.post(endpoint_url, params=submission_params, json=to_post)


