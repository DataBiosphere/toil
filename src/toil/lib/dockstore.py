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
Contains functions for integrating Toil with UCSC Dockstore, for reporting metrics.

For basic TRS functionality for fetching workflows, see trs.py.
"""

import datetime
import logging
import os
import sys
import uuid
from typing import Any, Literal, Optional, Union, TypedDict, cast

from urllib.parse import urlparse, unquote, quote
import requests

from toil.lib.misc import unix_seconds_to_timestamp, seconds_to_duration
from toil.lib.trs import TRS_ROOT
from toil.lib.retry import retry
from toil.lib.web import session
from toil.version import baseVersion

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

logger = logging.getLogger(__name__)

# We assume TRS_ROOT is actually a Dockstore instance.

# How shoudl we authenticate our Dockstore requests?
DOCKSTORE_TOKEN = None if "TOIL_DOCKSTORE_TOKEN" not in os.environ else os.environ["TOIL_DOCKSTORE_TOKEN"]


# This is a https://schema.org/CompletedActionStatus
# The values here are from expanding the type info in the Docksotre docs at
# <https://dockstore.org/api/static/swagger-ui/index.html#/extendedGA4GH/executionMetricsPost>
ExecutionStatus = Union[Literal["ALL"], Literal["SUCCESSFUL"], Literal["FAILED"], Literal["FAILED_SEMANTIC_INVALID"], Literal["FAILED_RUNTIME_INVALID"], Literal["ABORTED"]]

class Cost(TypedDict):
    """
    Representation of the cost of running something.
    """

    value: float
    """
    Cost in US Dollars.
    """

class RunExecution(TypedDict):
    """
    Dockstore metrics data for a workflow or task run.
    """

    executionId: str
    """
    Executor-generated unique execution ID for this workflow or task.
    """

    # TODO: Is this start or end?
    dateExecuted: str
    """
    ISO 8601 UTC timestamp when the execution happend.
    """

    executionStatus: ExecutionStatus
    """
    Did the execution work?
    """

    executionTime: NotRequired[str]
    """
    Total time of the run in ISO 8601 duration format.
    """

    # TODO: Is this meant to be actual usage or amount provided?
    memoryRequirementsGB: NotRequired[float]
    """
    Memory required for the execution in gigabytes (not GiB).
    """

    cpuRequirements: NotRequired[int]
    """
    Number of CPUs required.
    """

    cost: NotRequired[Cost]
    """
    How much the execution cost to run.
    """

    # TODO: What if two cloud providers have the same region naming scheme?
    region: NotRequired[str]
    """
    The (cloud) region the workflow was executed in.
    """

    additionalProperties: NotRequired[dict[str, str]]
    """
    Any additional properties to send.

    Dockstore can take any JSON-able structured data, but we only use strings.
    """

class TaskExecutions(TypedDict):
    """
    Dockstore metrics data for all the tasks in a workflow.
    """

    # TODO: Should this match executionId for the whole workflow's RunExecution?
    executionId: str
    """
    Executor-generated unique execution ID.
    """

    # TODO: Is this start or end?
    dateExecuted: str
    """
    ISO 8601 UTC timestamp when the execution happend.
    """

    taskExecutions: list[RunExecution]
    """
    Individual executions of each task in the workflow.
    """

    additionalProperties: NotRequired[dict[str, str]]
    """
    Any additional properties to send.

    Dockstore can take any JSON-able structured data, but we only use strings.
    """

def send_metrics(trs_workflow_id: str, trs_version: str, execution_id: str, start_time: float, runtime: float, succeeded: bool) -> None:
    """
    Send the status of a workflow execution to Dockstore.

    :param execution_id: Unique ID for the workflow execution.
    :param start_time: Execution start time in seconds since the Unix epoch.
    :param rutime: Execution duration in seconds.
    :raises requests.HTTPError: if Dockstore does not accept the metrics.
    """

    # Pack up into a RunExecution
    execution = RunExecution(
        executionId=execution_id,
        dateExecuted=unix_seconds_to_timestamp(start_time),
        executionTime=seconds_to_duration(runtime),
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
        "platform": "OTHER",
        "description": "Workflow status from Toil"
    }

    # Set the headers. Even though user agent isn't in here, it still gets
    # sent.
    headers = {}
    if DOCKSTORE_TOKEN is not None:
        headers["Authorization"] = f"Bearer {DOCKSTORE_TOKEN}"

    # Note that Dockstore's metrics apparently need two levels of /api for some reason.
    endpoint_url = f"{TRS_ROOT}/api/api/ga4gh/v2/extended/{quote(trs_workflow_id, safe='')}/versions/{quote(trs_version, safe='')}/executions"

    logger.info("Sending workflow metrics to %s", endpoint_url)
    logger.debug("With data: %s", to_post)
    logger.debug("With headers: %s", headers)

    try:
        result = session.post(endpoint_url, params=submission_params, json=to_post, headers=headers)
        result.raise_for_status()
    except requests.HTTPError:
        logging.warning("Workflow metrics were not accepted by Dockstore")
        raise


