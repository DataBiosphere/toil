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

import logging
import math
import os
import re
import sys
from typing import Any, Literal, TypedDict, Union
from urllib.parse import quote

import requests

from toil.lib.misc import seconds_to_duration, unix_seconds_to_timestamp
from toil.lib.trs import TRS_ROOT
from toil.lib.web import web_session

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

logger = logging.getLogger(__name__)

# We assume TRS_ROOT is actually a Dockstore instance.

# This is a publish-able token for production Dockstore for Toil to use.
# This is NOT a secret value.
DEFAULT_DOCKSTORE_TOKEN = (
    "2bff46294daddef6df185452b04db6143ea8a59f52ee3c325d3e1df418511b7d"
)

# How should we authenticate our Dockstore requests?
DOCKSTORE_TOKEN = os.environ.get("TOIL_DOCKSTORE_TOKEN", DEFAULT_DOCKSTORE_TOKEN)

# What platform should we report metrics as?
DOCKSTORE_PLATFORM = "TOIL"


# This is a https://schema.org/CompletedActionStatus
# The values here are from expanding the type info in the Docksotre docs at
# <https://dockstore.org/api/static/swagger-ui/index.html#/extendedGA4GH/executionMetricsPost>
ExecutionStatus = Union[
    Literal["ALL"],
    Literal["SUCCESSFUL"],
    Literal["FAILED"],
    Literal["FAILED_SEMANTIC_INVALID"],
    Literal["FAILED_RUNTIME_INVALID"],
    Literal["ABORTED"],
]


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

    dateExecuted: str
    """
    ISO 8601 UTC timestamp when the execution happened.
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

    additionalProperties: NotRequired[dict[str, Any]]
    """
    Any additional properties to send.

    Dockstore can take any JSON-able structured data.
    """


class TaskExecutions(TypedDict):
    """
    Dockstore metrics data for all the tasks in a workflow.
    """

    # TODO: Right now we use different IDs for the workflow RunExecution and
    # for its corresponding collection of TaskExecutions, so there's no nice
    # way to find the one from the other.
    executionId: str
    """
    Executor-generated unique execution ID.
    """

    dateExecuted: str
    """
    ISO 8601 UTC timestamp when the execution happened.
    """

    taskExecutions: list[RunExecution]
    """
    Individual executions of each task in the workflow.
    """

    additionalProperties: NotRequired[dict[str, Any]]
    """
    Any additional properties to send.

    Dockstore can take any JSON-able structured data.
    """


def ensure_valid_id(execution_id: str) -> None:
    """
    Make sure the given execution ID is in Dockstore format and will be accepted by Dockstore.

    Must be alphanumeric (with internal underscores allowed) and <100
    characters long.

    :raises ValueError: if the ID is not in the right format
    """
    if len(execution_id) >= 100:
        raise ValueError("Execution ID too long")
    if len(execution_id) == 0:
        raise ValueError("Execution ID must not be empty")
    if execution_id[0] == "_" or execution_id[-1] == "_":
        raise ValueError("Execution ID must not start or end with an underscore")
    if not re.fullmatch("[a-zA-Z0-9_]+", execution_id):
        raise ValueError("Execution ID must be alphanumeric with internal underscores")


def pack_workflow_metrics(
    execution_id: str,
    start_time: float,
    runtime: float,
    succeeded: bool,
    job_store_type: str | None = None,
    batch_system: str | None = None,
    caching: bool | None = None,
    toil_version: str | None = None,
    python_version: str | None = None,
    platform_system: str | None = None,
    platform_machine: str | None = None,
) -> RunExecution:
    """
    Pack up per-workflow metrics into a format that can be submitted to Dockstore.

    :param execution_id: Unique ID for the workflow execution. Must be in
        Dockstore format.
    :param start_time: Execution start time in seconds since the Unix epoch.
    :param rutime: Execution duration in seconds.
    :param jobstore_type: Kind of job store used, like "file" or "aws".
    :param batch_system: Python class name implementing the batch system used.
    :param caching: Whether Toil filestore-level caching was used.
    :param toil_version: Version of Toil used (without any Git hash).
    :param python_version: Version of Python used.
    :param platform_system: Operating system type (like "Darwin" or "Linux").
    :param platform_machine: Machine architecture of the leader (like "AMD64").
    """

    # Enforce Dockstore's constraints
    ensure_valid_id(execution_id)

    # Pack up into a RunExecution
    result = RunExecution(
        executionId=execution_id,
        dateExecuted=unix_seconds_to_timestamp(start_time),
        executionTime=seconds_to_duration(runtime),
        executionStatus="SUCCESSFUL" if succeeded else "FAILED",
    )

    # TODO: Just use kwargs here?
    additional_properties: dict[str, Any] = {}

    if job_store_type is not None:
        additional_properties["jobStoreType"] = job_store_type

    if batch_system is not None:
        additional_properties["batchSystem"] = batch_system

    if caching is not None:
        additional_properties["caching"] = caching

    if toil_version is not None:
        additional_properties["toilVersion"] = toil_version

    if python_version is not None:
        additional_properties["pythonVersion"] = python_version

    if platform_system is not None:
        additional_properties["platformSystem"] = platform_system

    if platform_machine is not None:
        additional_properties["platformMachine"] = platform_machine

    if len(additional_properties) > 0:
        result["additionalProperties"] = additional_properties

    return result


def pack_single_task_metrics(
    execution_id: str,
    start_time: float,
    runtime: float,
    succeeded: bool,
    job_name: str | None = None,
    cores: float | None = None,
    cpu_seconds: float | None = None,
    memory_bytes: int | None = None,
    disk_bytes: int | None = None,
) -> RunExecution:
    """
    Pack up metrics for a single task execution in a format that can be used in a Dockstore submission.

    :param execution_id: Unique ID for the workflow execution. Must be in
        Dockstore format.
    :param start_time: Execution start time in seconds since the Unix epoch.
    :param rutime: Execution duration in seconds.
    :param succeeded: Whether the execution succeeded.
    :param job_name: Name of the job within the workflow.
    :param cores: CPU cores allocated to the job.
    :param cpu_seconds: CPU seconds consumed by the job.
    :param memory_bytes: Memory consumed by the job in bytes.
    :param disk_bytes: Disk space consumed by the job in bytes.
    """

    # TODO: Deduplicate with workflow code since the output type is the same.

    # Enforce Dockstore's constraints
    ensure_valid_id(execution_id)

    # Pack up into a RunExecution
    result = RunExecution(
        executionId=execution_id,
        dateExecuted=unix_seconds_to_timestamp(start_time),
        executionTime=seconds_to_duration(runtime),
        executionStatus="SUCCESSFUL" if succeeded else "FAILED",
    )

    if memory_bytes is not None:
        # Convert bytes to fractional gigabytes
        result["memoryRequirementsGB"] = memory_bytes / 1_000_000_000

    if cores is not None:
        # Convert possibly fractional cores to an integer for Dockstore
        result["cpuRequirements"] = int(math.ceil(cores))

    # TODO: Just use kwargs here?
    additional_properties: dict[str, Any] = {}

    if job_name is not None:
        # Convert to Doskstore-style camelCase property keys
        additional_properties["jobName"] = job_name

    if disk_bytes is not None:
        # Convert to a Dockstore-style fractional disk gigabytes
        additional_properties["diskRequirementsGB"] = disk_bytes / 1_000_000_000

    if cpu_seconds is not None:
        # Use a Dockstore-ier name here too
        additional_properties["cpuRequirementsCoreSeconds"] = cpu_seconds

    if len(additional_properties) > 0:
        result["additionalProperties"] = additional_properties

    return result


def pack_workflow_task_set_metrics(
    execution_id: str, start_time: float, tasks: list[RunExecution]
) -> TaskExecutions:
    """
    Pack up metrics for all the tasks in a workflow execution into a format that can be submitted to Dockstore.

    :param execution_id: Unique ID for the workflow execution. Must be in
        Dockstore format.
    :param start_time: Execution start time for the overall workflow execution
        in seconds since the Unix epoch.
    :param tasks: Packed tasks from pack_single_task_metrics()
    """

    # Enforce Dockstore's constraints
    ensure_valid_id(execution_id)

    return TaskExecutions(
        executionId=execution_id,
        dateExecuted=unix_seconds_to_timestamp(start_time),
        taskExecutions=tasks,
    )


def send_metrics(
    trs_workflow_id: str,
    trs_version: str,
    workflow_runs: list[RunExecution],
    workflow_task_sets: list[TaskExecutions],
) -> None:
    """
    Send packed workflow and/or task metrics to Dockstore.

    :param workflow_runs: list of packed metrics objects for each workflow.

    :param workflow_task_sets: list of packed metrics objects for the tasks in
        each workflow. Each workflow should have one entry containing all its
        tasks. Does not have to be the same order/set of workflows as
        workflow_runs.

    :raises requests.HTTPError: if Dockstore does not accept the metrics.
    """

    # Aggregate into a submission
    to_post = {
        "runExecutions": workflow_runs,
        "taskExecutions": workflow_task_sets,
        "validationExecutions": [],
    }

    # Set the submission query string metadata
    submission_params = {
        "platform": DOCKSTORE_PLATFORM,
        "description": "Workflow status from Toil",
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
        result = web_session.post(
            endpoint_url, params=submission_params, json=to_post, headers=headers
        )
        result.raise_for_status()
        logger.debug(
            "Workflow metrics were accepted by Dockstore. Dockstore response code: %s",
            result.status_code,
        )
    except requests.HTTPError as e:
        logger.warning(
            "Workflow metrics were not accepted by Dockstore. Dockstore complained: %s",
            e.response.text,
        )
        raise


def get_metrics_url(trs_workflow_id: str, trs_version: str, execution_id: str) -> str:
    """
    Get the URL where a workflow metrics object (for a workflow, or for a set of tasks) can be fetched back from.
    """

    return f"{TRS_ROOT}/api/api/ga4gh/v2/extended/{quote(trs_workflow_id, safe='')}/versions/{quote(trs_version, safe='')}/execution?platform={DOCKSTORE_PLATFORM}&executionId={quote(execution_id, safe='')}"
