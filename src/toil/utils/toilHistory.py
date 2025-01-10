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
"""View workflow run history."""
import logging
import os
import sys
from typing import Any, Optional

from toil.common import parser_with_common_options
from toil.statsAndLogging import set_logging_from_options

from toil.lib.dockstore import send_metrics, get_metrics_url, pack_workflow_metrics, pack_single_task_metrics, pack_workflow_task_set_metrics
from toil.lib.history import HistoryManager, WorkflowAttemptSummary, JobAttemptSummary
from toil.lib.misc import unix_seconds_to_local_time
from toil.lib.trs import parse_trs_spec


logger = logging.getLogger(__name__)

# These aren't methods on the types because I don't want to get too many
# Dockstore ideas into the database abstraction types.

def workflow_execution_id(workflow_attempt: WorkflowAttemptSummary) -> str:
    """
    Get the execution ID for a workflow attempt.

    Result will follow Dockstore's rules.

    Deterministic.
    """

    return workflow_attempt.workflow_id.replace("-", "_") + "_attempt_" + str(workflow_attempt.attempt_number)

def workflow_task_set_execution_id(workflow_attempt: WorkflowAttemptSummary) -> str:
    """
    Get the execution ID for a workflow attempt's collection of tasks.

    Result will follow Dockstore's rules.

    Deterministic.
    """

    return workflow_execution_id(workflow_attempt) + "_tasks"

def job_execution_id(job_attempt: JobAttemptSummary) -> str:
    """
    Get the execution ID for a job attempt.

    Result will follow Dockstore's rules.

    Deterministic.
    """

    return job_attempt.id.replace("-", "_")

def main() -> None:
    """Manage stored workflow run history."""
    parser = parser_with_common_options(jobstore_option=False, prog="toil history")

    parser.add_argument("--submit", action="store_true", help="Upload any unsubmitted metrics to Dockstore.")

    options = parser.parse_args()
    set_logging_from_options(options)
    
    print("History:")
    for summary in HistoryManager.summarize_workflows():
        status = "✅" if summary.succeeded else f"❌x{summary.total_attempts}"
        start_time = unix_seconds_to_local_time(summary.start_time).strftime("%Y-%m-%d %H:%M") if summary.start_time is not None else "<never started>"
        print(f"{status}\t{summary.id}\t{start_time}\t{summary.name}")

    print("")
    print("Executions to Submit:")
    for attempt in HistoryManager.get_submittable_workflow_attempts():
        status = "✅" if attempt.succeeded else f"❌"
        start_time = unix_seconds_to_local_time(attempt.start_time).strftime("%Y-%m-%d %H:%M")
        print(f"{status}\t{attempt.workflow_id}\t{start_time}\t{attempt.workflow_trs_spec}\tAttempt #{attempt.attempt_number}")

    print("")
    print("Jobs to Submit:")
    for workflow_attempt in HistoryManager.get_workflow_attempts_with_submittable_job_attempts():
        print(f"\tWorkflow {workflow_attempt.workflow_id} Attempt #{workflow_attempt.attempt_number} of {workflow_attempt.workflow_trs_spec}:")
        for job_attempt in HistoryManager.get_unsubmitted_job_attempts(workflow_attempt.workflow_id, workflow_attempt.attempt_number):
            status = "✅" if job_attempt.succeeded else f"❌"
            start_time = unix_seconds_to_local_time(job_attempt.start_time).strftime("%Y-%m-%d %H:%M")
            print(f"\t\t{status}\t{job_attempt.job_name}\t{start_time}")
        

    if options.submit:
        # The submission code needs to submit things that can be submitted and
        # not stop just because one thing in the database is uninterpretable or
        # Dockstore rejects something.
        for workflow_attempt in HistoryManager.get_submittable_workflow_attempts():
            logger.info("Submitting %s attempt %s to Dockstore", workflow_attempt.workflow_id, workflow_attempt.attempt_number)
            submitted = False
            try:
                # If it's submittable the TRS spec will be filled in.
                # Satisfy MyPy
                # TODO: change the type?
                assert workflow_attempt.workflow_trs_spec is not None
                trs_id, trs_version = parse_trs_spec(workflow_attempt.workflow_trs_spec)
                if trs_version is None:
                    raise ValueError("Workflow stored in history with TRS ID but without TRS version")
                
                # Pack it up
                workflow_metrics = pack_workflow_metrics(workflow_execution_id(workflow_attempt), workflow_attempt.start_time, workflow_attempt.runtime, workflow_attempt.succeeded)
                # Send it in
                send_metrics(trs_id, trs_version, [workflow_metrics], [])
                submitted = True
            except:
                logger.exception("Could not submit to Dockstore")
                # Ignore failed submissions and keep working on other ones.

            if submitted:
                # Record submission.
                # TODO: We don't actually save the ID we generated for Dockstore, we just need to remember the algorithm.
                HistoryManager.mark_workflow_attempt_submitted(workflow_attempt.workflow_id, workflow_attempt.attempt_number)
                logger.info("Recorded Dockstore metrics submission %s in database", workflow_execution_id(workflow_attempt))

                # Compose the URL you would fetch it back from
                assert trs_version is not None
                execution_url = get_metrics_url(trs_id, trs_version, workflow_execution_id(workflow_attempt))
                logger.debug("Dockstore accepted submission %s", execution_url)

        for workflow_attempt in HistoryManager.get_workflow_attempts_with_submittable_job_attempts():
            job_attempts = HistoryManager.get_unsubmitted_job_attempts(workflow_attempt.workflow_id, workflow_attempt.attempt_number)
            logger.info("Submitting %s jobs from %s attempt %s to Dockstore", len(job_attempts), workflow_attempt.workflow_id, workflow_attempt.attempt_number)

            submitted = False
            try:
                # If it's submittable the TRS spec will be filled in.
                # Satisfy MyPy
                # TODO: change the type?
                assert workflow_attempt.workflow_trs_spec is not None
                trs_id, trs_version = parse_trs_spec(workflow_attempt.workflow_trs_spec)
                if trs_version is None:
                    raise ValueError("Workflow stored in history with TRS ID but without TRS version")
                
                # Pack it up
                per_task_metrics = [pack_single_task_metrics(job_execution_id(job_attempt), job_attempt.start_time, job_attempt.runtime, job_attempt.succeeded, job_name=job_attempt.job_name) for job_attempt in job_attempts]
                task_set_metrics = pack_workflow_task_set_metrics(workflow_task_set_execution_id(workflow_attempt), workflow_attempt.start_time, per_task_metrics)
                
                # Send it in
                send_metrics(trs_id, trs_version, [], [task_set_metrics])
                submitted = True
            except:
                logger.exception("Could not submit to Dockstore")
                # Ignore failed submissions and keep working on other ones.
            
            if submitted:
                # Record submission of all job attempts in one transaction.
                HistoryManager.mark_job_attempts_submitted([job_attempt.id for job_attempt in job_attempts])
                logger.info("Recorded Dockstore metrics submission %s in database", workflow_task_set_execution_id(workflow_attempt))

                # Compose the URL you would fetch it back from
                assert trs_version is not None
                execution_url = get_metrics_url(trs_id, trs_version, workflow_task_set_execution_id(workflow_attempt))
                logger.debug("Dockstore accepted submission %s", execution_url)
            
    
                
        



