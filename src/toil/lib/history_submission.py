# Copyright (C) 2025 Regents of the University of California
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
Contains logic for generating Toil usage history reports to send back to Toil
HQ (via Dockstore), and for working out if the user wants to send them.
"""

import collections
import io
import json
import logging
import os
import pathlib
import sys
import textwrap
from typing import Any, Literal, Optional, TypeVar, Union

from toil.lib.dockstore import (
    send_metrics,
    get_metrics_url,
    pack_workflow_metrics,
    pack_single_task_metrics,
    pack_workflow_task_set_metrics,
    RunExecution,
    TaskExecutions
)
from toil.lib.history import HistoryManager, WorkflowAttemptSummary, JobAttemptSummary
from toil.lib.misc import unix_seconds_to_local_time
from toil.lib.trs import parse_trs_spec
from toil.lib.io import get_toil_home

logger = logging.getLogger(__name__)

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

class Submission:
    """
    Class holding a package of information to submit to Dockstore, and the
    information needed to mark the workflows/tasks as submitted if it is accepted.

    Acceptance is at the TRS id/version combination level within the
    Submission, since we need one request per TRS ID/version combination.
    """

    def __init__(self) -> None:
        """
        Crerate a new empty submission.
        """

        # This collects everything by TRS ID and version, and keeps separate lists for workflows and for task sets.
        # Each thing to be submitted comes along with the reference to what in the database to makr submitted when it goes in.
        self.data: dict[tuple[str, str], tuple[list[tuple[RunExecution, str, int]], list[tuple[TaskExecutions, list[str]]]]] = collections.defaultdict(lambda: ([], []))


    def add_workflow_attempt(self, workflow_attempt: WorkflowAttemptSummary) -> None:
        """
        Add a workflow attempt to the submission.

        May raise an exception if the workflow attempt is not well-formed.
        """
        # If it's submittable the TRS spec will be filled in.
        # Satisfy MyPy
        # TODO: change the type?
        assert workflow_attempt.workflow_trs_spec is not None
        trs_id, trs_version = parse_trs_spec(workflow_attempt.workflow_trs_spec)
        if trs_version is None:
            raise ValueError("Workflow stored in history with TRS ID but without TRS version")
       
        # Figure out what kind of job store was used.
        job_store_type: Optional[str] = None
        try:
            from toil.common import Toil
            job_store_type = Toil.parseLocator(workflow_attempt.workflow_job_store)[0]
            if job_store_type not in Toil.JOB_STORE_TYPES:
                # Make sure we don't send typo'd job store types in.
                job_store_type = None
        except RuntimeError:
            # Could not parse the stored locator.
            pass
       
        # Pack it up
        workflow_metrics = pack_workflow_metrics(
            workflow_execution_id(workflow_attempt),
            workflow_attempt.start_time,
            workflow_attempt.runtime,
            workflow_attempt.succeeded,
            batch_system=workflow_attempt.batch_system,
            caching=workflow_attempt.caching,
            toil_version=workflow_attempt.toil_version,
            python_version=workflow_attempt.python_version,
            platform_system=workflow_attempt.platform_system,
            platform_machine=workflow_attempt.platform_machine,
            job_store_type=job_store_type
        )
        
        # Add it to the package
        self.data[(trs_id, trs_version)][0].append((workflow_metrics, workflow_attempt.workflow_id, workflow_attempt.attempt_number))

    def add_job_attempts(self, workflow_attempt: WorkflowAttemptSummary, job_attempts: list[JobAttemptSummary]) -> None:
        """
        Add the job attempts for a workflow attempt to the submission.
        """

        # If it's submittable the TRS spec will be filled in.
        # Satisfy MyPy
        # TODO: change the type?
        assert workflow_attempt.workflow_trs_spec is not None
        trs_id, trs_version = parse_trs_spec(workflow_attempt.workflow_trs_spec)
        if trs_version is None:
            raise ValueError("Workflow stored in history with TRS ID but without TRS version")
        
        # Pack it up
        per_task_metrics = [
            pack_single_task_metrics(
                job_execution_id(job_attempt),
                job_attempt.start_time,
                job_attempt.runtime,
                job_attempt.succeeded,
                job_name=job_attempt.job_name,
                cores=job_attempt.cores,
                cpu_seconds=job_attempt.cpu_seconds,
                memory_bytes=job_attempt.memory_bytes,
                disk_bytes=job_attempt.disk_bytes
            ) for job_attempt in job_attempts
        ]
        task_set_metrics = pack_workflow_task_set_metrics(workflow_task_set_execution_id(workflow_attempt), workflow_attempt.start_time, per_task_metrics)
        
        # Add it to the package
        self.data[(trs_id, trs_version)][1].append((task_set_metrics, [job_attempt.id for job_attempt in job_attempts]))

    def empty(self) -> bool:
        """
        Return True if there is nothing to actually submit.
        """

        for _, (workflow_metrics, task_metrics) in self.data.items():
            # For each TRS ID and version we might have data for
            if len(workflow_metrics) > 0:
                return False
            if len(task_metrics) > 0:
                # Even if there's no tasks in each, we can report that we ran no tasks
                return False

        return True

    def report(self) -> str:
        """
        Compose a multi-line human-readable string report about what will be submitted.
        """
        
        stream = io.StringIO()

        for (trs_id, trs_version), (workflow_metrics, task_metrics) in self.data.items():
            stream.write(f"For workflow {trs_id} version {trs_version}:\n")
            if len(workflow_metrics) > 0:
                stream.write("  Workflow executions:\n")
                for workflow_metric, _, _ in workflow_metrics:
                    stream.write(textwrap.indent(json.dumps(workflow_metric, indent=2), '    '))
            if len(task_metrics) > 0:
                stream.write("  Task execution sets:\n")
                for tasks_metric, _ in task_metrics:
                    stream.write(textwrap.indent(json.dumps(tasks_metric, indent=2), '    '))

        return stream.getvalue()

    def submit(self) -> bool:
        """
        Send in workflow and task information to Dockstore.

        Assumes the user has approved this already.

        If submission succeeds for a TRS id/version combination, records this in the history database.

        Handles errors internally. Will not raise if the submission doesn't go through.

        :returns: False if there was any error, True if submission was accepted and recorded locally.
        """
        
        all_submitted_and_marked = True

        for (trs_id, trs_version), (workflow_metrics, task_metrics) in self.data.items():
            submitted = False
            try:
                send_metrics(trs_id, trs_version, [item[0] for item in workflow_metrics], [item[0] for item in task_metrics])
                submitted = True
            except:
                logger.exception("Could not submit to Dockstore")
                all_submitted_and_marked = False
                # Ignore failed submissions and keep working on other ones.

            if submitted:
                for _, workflow_id, attempt_number in workflow_metrics:
                    # For each workflow attempt of this TRS ID/version that we successfully submitted, mark it
                    try:
                        HistoryManager.mark_workflow_attempt_submitted(workflow_id, attempt_number)
                    except:
                        logger.exception("Could not mark workflow attempt submitted")
                        all_submitted_and_marked = False
                        # If we can't mark one, keep marking others
                for _, job_attempt_ids in task_metrics:
                    # For each colleciton of task attempts of this TRS ID/version that we successfully submitted, mark it
                    try:
                        HistoryManager.mark_job_attempts_submitted(job_attempt_ids)
                    except:
                        logger.exception("Could not mark job attempts submitted")
                        all_submitted_and_marked = False
                        # If we can't mark one set, keep marking others

        return all_submitted_and_marked


def create_history_submission(batch_size: int = 10, desired_tasks: int = 1000) -> Submission:
    """
    Make a package of data about recent workflow runs to send in.

    Returns a Submission object that remembers how to update the history
    database to record when it is successfully submitted.
    """
    
    # Collect together some workflows and some lists of tasks into a submission.
    submission = Submission()
    
    # We don't want to submit many large workflows' tasks in one request. So we
    # count how many tasks we've colected so far.
    total_tasks = 0

    for workflow_attempt in HistoryManager.get_submittable_workflow_attempts(limit=batch_size):
        try:
            submission.add_workflow_attempt(workflow_attempt)
        except:
            logger.exception("Could not compose metrics report for workflow execution")
            # Ignore failed submissions and keep working on other ones.
            # TODO: What happens when they accumulate and fill the whole batch from the database?

    for workflow_attempt in HistoryManager.get_workflow_attempts_with_submittable_job_attempts(limit=batch_size):
        job_attempts = HistoryManager.get_unsubmitted_job_attempts(workflow_attempt.workflow_id, workflow_attempt.attempt_number)
        try:
            submission.add_job_attempts(workflow_attempt, job_attempts)

            total_tasks += len(job_attempts)

            if total_tasks >= desired_tasks:
                # Don't add any more task sets to this submission.
                break
        except:
            logger.exception("Could not compose metrics report for workflow task set")
            # Ignore failed submissions and keep working on other ones.
            # TODO: What happens when they accumulate and fill the whole batch from the database?

    return submission

def create_current_submission(workflow_id: str, attempt_number: int) -> Submission:
    """
    Make a package of data about the current workflow attempt to send in.

    Useful if the user wants to submit workflow metrics just this time.
    """

    submission = Submission()
    try:
        workflow_attempt = HistoryManager.get_workflow_attempt(workflow_id, attempt_number)
        if workflow_attempt is not None:
            if not workflow_attempt.submitted_to_dockstore:
                submission.add_workflow_attempt(workflow_attempt)
            try:
                job_attempts = HistoryManager.get_unsubmitted_job_attempts(workflow_attempt.workflow_id, workflow_attempt.attempt_number)
                submission.add_job_attempts(workflow_attempt, job_attempts)
            except:
                logger.exception("Could not compose metrics report for workflow task set")
                # Keep going with just the workflow.
    except:
        logger.exception("Could not compose metrics report for workflow execution")
        # Keep going with an empty submission.
    
    return submission

# We ahve dialog functions that MyPy knows can return strings from a possibly restricted set
KeyType = TypeVar('T', bound=str)

def dialog_tkinter(title: str, text: str, options: dict[KeyType, str]) -> KeyType:
    """
    Display a dialog with tkinter.

    Dialog will have the given title, text, and options.

    :param options: Dict from machine-readable option key to button text.
    :returns: the key of the selected option.
    :raises: an exception if the dialog cannot be displayed.
    """

    # TODO: implement
    import tkinter
    from tkinter import ttk
    
    # Get a root window
    root = tkinter.Tk()
    # Make a frame
    frame = ttk.Frame(root, padding=10)
    # Make it use a grid layout
    frame.grid()
    # Lay out a label in the frame's grid
    ttk.Label(frame, text="Hello World!").grid(column=0, row=0)
    # Also a button that destroys the window
    ttk.Button(frame, text="Quit", command=root.destroy).grid(column=1, row=0)
    # Run the window's main loop
    root.mainloop()
   
    return ""

def dialog_applescript(title: str, text: str, options: dict[KeyType, str]) -> KeyType:
    """
    Display a dialog with AppleScript.

    Dialog will have the given title, text, and options.

    :param options: Dict from machine-readable option key to button text.
    :returns: the key of the selected option.
    :raises: an exception if the dialog cannot be displayed.
    """

    raise NotImplementedError()

def dialog_tui(title: str, text: str, options: dict[KeyType, str]) -> KeyType:
    """
    Display a dialog in the terminal.

    Dialog will have the given title, text, and options.

    :param options: Dict from machine-readable option key to button text.
    :returns: the key of the selected option.
    :raises: an exception if the dialog cannot be displayed.
    """

   # See https://python-prompt-toolkit.readthedocs.io/en/master/pages/dialogs.html#button-dialog

    from prompt_toolkit.shortcuts import button_dialog
    
    # We take button options in the reverse order from how prompt_toolkit does it.
    # TODO: This is not scrollable! What if there's more than 1 screen of text?
    # TODO: Use come kind of ScrollablePane like <https://stackoverflow.com/q/68369073>
    return button_dialog(
        title=title,
        text=text,
        buttons=[(v, k) for k, v in options.items()],
    ).run()

    # TODO: After this, MiniWDL will crash in asyncio's
    # get_current_event_loop() and complain there isn't one.

# Define the dialog form in the abstract
Decision = Union[Literal["all"], Literal["current"], Literal["no"], Literal["never"]]

DIALOG_TITLE = "Publish Workflow Metrics on Dockstore?"

# We need to fromat the default config path in to the message, but we can't get
# it until we can close the circular import loop. So it's a placeholder here.
# We also leave this un-wrapped to let the dialog wrap it if needed.
DIALOG_TEXT = """
Would you like to publish execution metrics to Dockstore?

This includes information like a random unique ID for the workflow execution and each job execution, the TRS ID of the workflow you are running, the names of its jobs, when and for how long they run, how much CPU, memory, and disk they are allocated or use, whether they succeed or fail, the versions of Toil and Python you used, the operating system platform and processor type, and which Toil or Toil plugin features the workflow uses.

Dockstore uses this information to prepare reports about how well workflows run in different environments, and what resources they need, to help users plan their workflow runs. The Toil developers also consult this information to see which Toil features are the most popular and how pipular Toil is overall.

Note that publishing is PERMANENT! You WILL NOT be able to recall or un-publish any published metrics!

(You can change your choice by editing the "publishWorkflowMetrics" setting in the "{}" file. You can override it once with the "--publishWorkflowMetrics" command line option.)

All: Publish for this run and all future AND PAST runs of ALL workflows.
Yes: Publish for just this run, and ask again for future runs.
No: Do not publish anything now, but ask again for future workflow runs.
Never: Do not publish anything and stop asking.
""".strip()

DIALOG_OPTIONS: dict[Decision, str] = {
    "all": "All",
    "current": "Yes",
    "no": "No",
    "never": "Never"
}

# Make sure the option texts are short enough; prompt_toolkit can only handle 10 characters.
for k, v in DIALOG_OPTIONS.items():
    assert len(v) <= 10, f"Label for \"{k}\" dialog option is too long to work on all backends!"

def ask_user_about_publishing_metrics() -> Union[Literal["all"], Literal["current"], Literal["no"]]:
    """
    Ask the user to set standing workflow submission consent.

    If the user makes a persistent decision (always or never), save it to the default Toil config file.

    :returns: The user's decision about when to publish metrics.
    """

    from toil.common import update_config, get_default_config_path

    # Find the default config path to talk about or update
    default_config_path = get_default_config_path()

    # Actual chatting with the user will fill this in if possible
    decision: Optional[Decision] = None

    if sys.stdin.isatty() and sys.stderr.isatty():
        # IO is not redirected (except maybe JSON to a file). We might be able to raise the user.

        # TODO: Get a lock

        for strategy in [dialog_tkinter, dialog_applescript, dialog_tui]:
            try:
                decision = strategy(DIALOG_TITLE, DIALOG_TEXT.format(default_config_path), DIALOG_OPTIONS)
            except:
                logger.exception("Could not use %s", strategy)

        if decision is None:
            # If we think we should be able to reach the user, but we can't, fail and make them tell us via option
            # TODO: Provide a command (or a general toil config editing command) to managte this setting
            logger.critical("Decide whether to publish workflow metrics and pass --publishWorkflowMetrics=[all|current|no]")
            sys.exit(1)
    
    if decision is None:
        # If we can't reach the user
        decision = "no"

    result = decision if decision != "never" else "no"

    if decision in ("all", "never"):
        # These are persistent and should save to the config
        update_config(default_config_path, "publishWorkflowMetrics", result)
        
    return result
    



    

    



