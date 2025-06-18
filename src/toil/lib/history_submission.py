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

import asyncio
import collections
import io
import json
import logging
import multiprocessing
import os
import pathlib
import subprocess
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

def get_parsed_trs_spec(workflow_attempt: WorkflowAttemptSummary) -> tuple[str, str]:
    """
    Get the TRS ID and version of the workflow, or raise an error.

    :returns: The TRS ID and the TRS version of the wrokflow run.
    :raises: ValueError if the workflow does not have a TRS spec or if the spec
        does not contain a version.
    """
    # If it's submittable the TRS spec will be filled in.
    if workflow_attempt.workflow_trs_spec is None:
        raise ValueError("Workflow cannot be submitted without a TRS spec")
    trs_id, trs_version = parse_trs_spec(workflow_attempt.workflow_trs_spec)
    if trs_version is None:
        raise ValueError("Workflow stored in history with TRS ID but without TRS version")
    return trs_id, trs_version

class Submission:
    """
    Class holding a package of information to submit to Dockstore, and the
    information needed to mark the workflows/tasks as submitted if it is accepted.

    Acceptance is at the TRS id/version combination level within the
    Submission, since we need one request per TRS ID/version combination.
    """

    def __init__(self) -> None:
        """
        Create a new empty submission.
        """

        # This collects everything by TRS ID and version, and keeps separate lists for workflows and for task sets.
        # Each thing to be submitted comes along with the reference to what in the database to makr submitted when it goes in.
        self.data: dict[tuple[str, str], tuple[list[tuple[RunExecution, str, int]], list[tuple[TaskExecutions, list[str]]]]] = collections.defaultdict(lambda: ([], []))


    def add_workflow_attempt(self, workflow_attempt: WorkflowAttemptSummary) -> None:
        """
        Add a workflow attempt to the submission.

        May raise an exception if the workflow attempt is not well-formed.
        """
        trs_id, trs_version = get_parsed_trs_spec(workflow_attempt)

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

        trs_id, trs_version = get_parsed_trs_spec(workflow_attempt)

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


def create_history_submission(batch_size: Optional[int] = None, desired_tasks: Optional[int] = None) -> Submission:
    """
    Make a package of data about recent workflow runs to send in.

    Returns a Submission object that remembers how to update the history
    database to record when it is successfully submitted.

    :param batch_size: Number of workflows to try and submit in one request.
    :param desired_tasks: Number of tasks to try and put into a task submission
        batch. Use 0 to not submit any task information.
    """

    # By default, include the things we are set to track history for.
    if batch_size is None:
        batch_size = 10 if HistoryManager.enabled() else 0
    if desired_tasks is None:
        desired_tasks = 50 if HistoryManager.enabled_job() else 0

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

        # TODO: Dockstore limits request size to 60k. See
        # <https://github.com/dockstore/compose_setup/blob/a14cd1d390f4ae1e14a8ba6e36ec06ce5fe2604e/templates/default.nginx_http.shared.conf.template#L15>.
        # Apply that limit to the submission and don't add more attempts if we
        # would go over it.

    # TODO: Once we have a way to deal with single runs possibly having more
    # than 60k worth of tasks, and with Dockstore turning collections of tasks
    # into extra workflow runs (see
    # <https://ucsc-cgl.atlassian.net/browse/SEAB-6919>), set the default task
    # limit to submit to be nonzero.

    for workflow_attempt in HistoryManager.get_workflow_attempts_with_submittable_job_attempts(limit=batch_size):
        job_attempts = HistoryManager.get_unsubmitted_job_attempts(workflow_attempt.workflow_id, workflow_attempt.attempt_number)

        if desired_tasks == 0 or total_tasks + len(job_attempts) > desired_tasks:
            # Don't add any more task sets to the submission
            break

        try:
            submission.add_job_attempts(workflow_attempt, job_attempts)

            total_tasks += len(job_attempts)
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
        if workflow_attempt is not None and HistoryManager.enabled():
            if not workflow_attempt.submitted_to_dockstore:
                submission.add_workflow_attempt(workflow_attempt)
            if HistoryManager.enabled_job():
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

# We have dialog functions that MyPy knows can return strings from a possibly restricted set
KeyType = TypeVar('KeyType', bound=str)

def dialog_tkinter(title: str, text: str, options: dict[KeyType, str], timeout: float) -> Optional[KeyType]:
    """
    Display a dialog with tkinter.

    Dialog will have the given title, text, and options.

    Dialog will be displayed by a separate Python process to avoid a Mac dock
    icon sticking around as long as Toil runs.

    :param options: Dict from machine-readable option key to button text.
    :returns: the key of the selected option, or None if the user declined to
        select an option.
    :raises: an exception if the dialog cannot be displayed.
    """

    # Multiprocessing queues aren't actually generic, but MyPy requires them to be.
    result_queue: "multiprocessing.Queue[Union[Exception, Optional[KeyType]]]" = multiprocessing.Queue()
    process = multiprocessing.Process(target=display_dialog_tkinter, args=(title, text, options, timeout, result_queue))
    process.start()
    result = result_queue.get()
    process.join()

    if isinstance(result, Exception):
        raise result
    else:
        return result



def display_dialog_tkinter(title: str, text: str, options: dict[KeyType, str], timeout: float, result_queue: "multiprocessing.Queue[Union[Exception, Optional[KeyType]]]") -> None:
    """
    Display a dialog with tkinter in the current process.

    Dialog will have the given title, text, and options.

    :param options: Dict from machine-readable option key to button text.

    Sends back either the key of the chosen option, or an exception, via the
    result queue.
    """

    try:

        FRAME_PADDING = 10
        BUTTON_FRAME_PADDING = 5
        DEFAULT_LABEL_WRAP = 400

        import tkinter
        from tkinter import ttk

        # Get a root window
        root = tkinter.Tk()
        # Set the title
        root.title(title)

        def close_root() -> None:
            """
            Function to close the dialog window.
            """
            # Hide the window
            root.withdraw()
            # Now we want to exit the main loop. But if we do it right away the window hide never happens and the window stays open but not responding.
            # So we schedule the root to go away.
            root.after(100, root.destroy)


        # Make a frame
        frame = ttk.Frame(root, padding=FRAME_PADDING)
        # Put it on a grid in the parent
        frame.grid(row=0, column=0, sticky="nsew")

        # Lay out a label in the frame's grid
        label = ttk.Label(frame, text=text, wraplength=DEFAULT_LABEL_WRAP)
        label.grid(column=0, row=0, sticky="nsew")

        # Add a button frame below it so the buttons can be in columns narrower than the label
        button_frame = ttk.Frame(frame, padding=BUTTON_FRAME_PADDING)
        # Put it on a grid in the parent
        button_frame.grid(row=1, column=0, sticky="sw")

        # Use a list as a mutable slot
        result: list[KeyType] = []
        button_column = 0
        for k, v in options.items():
            # A function in here will capture k by reference. Sneak the current k
            # in through a default argument, which is evaluated at definition time.
            def setter(set_to: KeyType = k) -> None:
                # Record the choice
                result.append(set_to)
                # Close the window.
                close_root()
            ttk.Button(button_frame, text=v, command=setter).grid(column=button_column, row=0)
            button_column += 1

        # Buttons do not grow as the window resizes.

        # Say row 0 of the container frame gets all its extra height
        frame.rowconfigure(0, weight=1)
        # Say column 0 of the container frame gets all its extra width
        frame.columnconfigure(0, weight=1)

        # Say row 0 of the root gets all its extra height
        root.rowconfigure(0, weight=1)
        # Say column 0 of the root gets all its extra width
        root.columnconfigure(0, weight=1)

        # Make sure the window can't get too small for the content
        def force_fit() -> None:
            """
            Make sure the root window is no smaller than its contents require.
            """
            # Give it a minimum size in inches, and also no smaller than the
            # contents require. We can't use the required width exactly because
            # that would never let us shrink and rewrap the label, so we use the
            # button frame instead and add padding ourselves.
            min_width = max(int(root.winfo_fpixels("2i")), button_frame.winfo_reqwidth() + BUTTON_FRAME_PADDING + FRAME_PADDING)
            min_height = max(int(root.winfo_fpixels("1i")), root.winfo_reqheight())
            root.minsize(min_width, min_height)

            # TODO: If the window would be too tall for the screen, make it wider for the user?

        # Rewrap the text as the window size changes
        setattr(label, "last_width", 100)
        # MyPy demands a generic argument here but Python won't be able to handle
        # it until
        # <https://github.com/python/cpython/commit/42a818912bdb367c4ec2b7d58c18db35f55ebe3b>
        # ships.
        def resize_label(event: "tkinter.Event[ttk.Label]") -> None:
            """
            If the label's width has changed, rewrap the text.

            See <https://stackoverflow.com/a/71599924>
            """
            current_width = label.winfo_width()
            if getattr(label, "last_width") != current_width:
                setattr(label, "last_width", current_width)
                label.config(wraplength=current_width)
                # Update required height calculation based on new width
                label.update_idletasks()
                # Impose minimum height
                force_fit()
        label.bind('<Configure>', resize_label)

        # Do root sizing
        force_fit()

        if timeout:
            # If we run out of time, hide the window and move on without a choice.
            root.after(int(timeout * 1000), close_root)

        # To make the dialog pop up over the terminal instead of behind it, we
        # lift it and temporarily make it topmost. We don't keep it topmost
        # because we want to let the user switch away from it.
        root.attributes('-topmost', True)
        root.after(10, lambda: root.attributes('-topmost', False))
        root.lift()

        # Run the window's main loop
        root.mainloop()

        if len(result) == 0:
            # User didn't click any buttons. Probably they don't want to deal with
            # this right now.
            result_queue.put(None)
        else:
            result_queue.put(result[0])
    except Exception as e:
        result_queue.put(e)

def dialog_tui(title: str, text: str, options: dict[KeyType, str], timeout: float) -> Optional[KeyType]:
    """
    Display a dialog in the terminal.

    Dialog will have the given title, text, and options.

    :param options: Dict from machine-readable option key to button text.
    :returns: the key of the selected option, or None if the user declined to
        select an option.
    :raises: an exception if the dialog cannot be displayed.
    """

    # See
    # <https://python-prompt-toolkit.readthedocs.io/en/master/pages/dialogs.html#button-dialog>
    # for the dialog and
    # <https://python-prompt-toolkit.readthedocs.io/en/master/pages/advanced_topics/input_hooks.html>
    # for how we run concurrently with it.

    from prompt_toolkit.shortcuts import button_dialog
    from prompt_toolkit.eventloop.inputhook import set_eventloop_with_inputhook

    # We take button options in the reverse order from how prompt_toolkit does it.
    # TODO: This is not scrollable! What if there's more than 1 screen of text?
    # TODO: Use come kind of ScrollablePane like <https://stackoverflow.com/q/68369073>
    application = button_dialog(
        title=title,
        text=text,
        buttons=[(v, k) for k, v in options.items()],
    )

    # Make the coroutine to run the dialog
    application_coroutine = application.run_async()

    async def exit_application_after_timeout() -> None:
        """
        Wait for the timeout to elapse and then close the application.
        """
        await asyncio.sleep(timeout)
        # End the application and make it return None
        application.exit()

    # Make the coroutine to stop the dialog
    timeout_coroutine = exit_application_after_timeout()

    # To avoid messing around with a global event loop and instead just race
    # some coroutines, we make our own event loop.
    loop = asyncio.new_event_loop()

    # Attach the coroutines to it so it tries to advance them
    application_task = loop.create_task(application_coroutine)
    timeout_task = loop.create_task(timeout_coroutine)

    # This task finishes when the first task in the set finishes.
    race_task = asyncio.wait({application_task, timeout_task}, return_when=asyncio.FIRST_COMPLETED)
    # Run it until then
    loop.run_until_complete(race_task)
    # Maybe the application needs to finish its exit?
    loop.run_until_complete(application_task)

    # We no longer need the stopping task/coroutine
    timeout_task.cancel()
    # We don't need to await it because we cancel it.
    # It is OK to cancel it if it is finished already.

    return application_task.result()

# Define the dialog form in the abstract
Decision = Union[Literal["all"], Literal["current"], Literal["no"], Literal["never"]]

DIALOG_TITLE = "Publish Workflow Metrics on Dockstore.org?"

# We need to fromat the default config path in to the message, but we can't get
# it until we can close the circular import loop. So it's a placeholder here.
# We also leave this un-wrapped to let the dialog wrap it if needed.
DIALOG_TEXT = """
Would you like to publish execution metrics on Dockstore.org?

This includes information like a unique ID for the workflow execution and each job execution, the Tool Registry Service (TRS) ID of the workflow, the names of its jobs, when and for how long they run, how much CPU, memory, and disk they are allocated or use, whether they succeed or fail, the versions of Toil and Python used, the operating system platform and processor type, and which Toil or Toil plugin features are used.

Dockstore.org uses this information to prepare reports about how well workflows run in different environments, and what resources they need, in order to help users plan their workflow runs. The Toil developers also consult this information to see which Toil features are the most popular and how popular Toil is overall.

Note that publishing is PERMANENT! You WILL NOT be able to recall or un-publish any published metrics!

(You can change your choice by editing the "publishWorkflowMetrics" setting in the "{}" file. You can override it once with the "--publishWorkflowMetrics" command line option.)

All: Publish for this run and all future AND PAST runs of ALL workflows.
Yes: Publish for just this run, and ask again next time.
No: Do not publish anything now, but ask again next time.
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

# Make sure the options all have unique labels
assert len(set(DIALOG_OPTIONS.values())) == len(DIALOG_OPTIONS), "Labels for dialog options are not unique!"

# How many seconds should we show the dialog for before assuming the user means "no"?
DIALOG_TIMEOUT = 120

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

        for strategy in [dialog_tkinter, dialog_tui]:
            # TODO: Add a graphical strategy that, unlike tkinter (which needs
            # homebrew python-tk), can work out of the box on Mac! AppleScript
            # and builtin NSDialog can only use 3 buttons. AppleScript choice
            # dialogs can't hold enough text. We don't really want to bring in
            # e.g. QT for this.
            try:
                strategy_decision = strategy(DIALOG_TITLE, DIALOG_TEXT.format(default_config_path), DIALOG_OPTIONS, DIALOG_TIMEOUT)
                if strategy_decision is None:
                    # User declined to choose. Treat that as choosing "no".
                    logger.warning("User did not make a selection. Assuming \"no\".")
                    strategy_decision = "no"
                decision = strategy_decision
                break
            except:
                logger.exception("Could not use %s", strategy)

        if decision is None:
            # If we think we should be able to reach the user, but we can't, fail and make them tell us via option
            # TODO: Provide a command (or a general toil config editing command) to manage this setting
            logger.critical("Decide whether to publish workflow metrics and pass --publishWorkflowMetrics=[all|current|no]")
            sys.exit(1)

    if decision is None:
        # We can't reach the user
        decision = "no"

    result = decision if decision != "never" else "no"
    if decision in ("all", "never"):
        # These are persistent and should save to the config
        update_config(default_config_path, "publishWorkflowMetrics", result)

    return result











