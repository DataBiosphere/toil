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

from toil.lib.dockstore import send_metrics, get_metrics_url
from toil.lib.history import HistoryManager
from toil.lib.misc import unix_seconds_to_local_time
from toil.lib.trs import parse_trs_spec


logger = logging.getLogger(__name__)

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

    if options.submit:
        for attempt in HistoryManager.get_submittable_workflow_attempts():
            logger.info("Submitting %s attempt %s to Dockstore", attempt.workflow_id, attempt.attempt_number)
            submitted = False
            dockstore_execution_id: Optional[str] = None
            try:
                # If it's submittable the TRS spec will be filled in.
                # Satisfy MyPy
                # TODO: change the type?
                assert attempt.workflow_trs_spec is not None
                trs_id, trs_version = parse_trs_spec(attempt.workflow_trs_spec)
                if trs_version is None:
                    raise ValueError("Workflow stored in history with TRS ID but without TRS version")
                
                # Compose a Dockstore-compatible ID
                dockstore_execution_id = attempt.workflow_id.replace("-", "_") + "_attempt_" + str(attempt.attempt_number)
                # Send it in
                send_metrics(trs_id, trs_version, dockstore_execution_id, attempt.start_time, attempt.runtime, attempt.succeeded)
                submitted = True
            except:
                logger.exception("Could not submit to Dockstore")
                # Ignore failed submissions and keep working on other ones.

            
            if submitted:
                # Record submission.
                # TODO: We don't actually save the ID we generated for Dockstore, we just need to remember the algorithm.
                HistoryManager.mark_workflow_attempt_submitted(attempt.workflow_id, attempt.attempt_number)
                logger.info("Recorded Dockstore metrics submission %s in database", dockstore_execution_id)

                # Compose the URL you would fetch it back from
                assert dockstore_execution_id is not None
                assert trs_version is not None
                execution_url = get_metrics_url(trs_id, trs_version, dockstore_execution_id)
                logger.debug("Dockstore accepted submission %s", execution_url)

                
        



