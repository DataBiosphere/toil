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

import logging
import sys
from argparse import ArgumentParser, _ArgumentGroup
from typing import Any, Callable, List, Optional, TypeVar, Union

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

from toil.batchSystems.registry import (DEFAULT_BATCH_SYSTEM,
                                        get_batch_system,
                                        get_batch_systems)
from toil.lib.threading import cpu_count

logger = logging.getLogger(__name__)

class OptionSetter(Protocol):
    """
    Protocol for the setOption function we get to let us set up CLI options for
    each batch system.

    Actual functionality is defined in the Config class.
    """

    OptionType = TypeVar('OptionType')
    def __call__(
        self,
        option_name: str,
        parsing_function: Optional[Callable[[Any], OptionType]] = None,
        check_function: Optional[Callable[[OptionType], Union[None, bool]]] = None,
        default: Optional[OptionType] = None,
        env: Optional[List[str]] = None,
        old_names: Optional[List[str]] = None
    ) -> bool:
        ...

def set_batchsystem_options(batch_system: Optional[str], set_option: OptionSetter) -> None:
    """
    Call set_option for all the options for the given named batch system, or
    all batch systems if no name is provided.
    """
    if batch_system is not None:
        # Use this batch system
        batch_system_type = get_batch_system(batch_system)
        batch_system_type.setOptions(set_option)
    else:
        for name in get_batch_systems():
            # All the batch systems are responsible for setting their own options
            # with their setOptions() class methods.
            try:
                batch_system_type = get_batch_system(name)
            except ImportError:
                # Skip anything we can't import
                continue
            # Ask the batch system to tell us all its options.
            batch_system_type.setOptions(set_option)
    # Options shared between multiple batch systems
    set_option("disableAutoDeployment")
    # Make limits maximum if set to 0
    set_option("max_jobs")
    set_option("max_local_jobs")
    set_option("manualMemArgs")
    set_option("run_local_jobs_on_workers")
    set_option("statePollingWait")
    set_option("state_polling_timeout")
    set_option("batch_logs_dir")


def add_all_batchsystem_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    from toil.options.common import SYS_MAX_SIZE

    # Do the global cross-batch-system arguments
    parser.add_argument(
        "--batchSystem",
        dest="batchSystem",
        default=DEFAULT_BATCH_SYSTEM,
        choices=get_batch_systems(),
        help=f"The type of batch system to run the job(s) with, currently can be one "
        f"of {', '.join(get_batch_systems())}. Default = {DEFAULT_BATCH_SYSTEM}",
    )
    parser.add_argument(
        "--disableHotDeployment",
        dest="disableAutoDeployment",
        default=False,
        action="store_true",
        help="Hot-deployment was renamed to auto-deployment.  Option now redirects to "
        "--disableAutoDeployment. Left in for backwards compatibility.",
    )
    parser.add_argument(
        "--disableAutoDeployment",
        dest="disableAutoDeployment",
        default=False,
        help="Should auto-deployment of Toil Python workflows be deactivated? If True, the workflow's "
        "Python code should be present at the same location on all workers. Default = False.",
    )
    parser.add_argument(
        "--maxJobs",
        dest="max_jobs",
        default=SYS_MAX_SIZE, # This is *basically* unlimited and saves a lot of Optional[]
        type=lambda x: int(x) or SYS_MAX_SIZE,
        help="Specifies the maximum number of jobs to submit to the "
             "backing scheduler at once. Not supported on Mesos or "
             "AWS Batch. Use 0 for unlimited. Defaults to unlimited.",
    )
    parser.add_argument(
        "--maxLocalJobs",
        dest="max_local_jobs",
        default=None,
        type=lambda x: int(x) or 0,
        help=f"Specifies the maximum number of housekeeping jobs to "
             f"run sumultaneously on the local system. Use 0 for "
             f"unlimited. Defaults to the number of local cores ({cpu_count()}).",
    )
    parser.add_argument(
        "--manualMemArgs",
        default=False,
        dest="manualMemArgs",
        action="store_true",
        help="Do not add the default arguments: 'hv=MEMORY' & 'h_vmem=MEMORY' to the qsub "
        "call, and instead rely on TOIL_GRIDGENGINE_ARGS to supply alternative arguments.  "
        "Requires that TOIL_GRIDGENGINE_ARGS be set.",
    )
    parser.add_argument(
        "--runLocalJobsOnWorkers",
        "--runCwlInternalJobsOnWorkers",
        dest="run_local_jobs_on_workers",
        default=False,
        action="store_true",
        help="Whether to run jobs marked as local (e.g. CWLScatter) on the worker nodes "
        "instead of the leader node. If false (default), then all such jobs are run on "
        "the leader node. Setting this to true can speed up CWL pipelines for very large "
        "workflows with many sub-workflows and/or scatters, provided that the worker "
        "pool is large enough.",
    )
    parser.add_argument(
        "--coalesceStatusCalls",
        dest="coalesceStatusCalls",
        action="store_true",
        default=True,
        help=(
            "Ask for job statuses from the batch system in a batch. Deprecated; now always "
            "enabled where supported."
        ),
    )
    parser.add_argument(
        "--statePollingWait",
        dest="statePollingWait",
        type=int,
        default=None,
        help="Time, in seconds, to wait before doing a scheduler query for job state.  "
             "Return cached results if within the waiting period. Only works for grid "
             "engine batch systems such as gridengine, htcondor, torque, slurm, and lsf."
    )
    parser.add_argument(
        "--statePollingTimeout",
        dest="state_polling_timeout",
        type=int,
        default=1200,
        help="Time, in seconds, to retry against a broken scheduler. Only works for grid "
             "engine batch systems such as gridengine, htcondor, torque, slurm, and lsf."
    )
    parser.add_argument(
        "--batchLogsDir",
        dest="batch_logs_dir",
        default=None,
        env_var="TOIL_BATCH_LOGS_DIR",
        help="Directory to tell the backing batch system to log into. Should be available "
             "on both the leader and the workers, if the backing batch system writes logs "
             "to the worker machines' filesystems, as many HPC schedulers do. If unset, "
             "the Toil work directory will be used. Only works for grid engine batch "
             "systems such as gridengine, htcondor, torque, slurm, and lsf."
    )

    parser.add_argument('--memoryIsProduct', dest='memory_is_product', default=False, action="store_true",
                        help="If the batch system understands requested memory as a product of the requested memory and the number"
                             "of cores, set this flag to properly allocate memory.")

    for name in get_batch_systems():
        # All the batch systems are responsible for adding their own options
        # with the add_options class method.
        try:
            batch_system_type = get_batch_system(name)
        except ImportError:
            # Skip anything we can't import
            continue
        # Ask the batch system to create its options in the parser
        logger.debug('Add options for %s batch system', name)
        batch_system_type.add_options(parser)
