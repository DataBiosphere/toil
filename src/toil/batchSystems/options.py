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
import socket
from argparse import ArgumentParser, _ArgumentGroup
from contextlib import closing
from typing import Callable, Union

from toil.batchSystems.registry import (
    BATCH_SYSTEM_FACTORY_REGISTRY,
    BATCH_SYSTEMS,
    DEFAULT_BATCH_SYSTEM,
)
from toil.lib.threading import cpu_count


def getPublicIP() -> str:
    """Get the IP that this machine uses to contact the internet.

    If behind a NAT, this will still be this computer's IP, and not the router's."""
    try:
        # Try to get the internet-facing IP by attempting a connection
        # to a non-existent server and reading what IP was used.
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
            # 203.0.113.0/24 is reserved as TEST-NET-3 by RFC 5737, so
            # there is guaranteed to be no one listening on the other
            # end (and we won't accidentally DOS anyone).
            sock.connect(('203.0.113.1', 1))
            ip = sock.getsockname()[0]
        return ip
    except:
        # Something went terribly wrong. Just give loopback rather
        # than killing everything, because this is often called just
        # to provide a default argument
        return '127.0.0.1'


def add_parasol_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    parser.add_argument("--parasolCommand", dest="parasolCommand", default='parasol',
                        help="The name or path of the parasol program. Will be looked up on PATH "
                             "unless it starts with a slash.  (default: %(default)s).")
    parser.add_argument("--parasolMaxBatches", dest="parasolMaxBatches", default=1000,
                        help="Maximum number of job batches the Parasol batch is allowed to create. One batch is "
                             "created for jobs with a a unique set of resource requirements.  (default: %(default)s).")


def add_single_machine_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    parser.add_argument("--scale", dest="scale", default=1,
                        help="A scaling factor to change the value of all submitted tasks's submitted cores.  "
                             "Used in the single_machine batch system.  (default: %(default)s).")

    link_imports = parser.add_mutually_exclusive_group()
    link_imports_help = ("When using a filesystem based job store, CWL input files are by default symlinked in.  "
                         "Specifying this option instead copies the files into the job store, which may protect "
                         "them from being modified externally.  When not specified and as long as caching is enabled, "
                         "Toil will protect the file automatically by changing the permissions to read-only.")
    link_imports.add_argument("--linkImports", dest="linkImports", action='store_true', help=link_imports_help)
    link_imports.add_argument("--noLinkImports", dest="linkImports", action='store_false', help=link_imports_help)
    link_imports.set_defaults(linkImports=True)

    move_exports = parser.add_mutually_exclusive_group()
    move_exports_help = ('When using a filesystem based job store, output files are by default moved to the '
                         'output directory, and a symlink to the moved exported file is created at the initial '
                         'location.  Specifying this option instead copies the files into the output directory.  '
                         'Applies to filesystem-based job stores only.')
    move_exports.add_argument("--moveExports", dest="moveExports", action='store_true', help=move_exports_help)
    move_exports.add_argument("--noMoveExports", dest="moveExports", action='store_false', help=move_exports_help)
    move_exports.set_defaults(moveExports=False)


def add_mesos_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    parser.add_argument("--mesosMaster", dest="mesosMasterAddress", default=f'{getPublicIP()}:5050',
                        help="The host and port of the Mesos master separated by colon.  (default: %(default)s)")


def add_kubernetes_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    parser.add_argument("--kubernetesHostPath", dest="kubernetesHostPath", default=None,
                        help="Path on Kubernetes hosts to use as shared inter-pod temp directory.  "
                             "(default: %(default)s)")

def add_slurm_options(parser: Union[ArgumentParser, _ArgumentGroup]):
    allocate_mem = parser.add_mutually_exclusive_group()
    allocate_mem_help = ("A flag that can block allocating memory with '--mem' for job submissions "
                         "on SLURM since some system servers may reject any job request that "
                         "explicitly specifies the memory allocation.  The default is to always allocate memory.")
    allocate_mem.add_argument("--dont_allocate_mem", action='store_false', dest="allocate_mem", help=allocate_mem_help)
    allocate_mem.add_argument("--allocate_mem", action='store_true', dest="allocate_mem", help=allocate_mem_help)
    allocate_mem.set_defaults(allocate_mem=True)

def set_batchsystem_options(batch_system: str, set_option: Callable) -> None:
    batch_system_factory = BATCH_SYSTEM_FACTORY_REGISTRY[batch_system]()
    batch_system_factory.setOptions(set_option)


def add_all_batchsystem_options(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
    # TODO: Only add options for the system the user is specifying?
    parser.add_argument(
        "--batchSystem",
        dest="batchSystem",
        default=DEFAULT_BATCH_SYSTEM,
        choices=BATCH_SYSTEMS,
        help=f"The type of batch system to run the job(s) with, currently can be one "
        f"of {', '.join(BATCH_SYSTEMS)}. default={DEFAULT_BATCH_SYSTEM}",
    )
    parser.add_argument(
        "--disableHotDeployment",
        dest="disableAutoDeployment",
        action="store_true",
        default=None,
        help="Hot-deployment was renamed to auto-deployment.  Option now redirects to "
        "--disableAutoDeployment.  Left in for backwards compatibility.",
    )
    parser.add_argument(
        "--disableAutoDeployment",
        dest="disableAutoDeployment",
        action="store_true",
        default=None,
        help="Should auto-deployment of the user script be deactivated? If True, the user "
        "script/package should be present at the same location on all workers.  Default = False.",
    )
    parser.add_argument(
        "--maxLocalJobs",
        default=cpu_count(),
        help=f"For batch systems that support a local queue for housekeeping jobs "
        f"(Mesos, GridEngine, htcondor, lsf, slurm, torque).  Specifies the maximum "
        f"number of these housekeeping jobs to run on the local system.  The default "
        f"(equal to the number of cores) is a maximum of {cpu_count()} concurrent "
        f"local housekeeping jobs.",
    )
    parser.add_argument(
        "--manualMemArgs",
        default=False,
        action="store_true",
        dest="manualMemArgs",
        help="Do not add the default arguments: 'hv=MEMORY' & 'h_vmem=MEMORY' to the qsub "
        "call, and instead rely on TOIL_GRIDGENGINE_ARGS to supply alternative arguments.  "
        "Requires that TOIL_GRIDGENGINE_ARGS be set.",
    )
    parser.add_argument(
        "--runCwlInternalJobsOnWorkers",
        dest="runCwlInternalJobsOnWorkers",
        action="store_true",
        default=None,
        help="Whether to run CWL internal jobs (e.g. CWLScatter) on the worker nodes "
        "instead of the primary node. If false (default), then all such jobs are run on "
        "the primary node. Setting this to true can speed up the pipeline for very large "
        "workflows with many sub-workflows and/or scatters, provided that the worker "
        "pool is large enough.",
    )
    parser.add_argument(
        "--coalesceStatusCalls",
        dest="coalesceStatusCalls",
        action="store_true",
        default=None,
        help=(
            "Coalese status calls to prevent the batch system from being overloaded. "
            "Currently only supported for LSF. "
            "default=false"
        ),
    )

    add_parasol_options(parser)
    add_single_machine_options(parser)
    add_mesos_options(parser)
    add_slurm_options(parser)
    add_kubernetes_options(parser)


def set_batchsystem_config_defaults(config) -> None:
    """
    Set default options for builtin batch systems. This is required if a Config
    object is not constructed from an Options object.
    """
    config.batchSystem = "single_machine"
    config.disableAutoDeployment = False
    config.environment = {}
    config.statePollingWait = None
    config.maxLocalJobs = cpu_count()
    config.manualMemArgs = False
    config.coalesceStatusCalls = False

    # parasol
    config.parasolCommand = 'parasol'
    config.parasolMaxBatches = 10000

    # single machine
    config.scale = 1
    config.linkImports = False
    config.moveExports = False

    # mesos
    config.mesosMasterAddress = f'{getPublicIP()}:5050'

    # SLURM
    config.allocate_mem = True

    # Kubernetes
    config.kubernetesHostPath = None
