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
import json
import logging
import os
import pickle
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
import warnings
from argparse import (ArgumentDefaultsHelpFormatter,
                      ArgumentParser,
                      Namespace,
                      _ArgumentGroup)
from distutils.util import strtobool
from functools import lru_cache
from types import TracebackType
from typing import (IO,
                    TYPE_CHECKING,
                    Any,
                    Callable,
                    ContextManager,
                    Dict,
                    List,
                    MutableMapping,
                    Optional,
                    Set,
                    Tuple,
                    Type,
                    TypeVar,
                    Union,
                    cast,
                    overload)
from urllib.parse import urlparse

import requests

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from toil import logProcessContext, lookupEnvVar
from toil.batchSystems.options import (add_all_batchsystem_options,
                                       set_batchsystem_config_defaults,
                                       set_batchsystem_options)
from toil.bus import (ClusterDesiredSizeMessage,
                      ClusterSizeMessage,
                      JobCompletedMessage,
                      JobFailedMessage,
                      JobIssuedMessage,
                      JobMissingMessage,
                      MessageBus,
                      QueueSizeMessage,
                      gen_message_bus_path)
from toil.fileStores import FileID
from toil.lib.aws import zone_to_region, build_tag_dict_from_env
from toil.lib.compatibility import deprecated
from toil.lib.conversions import bytes2human, human2bytes
from toil.lib.io import try_path
from toil.lib.retry import retry
from toil.provisioners import (add_provisioner_options,
                               cluster_factory,
                               parse_node_types)
from toil.realtimeLogger import RealtimeLogger
from toil.statsAndLogging import (add_logging_options,
                                  root_logger,
                                  set_logging_from_options)
from toil.version import dockerRegistry, dockerTag, version

if TYPE_CHECKING:
    from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem
    from toil.batchSystems.options import OptionSetter
    from toil.job import (AcceleratorRequirement,
                          Job,
                          JobDescription,
                          TemporaryID)
    from toil.jobStores.abstractJobStore import AbstractJobStore
    from toil.provisioners.abstractProvisioner import AbstractProvisioner
    from toil.resource import ModuleDescriptor

# aim to pack autoscaling jobs within a 30 minute block before provisioning a new node
defaultTargetTime = 1800
SYS_MAX_SIZE = 9223372036854775807
# sys.max_size on 64 bit systems is 9223372036854775807, so that 32-bit systems
# use the same number
UUID_LENGTH = 32
logger = logging.getLogger(__name__)


class Config:
    """Class to represent configuration operations for a toil workflow run."""
    logFile: Optional[str]
    logRotating: bool
    cleanWorkDir: str
    max_jobs: int
    max_local_jobs: int
    run_local_jobs_on_workers: bool
    tes_endpoint: str
    tes_user: str
    tes_password: str
    tes_bearer_token: str
    jobStore: str
    batchSystem: str
    batch_logs_dir: Optional[str] = None
    """The backing scheduler will be instructed, if possible, to save logs
    to this directory, where the leader can read them."""
    workflowAttemptNumber: int
    disableAutoDeployment: bool

    def __init__(self) -> None:
        # Core options
        self.workflowID: Optional[str] = None
        """This attribute uniquely identifies the job store and therefore the workflow. It is
        necessary in order to distinguish between two consecutive workflows for which
        self.jobStore is the same, e.g. when a job store name is reused after a previous run has
        finished successfully and its job store has been clean up."""
        self.workflowAttemptNumber: int = 0
        self.jobStore: Optional[str] = None  # type: ignore
        self.logLevel: str = logging.getLevelName(root_logger.getEffectiveLevel())
        self.workDir: Optional[str] = None
        self.coordination_dir: Optional[str] = None
        self.noStdOutErr: bool = False
        self.stats: bool = False

        # Because the stats option needs the jobStore to persist past the end of the run,
        # the clean default value depends the specified stats option and is determined in setOptions
        self.clean: Optional[str] = None
        self.clusterStats = None

        # Restarting the workflow options
        self.restart: bool = False

        # Batch system options
        set_batchsystem_config_defaults(self)

        # File store options
        self.caching: Optional[bool] = None
        self.linkImports: bool = True
        self.moveExports: bool = False

        # Autoscaling options
        self.provisioner: Optional[str] = None
        self.nodeTypes: List[Tuple[Set[str], Optional[float]]] = []
        self.minNodes = None
        self.maxNodes = [10]
        self.targetTime: float = defaultTargetTime
        self.betaInertia: float = 0.1
        self.scaleInterval: int = 60
        self.preemptibleCompensation: float = 0.0
        self.nodeStorage: int = 50
        self.nodeStorageOverrides: List[str] = []
        self.metrics: bool = False
        self.assume_zero_overhead: bool = False

        # Parameters to limit service jobs, so preventing deadlock scheduling scenarios
        self.maxPreemptibleServiceJobs: int = sys.maxsize
        self.maxServiceJobs: int = sys.maxsize
        self.deadlockWait: Union[float, int] = 60  # Number of seconds we must be stuck with all services before declaring a deadlock
        self.deadlockCheckInterval: Union[float, int] = 30  # Minimum polling delay for deadlocks

        # Resource requirements
        self.defaultMemory: int = 2147483648
        self.defaultCores: Union[float, int] = 1
        self.defaultDisk: int = 2147483648
        self.defaultPreemptible: bool = False
        # TODO: These names are generated programmatically in
        # Requirer._fetchRequirement so we can't use snake_case until we fix
        # that (and add compatibility getters/setters?)
        self.defaultAccelerators: List['AcceleratorRequirement'] = []
        self.maxCores: int = SYS_MAX_SIZE
        self.maxMemory: int = SYS_MAX_SIZE
        self.maxDisk: int = SYS_MAX_SIZE

        # Retrying/rescuing jobs
        self.retryCount: int = 1
        self.enableUnlimitedPreemptibleRetries: bool = False
        self.doubleMem: bool = False
        self.maxJobDuration: int = sys.maxsize
        self.rescueJobsFrequency: int = 60

        # Log management
        self.maxLogFileSize: int = 64000
        self.writeLogs = None
        self.writeLogsGzip = None
        self.writeLogsFromAllJobs: bool = False
        self.write_messages: Optional[str] = None

        # Misc
        self.environment: Dict[str, str] = {}
        self.disableChaining: bool = False
        self.disableJobStoreChecksumVerification: bool = False
        self.sseKey: Optional[str] = None
        self.servicePollingInterval: int = 60
        self.useAsync: bool = True
        self.forceDockerAppliance: bool = False
        self.statusWait: int = 3600
        self.disableProgress: bool = False
        self.readGlobalFileMutableByDefault: bool = False
        self.kill_polling_interval: int = 5

        # Debug options
        self.debugWorker: bool = False
        self.disableWorkerOutputCapture: bool = False
        self.badWorker = 0.0
        self.badWorkerFailInterval = 0.01

        # CWL
        self.cwl: bool = False

    def prepare_start(self) -> None:
        """
        After options are set, prepare for initial start of workflow.
        """
        self.workflowAttemptNumber = 0

    def prepare_restart(self) -> None:
        """
        Before restart options are set, prepare for a restart of a workflow.
        Set up any execution-specific parameters and clear out any stale ones.
        """
        self.workflowAttemptNumber += 1
        # We should clear the stored message bus path, because it may have been
        # auto-generated and point to a temp directory that could no longer
        # exist and that can't safely be re-made.
        self.write_messages = None
        

    def setOptions(self, options: Namespace) -> None:
        """Creates a config object from the options object."""
        OptionType = TypeVar("OptionType")

        def set_option(option_name: str,
                       parsing_function: Optional[Callable[[Any], OptionType]] = None,
                       check_function: Optional[Callable[[OptionType], Union[None, bool]]] = None,
                       default: Optional[OptionType] = None,
                       env: Optional[List[str]] = None,
                       old_names: Optional[List[str]] = None) -> None:
            """
            Determine the correct value for the given option.

            Priority order is:

            1. options object under option_name
            2. options object under old_names
            3. environment variables in env
            4. provided default value

            Selected option value is run through parsing_funtion if it is set.
            Then the parsed value is run through check_function to check it for
            acceptability, which should raise AssertionError if the value is
            unacceptable.

            If the option gets a non-None value, sets it as an attribute in
            this Config.
            """
            option_value = getattr(options, option_name, default)

            if old_names is not None:
                for old_name in old_names:
                    # Try all the old names in case user code is setting them
                    # in an options object.
                    if option_value != default:
                        break
                    if hasattr(options, old_name):
                        warnings.warn(f'Using deprecated option field {old_name} to '
                                      f'provide value for config field {option_name}',
                                      DeprecationWarning)
                        option_value = getattr(options, old_name)

            if env is not None:
                for env_var in env:
                    # Try all the environment variables
                    if option_value != default:
                        break
                    option_value = os.environ.get(env_var, default)

            if option_value is not None or not hasattr(self, option_name):
                if parsing_function is not None:
                    # Parse whatever it is (string, argparse-made list, etc.)
                    option_value = parsing_function(option_value)
                if check_function is not None:
                    try:
                        check_function(option_value)  # type: ignore
                    except AssertionError:
                        raise RuntimeError(f"The {option_name} option has an invalid value: {option_value}")
                setattr(self, option_name, option_value)

        # Function to parse integer from string expressed in different formats
        h2b = lambda x: human2bytes(str(x))

        def parse_jobstore(jobstore_uri: str) -> str:
            name, rest = Toil.parseLocator(jobstore_uri)
            if name == 'file':
                # We need to resolve relative paths early, on the leader, because the worker process
                # may have a different working directory than the leader, e.g. under Mesos.
                return Toil.buildLocator(name, os.path.abspath(rest))
            else:
                return jobstore_uri

        def parse_str_list(s: str) -> List[str]:
            return [str(x) for x in s.split(",")]

        def parse_int_list(s: str) -> List[int]:
            return [int(x) for x in s.split(",")]

        # Core options
        set_option("jobStore", parsing_function=parse_jobstore)
        # TODO: LOG LEVEL STRING
        set_option("workDir")
        if self.workDir is not None:
            self.workDir = os.path.abspath(self.workDir)
            if not os.path.exists(self.workDir):
                raise RuntimeError(f"The path provided to --workDir ({self.workDir}) does not exist.")

            if len(self.workDir) > 80:
                logger.warning(f'Length of workDir path "{self.workDir}" is {len(self.workDir)} characters.  '
                               f'Consider setting a shorter path with --workPath or setting TMPDIR to something '
                               f'like "/tmp" to avoid overly long paths.')
        set_option("coordination_dir")
        if self.coordination_dir is not None:
            self.coordination_dir = os.path.abspath(self.coordination_dir)
            if not os.path.exists(self.coordination_dir):
                raise RuntimeError(f"The path provided to --coordinationDir ({self.coordination_dir}) does not exist.")

        set_option("noStdOutErr")
        set_option("stats")
        set_option("cleanWorkDir")
        set_option("clean")
        if self.stats:
            if self.clean != "never" and self.clean is not None:
                raise RuntimeError("Contradicting options passed: Clean flag is set to %s "
                                   "despite the stats flag requiring "
                                   "the jobStore to be intact at the end of the run. "
                                   "Set clean to \'never\'" % self.clean)
            self.clean = "never"
        elif self.clean is None:
            self.clean = "onSuccess"
        set_option('clusterStats')
        set_option("restart")

        # Batch system options
        set_option("batchSystem")
        set_batchsystem_options(self.batchSystem, cast("OptionSetter", set_option))

        # File store options
        set_option("linkImports", bool, default=True)
        set_option("moveExports", bool, default=False)
        set_option("caching", bool, default=None)

        # Autoscaling options
        set_option("provisioner")
        set_option("nodeTypes", parse_node_types)
        set_option("minNodes", parse_int_list)
        set_option("maxNodes", parse_int_list)
        set_option("targetTime", int)
        if self.targetTime <= 0:
            raise RuntimeError(f'targetTime ({self.targetTime}) must be a positive integer!')
        set_option("betaInertia", float)
        if not 0.0 <= self.betaInertia <= 0.9:
            raise RuntimeError(f'betaInertia ({self.betaInertia}) must be between 0.0 and 0.9!')
        set_option("scaleInterval", float)
        set_option("metrics")
        set_option("assume_zero_overhead")
        set_option("preemptibleCompensation", float)
        if not 0.0 <= self.preemptibleCompensation <= 1.0:
            raise RuntimeError(f'preemptibleCompensation ({self.preemptibleCompensation}) must be between 0.0 and 1.0!')
        set_option("nodeStorage", int)

        def check_nodestoreage_overrides(overrides: List[str]) -> bool:
            for override in overrides:
                tokens = override.split(":")
                if len(tokens) != 2:
                    raise ValueError("Each component of --nodeStorageOverrides must be of the form <instance type>:<storage in GiB>")
                if not any(tokens[0] in n[0] for n in self.nodeTypes):
                    raise ValueError("Instance type in --nodeStorageOverrides must be in --nodeTypes")
                if not tokens[1].isdigit():
                    raise ValueError("storage must be an integer in --nodeStorageOverrides")
            return True
        set_option("nodeStorageOverrides", parse_str_list, check_function=check_nodestoreage_overrides)

        # Parameters to limit service jobs / detect deadlocks
        set_option("maxServiceJobs", int)
        set_option("maxPreemptibleServiceJobs", int)
        set_option("deadlockWait", int)
        set_option("deadlockCheckInterval", int)

        # Resource requirements
        set_option("defaultMemory", h2b, iC(1))
        set_option("defaultCores", float, fC(1.0))
        set_option("defaultDisk", h2b, iC(1))
        set_option("defaultAccelerators", parse_accelerator_list)
        set_option("readGlobalFileMutableByDefault")
        set_option("maxCores", int, iC(1))
        set_option("maxMemory", h2b, iC(1))
        set_option("maxDisk", h2b, iC(1))
        set_option("defaultPreemptible")

        # Retrying/rescuing jobs
        set_option("retryCount", int, iC(1))
        set_option("enableUnlimitedPreemptibleRetries")
        set_option("doubleMem")
        set_option("maxJobDuration", int, iC(1))
        set_option("rescueJobsFrequency", int, iC(1))

        # Log management
        set_option("maxLogFileSize", h2b, iC(1))
        set_option("writeLogs")
        set_option("writeLogsGzip")
        set_option("writeLogsFromAllJobs")
        set_option("write_messages", os.path.abspath)

        if not self.write_messages:
            # The user hasn't specified a place for the message bus so we
            # should make one.
            self.write_messages = gen_message_bus_path()

        assert not (self.writeLogs and self.writeLogsGzip), \
            "Cannot use both --writeLogs and --writeLogsGzip at the same time."
        assert not self.writeLogsFromAllJobs or self.writeLogs or self.writeLogsGzip, \
            "To enable --writeLogsFromAllJobs, either --writeLogs or --writeLogsGzip must be set."

        # Misc
        set_option("environment", parseSetEnv)
        set_option("disableChaining")
        set_option("disableJobStoreChecksumVerification")
        set_option("statusWait", int)
        set_option("disableProgress")

        def check_sse_key(sse_key: str) -> None:
            with open(sse_key) as f:
                assert len(f.readline().rstrip()) == 32, 'SSE key appears to be invalid.'

        set_option("sseKey", check_function=check_sse_key)
        set_option("servicePollingInterval", float, fC(0.0))
        set_option("forceDockerAppliance")

        # Debug options
        set_option("debugWorker")
        set_option("disableWorkerOutputCapture")
        set_option("badWorker", float, fC(0.0, 1.0))
        set_option("badWorkerFailInterval", float, fC(0.0))

    def __eq__(self, other: object) -> bool:
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return self.__dict__.__hash__()  # type: ignore


JOBSTORE_HELP = ("The location of the job store for the workflow.  "
                 "A job store holds persistent information about the jobs, stats, and files in a "
                 "workflow. If the workflow is run with a distributed batch system, the job "
                 "store must be accessible by all worker nodes. Depending on the desired "
                 "job store implementation, the location should be formatted according to "
                 "one of the following schemes:\n\n"
                 "file:<path> where <path> points to a directory on the file systen\n\n"
                 "aws:<region>:<prefix> where <region> is the name of an AWS region like "
                 "us-west-2 and <prefix> will be prepended to the names of any top-level "
                 "AWS resources in use by job store, e.g. S3 buckets.\n\n "
                 "google:<project_id>:<prefix> TODO: explain\n\n"
                 "For backwards compatibility, you may also specify ./foo (equivalent to "
                 "file:./foo or just file:foo) or /bar (equivalent to file:/bar).")


def parser_with_common_options(
    provisioner_options: bool = False, jobstore_option: bool = True
) -> ArgumentParser:
    parser = ArgumentParser(prog="Toil", formatter_class=ArgumentDefaultsHelpFormatter)

    if provisioner_options:
        add_provisioner_options(parser)

    if jobstore_option:
        parser.add_argument('jobStore', type=str, help=JOBSTORE_HELP)

    # always add these
    add_logging_options(parser)
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument("--tempDirRoot", dest="tempDirRoot", type=str, default=tempfile.gettempdir(),
                        help="Path to where temporary directory containing all temp files are created, "
                             "by default generates a fresh tmp dir with 'tempfile.gettempdir()'.")
    return parser


def addOptions(parser: ArgumentParser, config: Optional[Config] = None, jobstore_as_flag: bool = False) -> None:
    """
    Add Toil command line options to a parser.

    :param config: If specified, take defaults from the given Config.

    :param jobstore_as_flag: make the job store option a --jobStore flag instead of a required jobStore positional argument.
    """

    if config is None:
        config = Config()
    if not (isinstance(parser, ArgumentParser) or isinstance(parser, _ArgumentGroup)):
        raise ValueError(f"Unanticipated class: {parser.__class__}.  Must be: argparse.ArgumentParser or ArgumentGroup.")

    add_logging_options(parser)
    parser.register("type", "bool", parseBool)  # Custom type for arg=True/False.

    # Core options
    core_options = parser.add_argument_group(
        title="Toil core options.",
        description="Options to specify the location of the Toil workflow and "
                    "turn on stats collation about the performance of jobs."
    )
    if jobstore_as_flag:
        core_options.add_argument('--jobStore', '--jobstore', dest='jobStore', type=str, default=None, help=JOBSTORE_HELP)
    else:
        core_options.add_argument('jobStore', type=str, help=JOBSTORE_HELP)
    core_options.add_argument("--workDir", dest="workDir", default=None,
                              help="Absolute path to directory where temporary files generated during the Toil "
                                   "run should be placed. Standard output and error from batch system jobs "
                                   "(unless --noStdOutErr is set) will be placed in this directory. A cache directory "
                                   "may be placed in this directory. Temp files and folders will be placed in a "
                                   "directory toil-<workflowID> within workDir. The workflowID is generated by "
                                   "Toil and will be reported in the workflow logs. Default is determined by the "
                                   "variables (TMPDIR, TEMP, TMP) via mkdtemp. This directory needs to exist on "
                                   "all machines running jobs; if capturing standard output and error from batch "
                                   "system jobs is desired, it will generally need to be on a shared file system. "
                                   "When sharing a cache between containers on a host, this directory must be "
                                   "shared between the containers.")
    core_options.add_argument("--coordinationDir", dest="coordination_dir", default=None,
                              help="Absolute path to directory where Toil will keep state and lock files."
                                   "When sharing a cache between containers on a host, this directory must be "
                                   "shared between the containers.")
    core_options.add_argument("--noStdOutErr", dest="noStdOutErr", action="store_true", default=None,
                              help="Do not capture standard output and error from batch system jobs.")
    core_options.add_argument("--stats", dest="stats", action="store_true", default=None,
                              help="Records statistics about the toil workflow to be used by 'toil stats'.")
    clean_choices = ['always', 'onError', 'never', 'onSuccess']
    core_options.add_argument("--clean", dest="clean", choices=clean_choices, default=None,
                              help=f"Determines the deletion of the jobStore upon completion of the program.  "
                                   f"Choices: {clean_choices}.  The --stats option requires information from the "
                                   f"jobStore upon completion so the jobStore will never be deleted with that flag.  "
                                   f"If you wish to be able to restart the run, choose \'never\' or \'onSuccess\'.  "
                                   f"Default is \'never\' if stats is enabled, and \'onSuccess\' otherwise.")
    core_options.add_argument("--cleanWorkDir", dest="cleanWorkDir", choices=clean_choices, default='always',
                              help=f"Determines deletion of temporary worker directory upon completion of a job.  "
                                   f"Choices: {clean_choices}.  Default = always.  WARNING: This option should be "
                                   f"changed for debugging only.  Running a full pipeline with this option could "
                                   f"fill your disk with excessive intermediate data.")
    core_options.add_argument("--clusterStats", dest="clusterStats", nargs='?', action='store', default=None,
                              const=os.getcwd(),
                              help="If enabled, writes out JSON resource usage statistics to a file.  "
                                   "The default location for this file is the current working directory, but an "
                                   "absolute path can also be passed to specify where this file should be written. "
                                   "This options only applies when using scalable batch systems.")

    # Restarting the workflow options
    restart_options = parser.add_argument_group(
        title="Toil options for restarting an existing workflow.",
        description="Allows the restart of an existing workflow"
    )
    restart_options.add_argument("--restart", dest="restart", default=None, action="store_true",
                                 help="If --restart is specified then will attempt to restart existing workflow "
                                      "at the location pointed to by the --jobStore option. Will raise an exception "
                                      "if the workflow does not exist")

    # Batch system options
    batchsystem_options = parser.add_argument_group(
        title="Toil options for specifying the batch system.",
        description="Allows the specification of the batch system."
    )
    add_all_batchsystem_options(batchsystem_options)

    # File store options
    file_store_options = parser.add_argument_group(
        title="Toil options for configuring storage.",
        description="Allows configuring Toil's data storage."
    )
    link_imports = file_store_options.add_mutually_exclusive_group()
    link_imports_help = ("When using a filesystem based job store, CWL input files are by default symlinked in.  "
                         "Specifying this option instead copies the files into the job store, which may protect "
                         "them from being modified externally.  When not specified and as long as caching is enabled, "
                         "Toil will protect the file automatically by changing the permissions to read-only.")
    link_imports.add_argument("--linkImports", dest="linkImports", action='store_true', help=link_imports_help)
    link_imports.add_argument("--noLinkImports", dest="linkImports", action='store_false', help=link_imports_help)
    link_imports.set_defaults(linkImports=True)

    move_exports = file_store_options.add_mutually_exclusive_group()
    move_exports_help = ('When using a filesystem based job store, output files are by default moved to the '
                         'output directory, and a symlink to the moved exported file is created at the initial '
                         'location.  Specifying this option instead copies the files into the output directory.  '
                         'Applies to filesystem-based job stores only.')
    move_exports.add_argument("--moveExports", dest="moveExports", action='store_true', help=move_exports_help)
    move_exports.add_argument("--noMoveExports", dest="moveExports", action='store_false', help=move_exports_help)
    move_exports.set_defaults(moveExports=False)

    caching = file_store_options.add_mutually_exclusive_group()
    caching_help = ("Enable or disable caching for your workflow, specifying this overrides default from job store")
    caching.add_argument('--disableCaching', dest='caching', action='store_false', help=caching_help)
    caching.add_argument('--caching', dest='caching', type=lambda val: bool(strtobool(val)), help=caching_help)
    caching.set_defaults(caching=None)

    # Auto scaling options
    autoscaling_options = parser.add_argument_group(
        title="Toil options for autoscaling the cluster of worker nodes.",
        description="Allows the specification of the minimum and maximum number of nodes in an autoscaled cluster, "
                    "as well as parameters to control the level of provisioning."
    )
    provisioner_choices = ['aws', 'gce', None]
    # TODO: Better consolidate this provisioner arg and the one in provisioners/__init__.py?
    autoscaling_options.add_argument('--provisioner', '-p', dest="provisioner", choices=provisioner_choices,
                                     help=f"The provisioner for cluster auto-scaling.  This is the main Toil "
                                          f"'--provisioner' option, and defaults to None for running on single "
                                          f"machine and non-auto-scaling batch systems.  The currently supported "
                                          f"choices are {provisioner_choices}.  The default is {config.provisioner}.")
    autoscaling_options.add_argument('--nodeTypes', default=None,
                                     help="Specifies a list of comma-separated node types, each of which is "
                                          "composed of slash-separated instance types, and an optional spot "
                                          "bid set off by a colon, making the node type preemptible. Instance "
                                          "types may appear in multiple node types, and the same node type "
                                          "may appear as both preemptible and non-preemptible.\n"
                                          "Valid argument specifying two node types:\n"
                                          "\tc5.4xlarge/c5a.4xlarge:0.42,t2.large\n"
                                          "Node types:\n"
                                          "\tc5.4xlarge/c5a.4xlarge:0.42 and t2.large\n"
                                          "Instance types:\n"
                                          "\tc5.4xlarge, c5a.4xlarge, and t2.large\n"
                                          "Semantics:\n"
                                          "\tBid $0.42/hour for either c5.4xlarge or c5a.4xlarge instances,\n"
                                          "\ttreated interchangeably, while they are available at that price,\n"
                                          "\tand buy t2.large instances at full price")
    autoscaling_options.add_argument('--minNodes', default=None,
                                     help="Mininum number of nodes of each type in the cluster, if using "
                                          "auto-scaling.  This should be provided as a comma-separated list of the "
                                          "same length as the list of node types. default=0")
    autoscaling_options.add_argument('--maxNodes', default=None,
                                     help=f"Maximum number of nodes of each type in the cluster, if using autoscaling, "
                                          f"provided as a comma-separated list.  The first value is used as a default "
                                          f"if the list length is less than the number of nodeTypes.  "
                                          f"default={config.maxNodes[0]}")
    autoscaling_options.add_argument("--targetTime", dest="targetTime", default=None,
                                     help=f"Sets how rapidly you aim to complete jobs in seconds. Shorter times mean "
                                          f"more aggressive parallelization. The autoscaler attempts to scale up/down "
                                          f"so that it expects all queued jobs will complete within targetTime "
                                          f"seconds.  default={config.targetTime}")
    autoscaling_options.add_argument("--betaInertia", dest="betaInertia", default=None,
                                     help=f"A smoothing parameter to prevent unnecessary oscillations in the number "
                                          f"of provisioned nodes. This controls an exponentially weighted moving "
                                          f"average of the estimated number of nodes. A value of 0.0 disables any "
                                          f"smoothing, and a value of 0.9 will smooth so much that few changes will "
                                          f"ever be made.  Must be between 0.0 and 0.9.  default={config.betaInertia}")
    autoscaling_options.add_argument("--scaleInterval", dest="scaleInterval", default=None,
                                     help=f"The interval (seconds) between assessing if the scale of "
                                          f"the cluster needs to change. default={config.scaleInterval}")
    autoscaling_options.add_argument("--preemptibleCompensation", "--preemptableCompensation", dest="preemptibleCompensation", default=None,
                                     help=f"The preference of the autoscaler to replace preemptible nodes with "
                                          f"non-preemptible nodes, when preemptible nodes cannot be started for some "
                                          f"reason. Defaults to {config.preemptibleCompensation}. This value must be "
                                          f"between 0.0 and 1.0, inclusive.  A value of 0.0 disables such "
                                          f"compensation, a value of 0.5 compensates two missing preemptible nodes "
                                          f"with a non-preemptible one. A value of 1.0 replaces every missing "
                                          f"pre-emptable node with a non-preemptible one.")
    autoscaling_options.add_argument("--nodeStorage", dest="nodeStorage", default=50,
                                     help="Specify the size of the root volume of worker nodes when they are launched "
                                          "in gigabytes. You may want to set this if your jobs require a lot of disk "
                                          "space.  (default: %(default)s).")
    autoscaling_options.add_argument('--nodeStorageOverrides', default=None,
                                     help="Comma-separated list of nodeType:nodeStorage that are used to override "
                                          "the default value from --nodeStorage for the specified nodeType(s).  "
                                          "This is useful for heterogeneous jobs where some tasks require much more "
                                          "disk than others.")
    autoscaling_options.add_argument("--metrics", dest="metrics", default=False, action="store_true",
                                     help="Enable the prometheus/grafana dashboard for monitoring CPU/RAM usage, "
                                          "queue size, and issued jobs.")
    autoscaling_options.add_argument("--assumeZeroOverhead", dest="assume_zero_overhead", default=False, action="store_true",
                                     help="Ignore scheduler and OS overhead and assume jobs can use every last byte "
                                          "of memory and disk on a node when autoscaling.")

    # Parameters to limit service jobs / detect service deadlocks
    if not config.cwl:
        service_options = parser.add_argument_group(
            title="Toil options for limiting the number of service jobs and detecting service deadlocks",
            description="Allows the specification of the maximum number of service jobs in a cluster.  By keeping "
                        "this limited we can avoid nodes occupied with services causing deadlocks."
        )
        service_options.add_argument("--maxServiceJobs", dest="maxServiceJobs", default=None, type=int,
                                     help=f"The maximum number of service jobs that can be run concurrently, "
                                          f"excluding service jobs running on preemptible nodes.  "
                                          f"default={config.maxServiceJobs}")
        service_options.add_argument("--maxPreemptibleServiceJobs", dest="maxPreemptibleServiceJobs", default=None,
                                     type=int,
                                     help=f"The maximum number of service jobs that can run concurrently on "
                                          f"preemptible nodes.  default={config.maxPreemptibleServiceJobs}")
        service_options.add_argument("--deadlockWait", dest="deadlockWait", default=None, type=int,
                                     help=f"Time, in seconds, to tolerate the workflow running only the same service "
                                          f"jobs, with no jobs to use them, before declaring the workflow to be "
                                          f"deadlocked and stopping.  default={config.deadlockWait}")
        service_options.add_argument("--deadlockCheckInterval", dest="deadlockCheckInterval", default=None, type=int,
                                     help="Time, in seconds, to wait between checks to see if the workflow is stuck "
                                          "running only service jobs, with no jobs to use them. Should be shorter "
                                          "than --deadlockWait. May need to be increased if the batch system cannot "
                                          "enumerate running jobs quickly enough, or if polling for running jobs is "
                                          "placing an unacceptable load on a shared cluster.  "
                                          "default={config.deadlockCheckInterval}")

    # Resource requirements
    resource_options = parser.add_argument_group(
        title="Toil options for cores/memory requirements.",
        description="The options to specify default cores/memory requirements (if not specified by the jobs "
                    "themselves), and to limit the total amount of memory/cores requested from the batch system."
    )
    resource_help_msg = ('The {} amount of {} to request for a job.  '
                         'Only applicable to jobs that do not specify an explicit value for this requirement.  '
                         '{}.  '
                         'Default is {}.')
    cpu_note = 'Fractions of a core (for example 0.1) are supported on some batch systems [mesos, single_machine]'
    disk_mem_note = 'Standard suffixes like K, Ki, M, Mi, G or Gi are supported'
    accelerators_note = ('Each accelerator specification can have a type (gpu [default], nvidia, amd, cuda, rocm, opencl, '
                         'or a specific model like nvidia-tesla-k80), and a count [default: 1]. If both a type and a count '
                         'are used, they must be separated by a colon. If multiple types of accelerators are '
                         'used, the specifications are separated by commas')
    resource_options.add_argument('--defaultMemory', dest='defaultMemory', default=None, metavar='INT',
                                  help=resource_help_msg.format('default', 'memory', disk_mem_note, bytes2human(config.defaultMemory)))
    resource_options.add_argument('--defaultCores', dest='defaultCores', default=None, metavar='FLOAT',
                                  help=resource_help_msg.format('default', 'cpu', cpu_note, str(config.defaultCores)))
    resource_options.add_argument('--defaultDisk', dest='defaultDisk', default=None, metavar='INT',
                                  help=resource_help_msg.format('default', 'disk', disk_mem_note, bytes2human(config.defaultDisk)))
    resource_options.add_argument('--defaultAccelerators', dest='defaultAccelerators', default=None, metavar='ACCELERATOR[,ACCELERATOR...]',
                                  help=resource_help_msg.format('default', 'accelerators', accelerators_note, config.defaultAccelerators))
    resource_options.add_argument('--defaultPreemptible', '--defaultPreemptable', dest='defaultPreemptible', metavar='BOOL',
                                  type=bool, nargs='?', const=True, default=False,
                                  help='Make all jobs able to run on preemptible (spot) nodes by default.')
    resource_options.add_argument('--maxCores', dest='maxCores', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'cpu', cpu_note, str(config.maxCores)))
    resource_options.add_argument('--maxMemory', dest='maxMemory', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'memory', disk_mem_note, bytes2human(config.maxMemory)))
    resource_options.add_argument('--maxDisk', dest='maxDisk', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'disk', disk_mem_note, bytes2human(config.maxDisk)))

    # Retrying/rescuing jobs
    job_options = parser.add_argument_group(
        title="Toil options for rescuing/killing/restarting jobs.",
        description="The options for jobs that either run too long/fail or get lost (some batch systems have issues!)."
    )
    job_options.add_argument("--retryCount", dest="retryCount", default=None,
                             help=f"Number of times to retry a failing job before giving up and "
                                  f"labeling job failed. default={config.retryCount}")
    job_options.add_argument("--enableUnlimitedPreemptibleRetries", "--enableUnlimitedPreemptableRetries", dest="enableUnlimitedPreemptibleRetries",
                             action='store_true', default=False,
                             help="If set, preemptible failures (or any failure due to an instance getting "
                                  "unexpectedly terminated) will not count towards job failures and --retryCount.")
    job_options.add_argument("--doubleMem", dest="doubleMem", action='store_true', default=False,
                             help="If set, batch jobs which die to reaching memory limit on batch schedulers "
                                  "will have their memory doubled and they will be retried. The remaining "
                                  "retry count will be reduced by 1. Currently supported by LSF.")
    job_options.add_argument("--maxJobDuration", dest="maxJobDuration", default=None,
                             help=f"Maximum runtime of a job (in seconds) before we kill it (this is a lower bound, "
                                  f"and the actual time before killing the job may be longer).  "
                                  f"default={config.maxJobDuration}")
    job_options.add_argument("--rescueJobsFrequency", dest="rescueJobsFrequency", default=None,
                             help=f"Period of time to wait (in seconds) between checking for missing/overlong jobs, "
                                  f"that is jobs which get lost by the batch system. Expert parameter.  "
                                  f"default={config.rescueJobsFrequency}")

    # Log management options
    log_options = parser.add_argument_group(
        title="Toil log management options.",
        description="Options for how Toil should manage its logs."
    )
    log_options.add_argument("--maxLogFileSize", dest="maxLogFileSize", default=None,
                             help=f"The maximum size of a job log file to keep (in bytes), log files larger than "
                                  f"this will be truncated to the last X bytes. Setting this option to zero will "
                                  f"prevent any truncation. Setting this option to a negative value will truncate "
                                  f"from the beginning.  Default={bytes2human(config.maxLogFileSize)}")
    log_options.add_argument("--writeLogs", dest="writeLogs", nargs='?', action='store', default=None,
                             const=os.getcwd(),
                             help="Write worker logs received by the leader into their own files at the specified "
                                  "path. Any non-empty standard output and error from failed batch system jobs will "
                                  "also be written into files at this path.  The current working directory will be "
                                  "used if a path is not specified explicitly. Note: By default only the logs of "
                                  "failed jobs are returned to leader. Set log level to 'debug' or enable "
                                  "'--writeLogsFromAllJobs' to get logs back from successful jobs, and adjust "
                                  "'maxLogFileSize' to control the truncation limit for worker logs.")
    log_options.add_argument("--writeLogsGzip", dest="writeLogsGzip", nargs='?', action='store', default=None,
                             const=os.getcwd(),
                             help="Identical to --writeLogs except the logs files are gzipped on the leader.")
    log_options.add_argument("--writeLogsFromAllJobs", dest="writeLogsFromAllJobs", action='store_true',
                             default=False,
                             help="Whether to write logs from all jobs (including the successful ones) without "
                                  "necessarily setting the log level to 'debug'. Ensure that either --writeLogs "
                                  "or --writeLogsGzip is set if enabling this option.")
    log_options.add_argument("--writeMessages", dest="write_messages", default=None,
                             help="File to send messages from the leader's message bus to.")
    log_options.add_argument("--realTimeLogging", dest="realTimeLogging", action="store_true", default=False,
                             help="Enable real-time logging from workers to leader")

    # Misc options
    misc_options = parser.add_argument_group(
        title="Toil miscellaneous options.",
        description="Everything else."
    )
    misc_options.add_argument('--disableChaining', dest='disableChaining', action='store_true', default=False,
                              help="Disables chaining of jobs (chaining uses one job's resource allocation "
                                   "for its successor job if possible).")
    misc_options.add_argument("--disableJobStoreChecksumVerification", dest="disableJobStoreChecksumVerification",
                              default=False, action="store_true",
                              help="Disables checksum verification for files transferred to/from the job store.  "
                                   "Checksum verification is a safety check to ensure the data is not corrupted "
                                   "during transfer. Currently only supported for non-streaming AWS files.")
    misc_options.add_argument("--sseKey", dest="sseKey", default=None,
                              help="Path to file containing 32 character key to be used for server-side encryption on "
                                   "awsJobStore or googleJobStore. SSE will not be used if this flag is not passed.")
    misc_options.add_argument("--setEnv", '-e', metavar='NAME=VALUE or NAME', dest="environment", default=[],
                              action="append",
                              help="Set an environment variable early on in the worker. If VALUE is omitted, it will "
                                   "be looked up in the current environment. Independently of this option, the worker "
                                   "will try to emulate the leader's environment before running a job, except for "
                                   "some variables known to vary across systems.  Using this option, a variable can "
                                   "be injected into the worker process itself before it is started.")
    misc_options.add_argument("--servicePollingInterval", dest="servicePollingInterval", default=None,
                              help=f"Interval of time service jobs wait between polling for the existence of the "
                                   f"keep-alive flag.  Default: {config.servicePollingInterval}")
    misc_options.add_argument('--forceDockerAppliance', dest='forceDockerAppliance', action='store_true', default=False,
                              help='Disables sanity checking the existence of the docker image specified by '
                                   'TOIL_APPLIANCE_SELF, which Toil uses to provision mesos for autoscaling.')
    misc_options.add_argument('--statusWait', dest='statusWait', type=int, default=3600,
                              help="Seconds to wait between reports of running jobs.")
    misc_options.add_argument('--disableProgress', dest='disableProgress', action='store_true', default=False,
                              help="Disables the progress bar shown when standard error is a terminal.")

    # Debug options
    debug_options = parser.add_argument_group(
        title="Toil debug options.",
        description="Debug options for finding problems or helping with testing."
    )
    debug_options.add_argument("--debugWorker", default=False, action="store_true",
                               help="Experimental no forking mode for local debugging.  Specifically, workers "
                                    "are not forked and stderr/stdout are not redirected to the log.")
    debug_options.add_argument("--disableWorkerOutputCapture", default=False, action="store_true",
                               help="Let worker output go to worker's standard out/error instead of per-job logs.")
    debug_options.add_argument("--badWorker", dest="badWorker", default=None,
                               help=f"For testing purposes randomly kill --badWorker proportion of jobs using "
                                    f"SIGKILL.  default={config.badWorker}")
    debug_options.add_argument("--badWorkerFailInterval", dest="badWorkerFailInterval", default=None,
                               help=f"When killing the job pick uniformly within the interval from 0.0 to "
                                    f"--badWorkerFailInterval seconds after the worker starts.  "
                                    f"default={config.badWorkerFailInterval}")


def parseBool(val: str) -> bool:
    if val.lower() in ['true', 't', 'yes', 'y', 'on', '1']:
        return True
    elif val.lower() in ['false', 'f', 'no', 'n', 'off', '0']:
        return False
    else:
        raise RuntimeError("Could not interpret \"%s\" as a boolean value" % val)

@lru_cache(maxsize=None)
def getNodeID() -> str:
    """
    Return unique ID of the current node (host). The resulting string will be convertable to a uuid.UUID.

    Tries several methods until success. The returned ID should be identical across calls from different processes on
    the same node at least until the next OS reboot.

    The last resort method is uuid.getnode() that in some rare OS configurations may return a random ID each time it is
    called. However, this method should never be reached on a Linux system, because reading from
    /proc/sys/kernel/random/boot_id will be tried prior to that. If uuid.getnode() is reached, it will be called twice,
    and exception raised if the values are not identical.
    """
    for idSourceFile in ["/var/lib/dbus/machine-id", "/proc/sys/kernel/random/boot_id"]:
        if os.path.exists(idSourceFile):
            try:
                with open(idSourceFile) as inp:
                    nodeID = inp.readline().strip()
            except OSError:
                logger.warning(f"Exception when trying to read ID file {idSourceFile}.  "
                               f"Will try next method to get node ID.", exc_info=True)
            else:
                if len(nodeID.split()) == 1:
                    logger.debug(f"Obtained node ID {nodeID} from file {idSourceFile}")
                    break
                else:
                    logger.warning(f"Node ID {nodeID} from file {idSourceFile} contains spaces.  "
                                   f"Will try next method to get node ID.")
    else:
        nodeIDs = []
        for i_call in range(2):
            nodeID = str(uuid.getnode()).strip()
            if len(nodeID.split()) == 1:
                nodeIDs.append(nodeID)
            else:
                logger.warning(f"Node ID {nodeID} from uuid.getnode() contains spaces")
        nodeID = ""
        if len(nodeIDs) == 2:
            if nodeIDs[0] == nodeIDs[1]:
                nodeID = nodeIDs[0]
            else:
                logger.warning(f"Different node IDs {nodeIDs} received from repeated calls to uuid.getnode().  "
                               f"You should use another method to generate node ID.")

            logger.debug(f"Obtained node ID {nodeID} from uuid.getnode()")
    if not nodeID:
        logger.warning("Failed to generate stable node ID, returning empty string. If you see this message with a "
                       "work dir on a shared file system when using workers running on multiple nodes, you might "
                       "experience cryptic job failures")
    if len(nodeID.replace('-', '')) < UUID_LENGTH:
        # Some platforms (Mac) give us not enough actual hex characters.
        # Repeat them so the result is convertable to a uuid.UUID
        nodeID = nodeID.replace('-', '')
        num_repeats = UUID_LENGTH // len(nodeID) + 1
        nodeID = nodeID * num_repeats
        nodeID = nodeID[:UUID_LENGTH]
    return nodeID


class Toil(ContextManager["Toil"]):
    """
    A context manager that represents a Toil workflow.

    Specifically the batch system, job store, and its configuration.
    """
    config: Config
    _jobStore: "AbstractJobStore"
    _batchSystem: "AbstractBatchSystem"
    _provisioner: Optional["AbstractProvisioner"]

    def __init__(self, options: Namespace) -> None:
        """
        Initialize a Toil object from the given options.

        Note that this is very light-weight and that the bulk of the work is
        done when the context is entered.

        :param options: command line options specified by the user
        """
        super().__init__()
        self.options = options
        self._jobCache: Dict[Union[str, "TemporaryID"], "JobDescription"] = {}
        self._inContextManager: bool = False
        self._inRestart: bool = False

    def __enter__(self) -> "Toil":
        """
        Derive configuration from the command line options.

        Then load the job store and, on restart, consolidate the derived
        configuration with the one from the previous invocation of the workflow.
        """
        set_logging_from_options(self.options)
        config = Config()
        config.setOptions(self.options)
        jobStore = self.getJobStore(config.jobStore)
        if config.caching is None:
            config.caching = jobStore.default_caching()
            #Set the caching option because it wasn't set originally, resuming jobstore rebuilds config from CLI options
            self.options.caching = config.caching

        if not config.restart:
            config.prepare_start()
            jobStore.initialize(config)
        else:
            jobStore.resume()
            # Merge configuration from job store with command line options
            config = jobStore.config
            config.prepare_restart()
            config.setOptions(self.options)
            jobStore.write_config()
        self.config = config
        self._jobStore = jobStore
        self._inContextManager = True

        # This will make sure `self.__exit__()` is called when we get a SIGTERM signal.
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        """
        Clean up after a workflow invocation.

        Depending on the configuration, delete the job store.
        """
        try:
            if (exc_type is not None and self.config.clean == "onError" or
                    exc_type is None and self.config.clean == "onSuccess" or
                    self.config.clean == "always"):

                try:
                    if self.config.restart and not self._inRestart:
                        pass
                    else:
                        self._jobStore.destroy()
                        logger.info("Successfully deleted the job store: %s" % str(self._jobStore))
                except:
                    logger.info("Failed to delete the job store: %s" % str(self._jobStore))
                    raise
        except Exception as e:
            if exc_type is None:
                raise
            else:
                logger.exception('The following error was raised during clean up:')
        self._inContextManager = False
        self._inRestart = False
        return False  # let exceptions through

    def start(self, rootJob: "Job") -> Any:
        """
        Invoke a Toil workflow with the given job as the root for an initial run.

        This method must be called in the body of a ``with Toil(...) as toil:``
        statement. This method should not be called more than once for a workflow
        that has not finished.

        :param rootJob: The root job of the workflow
        :return: The root job's return value
        """
        self._assertContextManagerUsed()

        # Write shared files to the job store
        self._jobStore.write_leader_pid()
        self._jobStore.write_leader_node_id()

        if self.config.restart:
            raise ToilRestartException('A Toil workflow can only be started once. Use '
                                       'Toil.restart() to resume it.')

        self._batchSystem = self.createBatchSystem(self.config)
        self._setupAutoDeployment(rootJob.getUserScript())
        try:
            self._setBatchSystemEnvVars()
            self._serialiseEnv()
            self._cacheAllJobs()

            # Pickle the promised return value of the root job, then write the pickled promise to
            # a shared file, where we can find and unpickle it at the end of the workflow.
            # Unpickling the promise will automatically substitute the promise for the actual
            # return value.
            with self._jobStore.write_shared_file_stream('rootJobReturnValue') as fH:
                rootJob.prepareForPromiseRegistration(self._jobStore)
                promise = rootJob.rv()
                pickle.dump(promise, fH, protocol=pickle.HIGHEST_PROTOCOL)

            # Setup the first JobDescription and cache it
            rootJobDescription = rootJob.saveAsRootJob(self._jobStore)
            self._cacheJob(rootJobDescription)

            self._setProvisioner()
            return self._runMainLoop(rootJobDescription)
        finally:
            self._shutdownBatchSystem()

    def restart(self) -> Any:
        """
        Restarts a workflow that has been interrupted.

        :return: The root job's return value
        """
        self._inRestart = True
        self._assertContextManagerUsed()

        # Write shared files to the job store
        self._jobStore.write_leader_pid()
        self._jobStore.write_leader_node_id()

        if not self.config.restart:
            raise ToilRestartException('A Toil workflow must be initiated with Toil.start(), '
                                       'not restart().')

        from toil.job import JobException
        try:
            self._jobStore.load_root_job()
        except JobException:
            logger.warning(
                'Requested restart but the workflow has already been completed; allowing exports to rerun.')
            return self._jobStore.get_root_job_return_value()

        self._batchSystem = self.createBatchSystem(self.config)
        self._setupAutoDeployment()
        try:
            self._setBatchSystemEnvVars()
            self._serialiseEnv()
            self._cacheAllJobs()
            self._setProvisioner()
            rootJobDescription = self._jobStore.clean(jobCache=self._jobCache)
            return self._runMainLoop(rootJobDescription)
        finally:
            self._shutdownBatchSystem()

    def _setProvisioner(self) -> None:
        if self.config.provisioner is None:
            self._provisioner = None
        else:
            self._provisioner = cluster_factory(provisioner=self.config.provisioner,
                                                clusterName=None,
                                                zone=None,  # read from instance meta-data
                                                nodeStorage=self.config.nodeStorage,
                                                nodeStorageOverrides=self.config.nodeStorageOverrides,
                                                sseKey=self.config.sseKey)
            self._provisioner.setAutoscaledNodeTypes(self.config.nodeTypes)

    @classmethod
    def getJobStore(cls, locator: str) -> "AbstractJobStore":
        """
        Create an instance of the concrete job store implementation that matches the given locator.

        :param str locator: The location of the job store to be represent by the instance

        :return: an instance of a concrete subclass of AbstractJobStore
        """
        name, rest = cls.parseLocator(locator)
        if name == 'file':
            from toil.jobStores.fileJobStore import FileJobStore
            return FileJobStore(rest)
        elif name == 'aws':
            from toil.jobStores.aws.jobStore import AWSJobStore
            return AWSJobStore(rest)
        elif name == 'google':
            from toil.jobStores.googleJobStore import GoogleJobStore
            return GoogleJobStore(rest)
        else:
            raise RuntimeError("Unknown job store implementation '%s'" % name)

    @staticmethod
    def parseLocator(locator: str) -> Tuple[str, str]:
        if locator[0] in '/.' or ':' not in locator:
            return 'file', locator
        else:
            try:
                name, rest = locator.split(':', 1)
            except ValueError:
                raise RuntimeError('Invalid job store locator syntax.')
            else:
                return name, rest

    @staticmethod
    def buildLocator(name: str, rest: str) -> str:
        if ":" in name:
            raise ValueError(f"Can't have a ':' in the name: '{name}'.")
        return f'{name}:{rest}'

    @classmethod
    def resumeJobStore(cls, locator: str) -> "AbstractJobStore":
        jobStore = cls.getJobStore(locator)
        jobStore.resume()
        return jobStore

    @staticmethod
    def createBatchSystem(config: Config) -> "AbstractBatchSystem":
        """
        Create an instance of the batch system specified in the given config.

        :param config: the current configuration

        :return: an instance of a concrete subclass of AbstractBatchSystem
        """
        kwargs = dict(config=config,
                      maxCores=config.maxCores,
                      maxMemory=config.maxMemory,
                      maxDisk=config.maxDisk)

        from toil.batchSystems.registry import BATCH_SYSTEM_FACTORY_REGISTRY

        try:
            batch_system = BATCH_SYSTEM_FACTORY_REGISTRY[config.batchSystem]()
        except KeyError:
            raise RuntimeError(f'Unrecognized batch system: {config.batchSystem}  '
                               f'(choose from: {BATCH_SYSTEM_FACTORY_REGISTRY.keys()})')

        if config.caching and not batch_system.supportsWorkerCleanup():
            raise RuntimeError(f'{config.batchSystem} currently does not support shared caching, because it '
                               'does not support cleaning up a worker after the last job finishes. Set '
                               '--caching=false')

        logger.debug('Using the %s' % re.sub("([a-z])([A-Z])", r"\g<1> \g<2>", batch_system.__name__).lower())

        return batch_system(**kwargs)

    def _setupAutoDeployment(
        self, userScript: Optional["ModuleDescriptor"] = None
    ) -> None:
        """
        Determine the user script, save it to the job store and inject a reference to the saved copy into the batch system.

        Do it such that the batch system can auto-deploy the resource on the worker nodes.

        :param userScript: the module descriptor referencing the user script.
               If None, it will be looked up in the job store.
        """
        if userScript is not None:
            # This branch is hit when a workflow is being started
            if userScript.belongsToToil:
                logger.debug('User script %s belongs to Toil. No need to auto-deploy it.', userScript)
                userScript = None
            else:
                if (self._batchSystem.supportsAutoDeployment() and
                        not self.config.disableAutoDeployment):
                    # Note that by saving the ModuleDescriptor, and not the Resource we allow for
                    # redeploying a potentially modified user script on workflow restarts.
                    with self._jobStore.write_shared_file_stream('userScript') as f:
                        pickle.dump(userScript, f, protocol=pickle.HIGHEST_PROTOCOL)
                else:
                    from toil.batchSystems.singleMachine import \
                        SingleMachineBatchSystem
                    if not isinstance(self._batchSystem, SingleMachineBatchSystem):
                        logger.warning('Batch system does not support auto-deployment. The user script '
                                       '%s will have to be present at the same location on every worker.', userScript)
                    userScript = None
        else:
            # This branch is hit on restarts
            if self._batchSystem.supportsAutoDeployment() and not self.config.disableAutoDeployment:
                # We could deploy a user script
                from toil.jobStores.abstractJobStore import NoSuchFileException
                try:
                    with self._jobStore.read_shared_file_stream('userScript') as f:
                        userScript = safeUnpickleFromStream(f)
                except NoSuchFileException:
                    logger.debug('User script neither set explicitly nor present in the job store.')
                    userScript = None
        if userScript is None:
            logger.debug('No user script to auto-deploy.')
        else:
            logger.debug('Saving user script %s as a resource', userScript)
            userScriptResource = userScript.saveAsResourceTo(self._jobStore)  # type: ignore[misc]
            logger.debug('Injecting user script %s into batch system.', userScriptResource)
            self._batchSystem.setUserScript(userScriptResource)

    # Importing a file with a shared file name returns None, but without one it
    # returns a file ID. Explain this to MyPy.

    @overload
    def importFile(self,
                   srcUrl: str,
                   sharedFileName: str,
                   symlink: bool = True) -> None: ...

    @overload
    def importFile(self,
                   srcUrl: str,
                   sharedFileName: None = None,
                   symlink: bool = True) -> FileID: ...

    @deprecated(new_function_name='import_file')
    def importFile(self,
                   srcUrl: str,
                   sharedFileName: Optional[str] = None,
                   symlink: bool = True) -> Optional[FileID]:
        return self.import_file(srcUrl, sharedFileName, symlink)

    @overload
    def import_file(self,
                    src_uri: str,
                    shared_file_name: str,
                    symlink: bool = True,
                    check_existence: bool = True) -> None: ...

    @overload
    def import_file(self,
                    src_uri: str,
                    shared_file_name: None = None,
                    symlink: bool = True,
                    check_existence: bool = True) -> FileID: ...

    def import_file(self,
                    src_uri: str,
                    shared_file_name: Optional[str] = None,
                    symlink: bool = True,
                    check_existence: bool = True) -> Optional[FileID]:
        """
        Import the file at the given URL into the job store.

        By default, returns None if the file does not exist.

        :param check_existence: If true, raise FileNotFoundError if the file
               does not exist. If false, return None when the file does not
               exist.

        See :func:`toil.jobStores.abstractJobStore.AbstractJobStore.importFile` for a
        full description
        """
        self._assertContextManagerUsed()
        full_uri = self.normalize_uri(src_uri, check_existence=check_existence)
        try:
            imported = self._jobStore.import_file(full_uri, shared_file_name=shared_file_name, symlink=symlink)
        except FileNotFoundError:
            # TODO: I thought we refactored the different job store import
            # methods to not raise and instead return None, but that looks to
            # not currently be the case.
            if check_existence:
                raise
            else:
                # So translate the raise-based API if needed.
                # TODO: If check_existence is false but a shared file name is
                # specified, we have no way to report the lack of file
                # existence, since we also return None on success!
                return None
        if imported is None and shared_file_name is None and check_existence:
            # We need to protect the caller from missing files.
            # We think a file was missing, and we got None becasuse of it.
            # We didn't get None instead because of usign a shared file name.
            raise FileNotFoundError(f'Could not find file {src_uri}')
        return imported

    @deprecated(new_function_name='export_file')
    def exportFile(self, jobStoreFileID: FileID, dstUrl: str) -> None:
        return self.export_file(jobStoreFileID, dstUrl)

    def export_file(self, file_id: FileID, dst_uri: str) -> None:
        """
        Export file to destination pointed at by the destination URL.

        See :func:`toil.jobStores.abstractJobStore.AbstractJobStore.exportFile` for a
        full description
        """
        self._assertContextManagerUsed()
        dst_uri = self.normalize_uri(dst_uri)
        self._jobStore.export_file(file_id, dst_uri)

    @staticmethod
    def normalize_uri(uri: str, check_existence: bool = False) -> str:
        """
        Given a URI, if it has no scheme, prepend "file:".

        :param check_existence: If set, raise FileNotFoundError if a URI points to
               a local file that does not exist.
        """
        if urlparse(uri).scheme == 'file':
            uri = urlparse(uri).path  # this should strip off the local file scheme; it will be added back

        # account for the scheme-less case, which should be coerced to a local absolute path
        if urlparse(uri).scheme == '':
            abs_path = os.path.abspath(uri)
            if not os.path.exists(abs_path) and check_existence:
                raise FileNotFoundError(
                    f'Could not find local file "{abs_path}" when importing "{uri}".\n'
                    f'Make sure paths are relative to "{os.getcwd()}" or use absolute paths.\n'
                    f'If this is not a local file, please include the scheme (s3:/, gs:/, ftp://, etc.).')
            return f'file://{abs_path}'
        return uri

    def _setBatchSystemEnvVars(self) -> None:
        """Set the environment variables required by the job store and those passed on command line."""
        for envDict in (self._jobStore.get_env(), self.config.environment):
            for k, v in envDict.items():
                self._batchSystem.setEnv(k, v)

    def _serialiseEnv(self) -> None:
        """Put the environment in a globally accessible pickle file."""
        # Dump out the environment of this process in the environment pickle file.
        with self._jobStore.write_shared_file_stream("environment.pickle") as fileHandle:
            pickle.dump(dict(os.environ), fileHandle, pickle.HIGHEST_PROTOCOL)
        logger.debug("Written the environment for the jobs to the environment file")

    def _cacheAllJobs(self) -> None:
        """Download all jobs in the current job store into self.jobCache."""
        logger.debug('Caching all jobs in job store')
        self._jobCache = {jobDesc.jobStoreID: jobDesc for jobDesc in self._jobStore.jobs()}
        logger.debug(f'{len(self._jobCache)} jobs downloaded.')

    def _cacheJob(self, job: "JobDescription") -> None:
        """
        Add given job to current job cache.

        :param job: job to be added to current job cache
        """
        self._jobCache[job.jobStoreID] = job

    @staticmethod
    def getToilWorkDir(configWorkDir: Optional[str] = None) -> str:
        """
        Return a path to a writable directory under which per-workflow directories exist.

        This directory is always required to exist on a machine, even if the Toil
        worker has not run yet.  If your workers and leader have different temp
        directories, you may need to set TOIL_WORKDIR.

        :param configWorkDir: Value passed to the program using the --workDir flag
        :return: Path to the Toil work directory, constant across all machines
        """
        workDir = os.getenv('TOIL_WORKDIR_OVERRIDE') or configWorkDir or os.getenv('TOIL_WORKDIR') or tempfile.gettempdir()
        if not os.path.exists(workDir):
            raise RuntimeError(f'The directory specified by --workDir or TOIL_WORKDIR ({workDir}) does not exist.')
        return workDir

    @classmethod
    def get_toil_coordination_dir(cls, config_work_dir: Optional[str], config_coordination_dir: Optional[str]) -> str:
        """
        Return a path to a writable directory, which will be in memory if
        convenient. Ought to be used for file locking and coordination.

        :param config_work_dir: Value passed to the program using the
               --workDir flag
        :param config_coordination_dir: Value passed to the program using the
               --coordinationDir flag

        :return: Path to the Toil coordination directory. Ought to be on a
                 POSIX filesystem that allows directories containing open files to be
                 deleted.
        """

        if 'XDG_RUNTIME_DIR' in os.environ and not os.path.exists(os.environ['XDG_RUNTIME_DIR']):
            # Slurm has been observed providing this variable but not keeping
            # the directory live as long as we run for.
            logger.warning('XDG_RUNTIME_DIR is set to nonexistent directory %s; your environment may be out of spec!', os.environ['XDG_RUNTIME_DIR'])

        # Go get a coordination directory, using a lot of short-circuiting of
        # or and the fact that and returns its second argument when it
        # succeeds.
        coordination_dir: Optional[str] = (
            # First try an override env var
            os.getenv('TOIL_COORDINATION_DIR_OVERRIDE') or
            # Then the value from the config
            config_coordination_dir or
            # Then a normal env var
            # TODO: why/how would this propagate when not using single machine?
            os.getenv('TOIL_COORDINATION_DIR') or
            # Then try a `toil` subdirectory of the XDG runtime directory
            # (often /var/run/users/<UID>). But only if we are actually in a
            # session that has the env var set. Otherwise it might belong to a
            # different set of sessions and get cleaned up out from under us
            # when that session ends.
            # We don't think Slurm XDG sessions are trustworthy, depending on
            # the cluster's PAM configuration, so don't use them.
            ('XDG_RUNTIME_DIR' in os.environ and 'SLURM_JOBID' not in os.environ and try_path(os.path.join(os.environ['XDG_RUNTIME_DIR'], 'toil'))) or
            # Try under /run/lock. It might be a temp dir style sticky directory.
            try_path('/run/lock') or
            # Finally, fall back on the work dir and hope it's a legit filesystem.
            cls.getToilWorkDir(config_work_dir)
        )

        if coordination_dir is None:
            raise RuntimeError("Could not determine a coordination directory by any method!")

        return coordination_dir

    @staticmethod
    def _get_workflow_path_component(workflow_id: str) -> str:
        """
        Get a safe filesystem path component for a workflow.

        Will be consistent for all processes on a given machine, and different
        for all processes on different machines.

        :param workflow_id: The ID of the current Toil workflow.
        """
        return str(uuid.uuid5(uuid.UUID(getNodeID()), workflow_id)).replace('-', '')

    @classmethod
    def getLocalWorkflowDir(
        cls, workflowID: str, configWorkDir: Optional[str] = None
    ) -> str:
        """
        Return the directory where worker directories and the cache will be located for this workflow on this machine.

        :param configWorkDir: Value passed to the program using the --workDir flag
        :return: Path to the local workflow directory on this machine
        """
        # Get the global Toil work directory. This ensures that it exists.
        base = cls.getToilWorkDir(configWorkDir=configWorkDir)

        # Create a directory unique to each host in case workDir is on a shared FS.
        # This prevents workers on different nodes from erasing each other's directories.
        workflowDir: str = os.path.join(base, cls._get_workflow_path_component(workflowID))
        try:
            # Directory creation is atomic
            os.mkdir(workflowDir)
        except OSError as err:
            if err.errno != 17:
                # The directory exists if a previous worker set it up.
                raise
        else:
            logger.debug('Created the workflow directory for this machine at %s' % workflowDir)
        return workflowDir

    @classmethod
    def get_local_workflow_coordination_dir(
        cls,
        workflow_id: str,
        config_work_dir: Optional[str],
        config_coordination_dir: Optional[str]
    ) -> str:
        """
        Return the directory where coordination files should be located for
        this workflow on this machine. These include internal Toil databases
        and lock files for the machine.

        If an in-memory filesystem is available, it is used. Otherwise, the
        local workflow directory, which may be on a shared network filesystem,
        is used.

        :param workflow_id: Unique ID of the current workflow.
        :param config_work_dir: Value used for the work directory in the
               current Toil Config.
        :param config_coordination_dir: Value used for the coordination
               directory in the current Toil Config.

        :return: Path to the local workflow coordination directory on this
                 machine.
        """

        # Start with the base coordination or work dir
        base = cls.get_toil_coordination_dir(config_work_dir, config_coordination_dir)

        # Make a per-workflow and node subdirectory
        subdir = os.path.join(base, cls._get_workflow_path_component(workflow_id))
        # Make it exist
        os.makedirs(subdir, exist_ok=True)
        # TODO: May interfere with workflow directory creation logging if it's the same directory.
        # Return it
        return subdir

    def _runMainLoop(self, rootJob: "JobDescription") -> Any:
        """
        Run the main loop with the given job.

        :param rootJob: The root job for the workflow.
        """
        logProcessContext(self.config)

        with RealtimeLogger(self._batchSystem,
                            level=self.options.logLevel if self.options.realTimeLogging else None):
            # FIXME: common should not import from leader
            from toil.leader import Leader
            return Leader(config=self.config,
                          batchSystem=self._batchSystem,
                          provisioner=self._provisioner,
                          jobStore=self._jobStore,
                          rootJob=rootJob,
                          jobCache=self._jobCache).run()

    def _shutdownBatchSystem(self) -> None:
        """Shuts down current batch system if it has been created."""
        startTime = time.time()
        logger.debug('Shutting down batch system ...')
        self._batchSystem.shutdown()
        logger.debug('... finished shutting down the batch system in %s seconds.'
                     % (time.time() - startTime))

    def _assertContextManagerUsed(self) -> None:
        if not self._inContextManager:
            raise ToilContextManagerException()


class ToilRestartException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class ToilContextManagerException(Exception):
    def __init__(self) -> None:
        super().__init__(
            'This method cannot be called outside the "with Toil(...)" context manager.')


class ToilMetrics:
    def __init__(self, bus: MessageBus, provisioner: Optional["AbstractProvisioner"] = None) -> None:
        clusterName = "none"
        region = "us-west-2"
        if provisioner is not None:
            clusterName = str(provisioner.clusterName)
            if provisioner._zone is not None:
                if provisioner.cloud == 'aws':
                    # Remove AZ name
                    region = zone_to_region(provisioner._zone)
                else:
                    region = provisioner._zone

        registry = lookupEnvVar(name='docker registry',
                                envName='TOIL_DOCKER_REGISTRY',
                                defaultValue=dockerRegistry)

        self.mtailImage = f"{registry}/toil-mtail:{dockerTag}"
        self.grafanaImage = f"{registry}/toil-grafana:{dockerTag}"
        self.prometheusImage = f"{registry}/toil-prometheus:{dockerTag}"

        self.startDashboard(clusterName=clusterName, zone=region)

        # Always restart the mtail container, because metrics should start from scratch
        # for each workflow
        try:
            subprocess.check_call(["docker", "rm", "-f", "toil_mtail"])
        except subprocess.CalledProcessError:
            pass

        try:
            self.mtailProc: Optional[subprocess.Popen[bytes]] = subprocess.Popen(
                ["docker", "run",
                 "--rm",
                 "--interactive",
                 "--net=host",
                 "--name", "toil_mtail",
                 "-p", "3903:3903",
                 self.mtailImage],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError:
            logger.warning("Couldn't start toil metrics server.")
            self.mtailProc = None
        except KeyboardInterrupt:
            self.mtailProc.terminate()  # type: ignore[union-attr]

        # On single machine, launch a node exporter instance to monitor CPU/RAM usage.
        # On AWS this is handled by the EC2 init script
        self.nodeExporterProc: Optional[subprocess.Popen[bytes]] = None
        if not provisioner:
            try:
                self.nodeExporterProc = subprocess.Popen(
                    ["docker", "run",
                     "--rm",
                     "--net=host",
                     "-p", "9100:9100",
                     "-v", "/proc:/host/proc",
                     "-v", "/sys:/host/sys",
                     "-v", "/:/rootfs",
                     "quay.io/prometheus/node-exporter:v1.3.1",
                     "-collector.procfs", "/host/proc",
                     "-collector.sysfs", "/host/sys",
                     "-collector.filesystem.ignored-mount-points",
                     "^/(sys|proc|dev|host|etc)($|/)"])
            except subprocess.CalledProcessError:
                logger.warning("Couldn't start node exporter, won't get RAM and CPU usage for dashboard.")
            except KeyboardInterrupt:
                if self.nodeExporterProc is not None:
                    self.nodeExporterProc.terminate()

        # When messages come in on the message bus, call our methods.
        # TODO: Just annotate the methods with some kind of @listener and get
        # their argument types and magically register them?
        # TODO: There's no way to tell MyPy we have a dict from types to
        # functions that take them.
        TARGETS = {
            ClusterSizeMessage: self.logClusterSize,
            ClusterDesiredSizeMessage: self.logClusterDesiredSize,
            QueueSizeMessage: self.logQueueSize,
            JobMissingMessage: self.logMissingJob,
            JobIssuedMessage: self.logIssuedJob,
            JobFailedMessage: self.logFailedJob,
            JobCompletedMessage: self.logCompletedJob
        }
        # The only way to make this inteligible to MyPy is to wrap the dict in
        # a function that can cast.
        MessageType = TypeVar('MessageType')
        def get_listener(message_type: Type[MessageType]) -> Callable[[MessageType], None]:
            return cast(Callable[[MessageType], None], TARGETS[message_type])
        # Then set up the listeners.
        self._listeners = [bus.subscribe(message_type, get_listener(message_type)) for message_type in TARGETS.keys()]

    @staticmethod
    def _containerRunning(containerName: str) -> bool:
        try:
            result = subprocess.check_output(["docker", "inspect", "-f",
                                              "'{{.State.Running}}'", containerName]).decode('utf-8') == "true"
        except subprocess.CalledProcessError:
            result = False
        return result

    def startDashboard(self, clusterName: str, zone: str) -> None:
        try:
            if not self._containerRunning("toil_prometheus"):
                try:
                    subprocess.check_call(["docker", "rm", "-f", "toil_prometheus"])
                except subprocess.CalledProcessError:
                    pass
                subprocess.check_call(["docker", "run",
                                       "--name", "toil_prometheus",
                                       "--net=host",
                                       "-d",
                                       "-p", "9090:9090",
                                       self.prometheusImage,
                                       clusterName,
                                       zone])

            if not self._containerRunning("toil_grafana"):
                try:
                    subprocess.check_call(["docker", "rm", "-f", "toil_grafana"])
                except subprocess.CalledProcessError:
                    pass
                subprocess.check_call(["docker", "run",
                                       "--name", "toil_grafana",
                                       "-d", "-p=3000:3000",
                                       self.grafanaImage])
        except subprocess.CalledProcessError:
            logger.warning("Could not start prometheus/grafana dashboard.")
            return

        try:
            self.add_prometheus_data_source()
        except requests.exceptions.ConnectionError:
            logger.debug("Could not add data source to Grafana dashboard - no metrics will be displayed.")

    @retry(errors=[requests.exceptions.ConnectionError])
    def add_prometheus_data_source(self) -> None:
        requests.post(
            'http://localhost:3000/api/datasources',
            auth=('admin', 'admin'),
            data='{"name":"DS_PROMETHEUS","type":"prometheus", "url":"http://localhost:9090", "access":"direct"}',
            headers={'content-type': 'application/json', "access": "direct"}
        )

    def log(self, message: str) -> None:
        if self.mtailProc:
            self.mtailProc.stdin.write((message + "\n").encode("utf-8"))  # type: ignore[union-attr]
            self.mtailProc.stdin.flush()  # type: ignore[union-attr]

    # Note: The mtail configuration (dashboard/mtail/toil.mtail) depends on these messages
    # remaining intact

    def logClusterSize(
        self, m: ClusterSizeMessage
    ) -> None:
        self.log("current_size '%s' %i" % (m.instance_type, m.current_size))

    def logClusterDesiredSize(
        self, m: ClusterDesiredSizeMessage
    ) -> None:
        self.log("desired_size '%s' %i" % (m.instance_type, m.desired_size))

    def logQueueSize(self, m: QueueSizeMessage) -> None:
        self.log("queue_size %i" % m.queue_size)

    def logMissingJob(self, m: JobMissingMessage) -> None:
        self.log("missing_job")

    def logIssuedJob(self, m: JobIssuedMessage) -> None:
        self.log("issued_job %s" % m.job_type)

    def logFailedJob(self, m: JobFailedMessage) -> None:
        self.log("failed_job %s" % m.job_type)

    def logCompletedJob(self, m: JobCompletedMessage) -> None:
        self.log("completed_job %s" % m.job_type)

    def shutdown(self) -> None:
        if self.mtailProc is not None:
            logger.debug('Stopping mtail')
            self.mtailProc.kill()
            logger.debug('Stopped mtail')
        if self.nodeExporterProc is not None:
            logger.debug('Stopping node exporter')
            self.nodeExporterProc.kill()
            logger.debug('Stopped node exporter')
        self._listeners = []


def parseSetEnv(l: List[str]) -> Dict[str, Optional[str]]:
    """
    Parse a list of strings of the form "NAME=VALUE" or just "NAME" into a dictionary.

    Strings of the latter from will result in dictionary entries whose value is None.

    >>> parseSetEnv([])
    {}
    >>> parseSetEnv(['a'])
    {'a': None}
    >>> parseSetEnv(['a='])
    {'a': ''}
    >>> parseSetEnv(['a=b'])
    {'a': 'b'}
    >>> parseSetEnv(['a=a', 'a=b'])
    {'a': 'b'}
    >>> parseSetEnv(['a=b', 'c=d'])
    {'a': 'b', 'c': 'd'}
    >>> parseSetEnv(['a=b=c'])
    {'a': 'b=c'}
    >>> parseSetEnv([''])
    Traceback (most recent call last):
    ...
    ValueError: Empty name
    >>> parseSetEnv(['=1'])
    Traceback (most recent call last):
    ...
    ValueError: Empty name
    """
    d = {}
    v: Optional[str] = None
    for i in l:
        try:
            k, v = i.split('=', 1)
        except ValueError:
            k, v = i, None
        if not k:
            raise ValueError('Empty name')
        d[k] = v
    return d


def iC(minValue: int, maxValue: int = SYS_MAX_SIZE) -> Callable[[int], bool]:
    """Returns a function that checks if a given int is in the given half-open interval."""
    assert isinstance(minValue, int) and isinstance(maxValue, int)
    return lambda x: minValue <= x < maxValue


def fC(minValue: float, maxValue: Optional[float] = None) -> Callable[[float], bool]:
    """Returns a function that checks if a given float is in the given half-open interval."""
    assert isinstance(minValue, float)
    if maxValue is None:
        return lambda x: minValue <= x
    assert isinstance(maxValue, float)
    return lambda x: minValue <= x < maxValue  # type: ignore

def parse_accelerator_list(specs: Optional[str]) -> List['AcceleratorRequirement']:
    """
    Parse a string description of one or more accelerator requirements.
    """

    if specs is None or len(specs) == 0:
        # Not specified, so the default default is to not need any.
        return []
    # Otherwise parse each requirement.
    from toil.job import parse_accelerator

    return [parse_accelerator(r) for r in specs.split(',')]


def cacheDirName(workflowID: str) -> str:
    """
    :return: Name of the cache directory.
    """
    return f'cache-{workflowID}'


def getDirSizeRecursively(dirPath: str) -> int:
    """
    This method will return the cumulative number of bytes occupied by the files
    on disk in the directory and its subdirectories.

    If the method is unable to access a file or directory (due to insufficient
    permissions, or due to the file or directory having been removed while this
    function was attempting to traverse it), the error will be handled
    internally, and a (possibly 0) lower bound on the size of the directory
    will be returned.

    The environment variable 'BLOCKSIZE'='512' is set instead of the much cleaner
    --block-size=1 because Apple can't handle it.

    :param str dirPath: A valid path to a directory or file.
    :return: Total size, in bytes, of the file or directory at dirPath.
    """

    # du is often faster than using os.lstat(), sometimes significantly so.

    # The call: 'du -s /some/path' should give the number of 512-byte blocks
    # allocated with the environment variable: BLOCKSIZE='512' set, and we
    # multiply this by 512 to return the filesize in bytes.

    try:
        return int(subprocess.check_output(['du', '-s', dirPath],
                                           env=dict(os.environ, BLOCKSIZE='512')).decode('utf-8').split()[0]) * 512
    except subprocess.CalledProcessError:
        # Something was inaccessible or went away
        return 0


def getFileSystemSize(dirPath: str) -> Tuple[int, int]:
    """
    Return the free space, and total size of the file system hosting `dirPath`.

    :param dirPath: A valid path to a directory.
    :return: free space and total size of file system
    """
    if not os.path.exists(dirPath):
        raise RuntimeError(f'Could not find dir size for non-existent path: {dirPath}')
    diskStats = os.statvfs(dirPath)
    freeSpace = diskStats.f_frsize * diskStats.f_bavail
    diskSize = diskStats.f_frsize * diskStats.f_blocks
    return freeSpace, diskSize


def safeUnpickleFromStream(stream: IO[Any]) -> Any:
    string = stream.read()
    return pickle.loads(string)
