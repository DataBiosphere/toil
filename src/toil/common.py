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
import logging
import os
import pickle
import re
import requests
import subprocess
import sys
import tempfile
import time
import uuid

from argparse import _ArgumentGroup, ArgumentParser, ArgumentDefaultsHelpFormatter
from typing import Optional, Callable, Any, List, Tuple, Union

from toil import logProcessContext, lookupEnvVar
from toil.batchSystems.options import (add_all_batchsystem_options,
                                       set_batchsystem_config_defaults,
                                       set_batchsystem_options)
from toil.lib.conversions import human2bytes, bytes2human
from toil.lib.retry import retry
from toil.provisioners import (add_provisioner_options,
                               parse_node_types,
                               check_valid_node_types,
                               cluster_factory)
from toil.lib.aws import zone_to_region
from toil.realtimeLogger import RealtimeLogger
from toil.statsAndLogging import (add_logging_options,
                                  root_logger,
                                  set_logging_from_options)
from toil.version import dockerRegistry, dockerTag, version

# aim to pack autoscaling jobs within a 30 minute block before provisioning a new node
defaultTargetTime = 1800
UUID_LENGTH = 32
logger = logging.getLogger(__name__)


class Config:
    """Class to represent configuration operations for a toil workflow run."""
    def __init__(self):
        # Core options
        self.workflowID: Optional[str] = None
        """This attribute uniquely identifies the job store and therefore the workflow. It is
        necessary in order to distinguish between two consecutive workflows for which
        self.jobStore is the same, e.g. when a job store name is reused after a previous run has
        finished successfully and its job store has been clean up."""
        self.workflowAttemptNumber = None
        self.jobStore = None
        self.logLevel: str = logging.getLevelName(root_logger.getEffectiveLevel())
        self.workDir: Optional[str] = None
        self.noStdOutErr: bool = False
        self.stats: bool = False

        # Because the stats option needs the jobStore to persist past the end of the run,
        # the clean default value depends the specified stats option and is determined in setOptions
        self.clean = None
        self.cleanWorkDir: Optional[bool] = None
        self.clusterStats = None

        # Restarting the workflow options
        self.restart: bool = False

        # Batch system options
        set_batchsystem_config_defaults(self)

        # Autoscaling options
        self.provisioner = None
        self.nodeTypes = []
        self.minNodes = None
        self.maxNodes = [10]
        self.targetTime = defaultTargetTime
        self.betaInertia = 0.1
        self.scaleInterval = 60
        self.preemptableCompensation = 0.0
        self.nodeStorage = 50
        self.nodeStorageOverrides = []
        self.metrics: bool = False

        # Parameters to limit service jobs, so preventing deadlock scheduling scenarios
        self.maxPreemptableServiceJobs: int = sys.maxsize
        self.maxServiceJobs: int = sys.maxsize
        self.deadlockWait: Union[float, int] = 60  # Number of seconds we must be stuck with all services before declaring a deadlock
        self.deadlockCheckInterval: Union[float, int] = 30  # Minimum polling delay for deadlocks
        self.statePollingWait: Union[float, int] = 1  # Number of seconds to wait before querying job state

        # Resource requirements
        self.defaultMemory: int = 2147483648
        self.defaultCores: Union[float, int] = 1
        self.defaultDisk: int = 2147483648
        self.readGlobalFileMutableByDefault: bool = False
        self.defaultPreemptable: bool = False
        self.maxCores: int = sys.maxsize
        self.maxMemory: int = sys.maxsize
        self.maxDisk: int = sys.maxsize

        # Retrying/rescuing jobs
        self.retryCount: int = 1
        self.enableUnlimitedPreemptableRetries: bool = False
        self.doubleMem: bool = False
        self.maxJobDuration: int = sys.maxsize
        self.rescueJobsFrequency: int = 3600

        # Misc
        self.disableCaching: bool = False
        self.disableChaining: bool = False
        self.disableJobStoreChecksumVerification: bool = False
        self.maxLogFileSize: int = 64000
        self.writeLogs = None
        self.writeLogsGzip = None
        self.writeLogsFromAllJobs: bool = False
        self.sseKey: str = None
        self.servicePollingInterval: int = 60
        self.useAsync: bool = True
        self.forceDockerAppliance: bool = False
        self.runCwlInternalJobsOnWorkers: bool = False
        self.statusWait: int = 3600
        self.disableProgress: bool = False

        # Debug options
        self.debugWorker: bool = False
        self.disableWorkerOutputCapture: bool = False
        self.badWorker = 0.0
        self.badWorkerFailInterval = 0.01

        # CWL
        self.cwl: bool = False

    def setOptions(self, options) -> None:
        """Creates a config object from the options object."""
        def set_option(option_name: str,
                       parsing_function: Optional[Callable] = None,
                       check_function: Optional[Callable] = None,
                       default: Any = None) -> None:
            option_value = getattr(options, option_name, default)

            if option_value is not None:
                if parsing_function is not None:
                    option_value = parsing_function(option_value)
                if check_function is not None:
                    try:
                        check_function(option_value)
                    except AssertionError:
                        raise RuntimeError(f"The {option_name} option has an invalid value: {option_value}")
                setattr(self, option_name, option_value)

        # Function to parse integer from string expressed in different formats
        h2b = lambda x: human2bytes(str(x))

        def parse_jobstore(jobstore_uri: str):
            name, rest = Toil.parseLocator(jobstore_uri)
            if name == 'file':
                # We need to resolve relative paths early, on the leader, because the worker process
                # may have a different working directory than the leader, e.g. under Mesos.
                return Toil.buildLocator(name, os.path.abspath(rest))
            else:
                return jobstore_uri

        def parse_str_list(s: str):
            return [str(x) for x in s.split(",")]

        def parse_int_list(s: str):
            return [int(x) for x in s.split(",")]

        # Core options
        set_option("jobStore", parsing_function=parse_jobstore)
        # TODO: LOG LEVEL STRING
        set_option("workDir")
        if self.workDir is not None:
            self.workDir: Optional[str] = os.path.abspath(self.workDir)
            if not os.path.exists(self.workDir):
                raise RuntimeError(f"The path provided to --workDir ({self.workDir}) does not exist.")

            if len(self.workDir) > 80:
                logger.warning(f'Length of workDir path "{self.workDir}" is {len(self.workDir)} characters.  '
                               f'Consider setting a shorter path with --workPath or setting TMPDIR to something '
                               f'like "/tmp" to avoid overly long paths.')

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
        set_batchsystem_options(self.batchSystem, set_option)
        set_option("disableAutoDeployment")
        set_option("scale", float, fC(0.0))
        set_option("parasolCommand")
        set_option("parasolMaxBatches", int, iC(1))
        set_option("linkImports")
        set_option("moveExports")
        set_option("allocate_mem")
        set_option("mesosMasterAddress")
        set_option("kubernetesHostPath")
        set_option("environment", parseSetEnv)

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
        set_option("preemptableCompensation", float)
        if not 0.0 <= self.preemptableCompensation <= 1.0:
            raise RuntimeError(f'preemptableCompensation ({self.preemptableCompensation}) must be between 0.0 and 1.0!')
        set_option("nodeStorage", int)

        def check_nodestoreage_overrides(overrides: List[str]) -> None:
            for override in overrides:
                tokens = override.split(":")
                assert len(tokens) == 2, \
                    'Each component of --nodeStorageOverrides must be of the form <instance type>:<storage in GiB>'
                assert any(tokens[0] in n[0] for n in self.nodeTypes), \
                    'instance type in --nodeStorageOverrides must be used in --nodeTypes'
                assert tokens[1].isdigit(), \
                    'storage must be an integer in --nodeStorageOverrides'
        set_option("nodeStorageOverrides", parse_str_list, check_function=check_nodestoreage_overrides)

        # Parameters to limit service jobs / detect deadlocks
        set_option("maxServiceJobs", int)
        set_option("maxPreemptableServiceJobs", int)
        set_option("deadlockWait", int)
        set_option("deadlockCheckInterval", int)
        set_option("statePollingWait", int)

        # Resource requirements
        set_option("defaultMemory", h2b, iC(1))
        set_option("defaultCores", float, fC(1.0))
        set_option("defaultDisk", h2b, iC(1))
        set_option("readGlobalFileMutableByDefault")
        set_option("maxCores", int, iC(1))
        set_option("maxMemory", h2b, iC(1))
        set_option("maxDisk", h2b, iC(1))
        set_option("defaultPreemptable")

        # Retrying/rescuing jobs
        set_option("retryCount", int, iC(1))
        set_option("enableUnlimitedPreemptableRetries")
        set_option("doubleMem")
        set_option("maxJobDuration", int, iC(1))
        set_option("rescueJobsFrequency", int, iC(1))

        # Misc
        set_option("maxLocalJobs", int)
        set_option("disableCaching")
        set_option("disableChaining")
        set_option("disableJobStoreChecksumVerification")
        set_option("maxLogFileSize", h2b, iC(1))
        set_option("writeLogs")
        set_option("writeLogsGzip")
        set_option("writeLogsFromAllJobs")
        set_option("runCwlInternalJobsOnWorkers")
        set_option("disableProgress")

        assert not (self.writeLogs and self.writeLogsGzip), \
            "Cannot use both --writeLogs and --writeLogsGzip at the same time."
        assert not self.writeLogsFromAllJobs or self.writeLogs or self.writeLogsGzip, \
            "To enable --writeLogsFromAllJobs, either --writeLogs or --writeLogsGzip must be set."

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

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return self.__dict__.__hash__()


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


def parser_with_common_options(provisioner_options=False, jobstore_option=True):
    parser = ArgumentParser(prog='Toil', formatter_class=ArgumentDefaultsHelpFormatter)

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


def addOptions(parser: ArgumentParser, config: Config = Config()):
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
    core_options.add_argument('jobStore', type=str, help=JOBSTORE_HELP)
    core_options.add_argument("--workDir", dest="workDir", default=None,
                              help="Absolute path to directory where temporary files generated during the Toil "
                                   "run should be placed. Standard output and error from batch system jobs "
                                   "(unless --noStdOutErr) will be placed in this directory. A cache directory "
                                   "may be placed in this directory. Temp files and folders will be placed in a "
                                   "directory toil-<workflowID> within workDir. The workflowID is generated by "
                                   "Toil and will be reported in the workflow logs. Default is determined by the "
                                   "variables (TMPDIR, TEMP, TMP) via mkdtemp. This directory needs to exist on "
                                   "all machines running jobs; if capturing standard output and error from batch "
                                   "system jobs is desired, it will generally need to be on a shared file system. "
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
    batchsystem_options.add_argument("--statePollingWait", dest="statePollingWait", default=1, type=int,
                                     help="Time, in seconds, to wait before doing a scheduler query for job state.  "
                                          "Return cached results if within the waiting period.")
    add_all_batchsystem_options(batchsystem_options)

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
                                          "bid set off by a colon, making the node type preemptable. Instance "
                                          "types may appear in multiple node types, and the same node type "
                                          "may appear as both preemptable and non-preemptable.\n"
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
    autoscaling_options.add_argument("--preemptableCompensation", dest="preemptableCompensation", default=None,
                                     help=f"The preference of the autoscaler to replace preemptable nodes with "
                                          f"non-preemptable nodes, when preemptable nodes cannot be started for some "
                                          f"reason. Defaults to {config.preemptableCompensation}. This value must be "
                                          f"between 0.0 and 1.0, inclusive.  A value of 0.0 disables such "
                                          f"compensation, a value of 0.5 compensates two missing preemptable nodes "
                                          f"with a non-preemptable one. A value of 1.0 replaces every missing "
                                          f"pre-emptable node with a non-preemptable one.")
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

    # Parameters to limit service jobs / detect service deadlocks
    if not config.cwl:
        service_options = parser.add_argument_group(
            title="Toil options for limiting the number of service jobs and detecting service deadlocks",
            description="Allows the specification of the maximum number of service jobs in a cluster.  By keeping "
                        "this limited we can avoid nodes occupied with services causing deadlocks."
        )
        service_options.add_argument("--maxServiceJobs", dest="maxServiceJobs", default=None, type=int,
                                     help=f"The maximum number of service jobs that can be run concurrently, "
                                          f"excluding service jobs running on preemptable nodes.  "
                                          f"default={config.maxServiceJobs}")
        service_options.add_argument("--maxPreemptableServiceJobs", dest="maxPreemptableServiceJobs", default=None,
                                     type=int,
                                     help=f"The maximum number of service jobs that can run concurrently on "
                                          f"preemptable nodes.  default={config.maxPreemptableServiceJobs}")
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
    resource_options.add_argument('--defaultMemory', dest='defaultMemory', default=None, metavar='INT',
                                  help=resource_help_msg.format('default', 'memory', disk_mem_note,
                                                                bytes2human(config.defaultMemory)))
    resource_options.add_argument('--defaultCores', dest='defaultCores', default=None, metavar='FLOAT',
                                  help=resource_help_msg.format('default', 'cpu', cpu_note, str(config.defaultCores)))
    resource_options.add_argument('--defaultDisk', dest='defaultDisk', default=None, metavar='INT',
                                  help=resource_help_msg.format('default', 'disk', disk_mem_note,
                                                                bytes2human(config.defaultDisk)))
    resource_options.add_argument('--defaultPreemptable', dest='defaultPreemptable', metavar='BOOL',
                                  type='bool', nargs='?', const=True, default=False,
                                  help='Make all jobs able to run on preemptable (spot) nodes by default.')
    resource_options.add_argument('--maxCores', dest='maxCores', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'cpu', cpu_note, str(config.maxCores)))
    resource_options.add_argument('--maxMemory', dest='maxMemory', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'memory', disk_mem_note,
                                                                bytes2human(config.maxMemory)))
    resource_options.add_argument('--maxDisk', dest='maxDisk', default=None, metavar='INT',
                                  help=resource_help_msg.format('max', 'disk', disk_mem_note,
                                                                bytes2human(config.maxDisk)))

    # Retrying/rescuing jobs
    job_options = parser.add_argument_group(
        title="Toil options for rescuing/killing/restarting jobs.",
        description="The options for jobs that either run too long/fail or get lost (some batch systems have issues!)."
    )
    job_options.add_argument("--retryCount", dest="retryCount", default=None,
                             help=f"Number of times to retry a failing job before giving up and "
                                  f"labeling job failed. default={config.retryCount}")
    job_options.add_argument("--enableUnlimitedPreemptableRetries", dest="enableUnlimitedPreemptableRetries",
                             action='store_true', default=False,
                             help="If set, preemptable failures (or any failure due to an instance getting "
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

    # Misc options
    misc_options = parser.add_argument_group(
        title="Toil miscellaneous options.",
        description="Everything else."
    )
    misc_options.add_argument('--disableCaching', dest='disableCaching', type='bool', nargs='?', const=True,
                              default=False,
                              help='Disables caching in the file store. This flag must be set to use '
                                   'a batch system that does not support cleanup, such as Parasol.')
    misc_options.add_argument('--disableChaining', dest='disableChaining', action='store_true', default=False,
                              help="Disables chaining of jobs (chaining uses one job's resource allocation "
                                   "for its successor job if possible).")
    misc_options.add_argument("--disableJobStoreChecksumVerification", dest="disableJobStoreChecksumVerification",
                              default=False, action="store_true",
                              help="Disables checksum verification for files transferred to/from the job store.  "
                                   "Checksum verification is a safety check to ensure the data is not corrupted "
                                   "during transfer. Currently only supported for non-streaming AWS files.")
    misc_options.add_argument("--maxLogFileSize", dest="maxLogFileSize", default=None,
                              help=f"The maximum size of a job log file to keep (in bytes), log files larger than "
                                   f"this will be truncated to the last X bytes. Setting this option to zero will "
                                   f"prevent any truncation. Setting this option to a negative value will truncate "
                                   f"from the beginning.  Default={bytes2human(config.maxLogFileSize)}")
    misc_options.add_argument("--writeLogs", dest="writeLogs", nargs='?', action='store', default=None,
                              const=os.getcwd(),
                              help="Write worker logs received by the leader into their own files at the specified "
                                   "path. Any non-empty standard output and error from failed batch system jobs will "
                                   "also be written into files at this path.  The current working directory will be "
                                   "used if a path is not specified explicitly. Note: By default only the logs of "
                                   "failed jobs are returned to leader. Set log level to 'debug' or enable "
                                   "'--writeLogsFromAllJobs' to get logs back from successful jobs, and adjust "
                                   "'maxLogFileSize' to control the truncation limit for worker logs.")
    misc_options.add_argument("--writeLogsGzip", dest="writeLogsGzip", nargs='?', action='store', default=None,
                              const=os.getcwd(),
                              help="Identical to --writeLogs except the logs files are gzipped on the leader.")
    misc_options.add_argument("--writeLogsFromAllJobs", dest="writeLogsFromAllJobs", action='store_true',
                              default=False,
                              help="Whether to write logs from all jobs (including the successful ones) without "
                                   "necessarily setting the log level to 'debug'. Ensure that either --writeLogs "
                                   "or --writeLogsGzip is set if enabling this option.")
    misc_options.add_argument("--realTimeLogging", dest="realTimeLogging", action="store_true", default=False,
                              help="Enable real-time logging from workers to masters")
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
    misc_options.add_argument('--disableProgress', dest='disableProgress', action='store_true', default=False,
                              help="Disables the progress bar shown when standard error is a terminal.")


def parseBool(val):
    if val.lower() in ['true', 't', 'yes', 'y', 'on', '1']:
        return True
    elif val.lower() in ['false', 'f', 'no', 'n', 'off', '0']:
        return False
    else:
        raise RuntimeError("Could not interpret \"%s\" as a boolean value" % val)


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
                with open(idSourceFile, "r") as inp:
                    nodeID = inp.readline().strip()
            except EnvironmentError:
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


class Toil:
    """
    A context manager that represents a Toil workflow, specifically the batch system, job store,
    and its configuration.
    """

    def __init__(self, options):
        """
        Initialize a Toil object from the given options. Note that this is very light-weight and
        that the bulk of the work is done when the context is entered.

        :param argparse.Namespace options: command line options specified by the user
        """
        super(Toil, self).__init__()
        self.options = options
        self.config = None
        """
        :type: toil.common.Config
        """
        self._jobStore = None
        """
        :type: toil.jobStores.abstractJobStore.AbstractJobStore
        """
        self._batchSystem = None
        """
        :type: toil.batchSystems.abstractBatchSystem.AbstractBatchSystem
        """
        self._provisioner = None
        """
        :type: toil.provisioners.abstractProvisioner.AbstractProvisioner
        """
        self._jobCache = dict()
        self._inContextManager = False
        self._inRestart = False

    def __enter__(self):
        """
        Derive configuration from the command line options, load the job store and, on restart,
        consolidate the derived configuration with the one from the previous invocation of the
        workflow.
        """
        set_logging_from_options(self.options)
        config = Config()
        config.setOptions(self.options)
        jobStore = self.getJobStore(config.jobStore)
        if not config.restart:
            config.workflowAttemptNumber = 0
            jobStore.initialize(config)
        else:
            jobStore.resume()
            # Merge configuration from job store with command line options
            config = jobStore.config
            config.setOptions(self.options)
            config.workflowAttemptNumber += 1
            jobStore.writeConfig()
        self.config = config
        self._jobStore = jobStore
        self._inContextManager = True
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up after a workflow invocation. Depending on the configuration, delete the job store.
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

    def start(self, rootJob):
        """
        Invoke a Toil workflow with the given job as the root for an initial run. This method
        must be called in the body of a ``with Toil(...) as toil:`` statement. This method should
        not be called more than once for a workflow that has not finished.

        :param toil.job.Job rootJob: The root job of the workflow
        :return: The root job's return value
        """
        self._assertContextManagerUsed()
        self.writePIDFile()
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
            with self._jobStore.writeSharedFileStream('rootJobReturnValue') as fH:
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

    def restart(self):
        """
        Restarts a workflow that has been interrupted.

        :return: The root job's return value
        """
        self._inRestart = True
        self._assertContextManagerUsed()
        self.writePIDFile()
        if not self.config.restart:
            raise ToilRestartException('A Toil workflow must be initiated with Toil.start(), '
                                       'not restart().')

        from toil.job import JobException
        try:
            self._jobStore.loadRootJob()
        except JobException:
            logger.warning(
                'Requested restart but the workflow has already been completed; allowing exports to rerun.')
            return self._jobStore.getRootJobReturnValue()

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

    def _setProvisioner(self):
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
    def getJobStore(cls, locator):
        """
        Create an instance of the concrete job store implementation that matches the given locator.

        :param str locator: The location of the job store to be represent by the instance

        :return: an instance of a concrete subclass of AbstractJobStore
        :rtype: toil.jobStores.abstractJobStore.AbstractJobStore
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
    def parseLocator(locator):
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
    def buildLocator(name, rest):
        assert ':' not in name
        return f'{name}:{rest}'

    @classmethod
    def resumeJobStore(cls, locator):
        jobStore = cls.getJobStore(locator)
        jobStore.resume()
        return jobStore

    @staticmethod
    def createBatchSystem(config):
        """
        Creates an instance of the batch system specified in the given config.

        :param toil.common.Config config: the current configuration

        :rtype: batchSystems.abstractBatchSystem.AbstractBatchSystem

        :return: an instance of a concrete subclass of AbstractBatchSystem
        """
        kwargs = dict(config=config,
                      maxCores=config.maxCores,
                      maxMemory=config.maxMemory,
                      maxDisk=config.maxDisk)

        from toil.batchSystems.registry import BATCH_SYSTEM_FACTORY_REGISTRY

        try:
            batch_system = BATCH_SYSTEM_FACTORY_REGISTRY[config.batchSystem]()
        except:
            raise RuntimeError(f'Unrecognized batch system: {config.batchSystem}')

        if not config.disableCaching and not batch_system.supportsWorkerCleanup():
            raise RuntimeError(f'{config.batchSystem} currently does not support shared caching, because it '
                               'does not support cleaning up a worker after the last job '
                               'finishes. Set the --disableCaching flag if you want to '
                               'use this batch system.')
        logger.debug('Using the %s' % re.sub("([a-z])([A-Z])", r"\g<1> \g<2>", batch_system.__name__).lower())

        return batch_system(**kwargs)

    def _setupAutoDeployment(self, userScript=None):
        """
        Determine the user script, save it to the job store and inject a reference to the saved
        copy into the batch system such that it can auto-deploy the resource on the worker
        nodes.

        :param toil.resource.ModuleDescriptor userScript: the module descriptor referencing the
               user script. If None, it will be looked up in the job store.
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
                    with self._jobStore.writeSharedFileStream('userScript') as f:
                        pickle.dump(userScript, f, protocol=pickle.HIGHEST_PROTOCOL)
                else:
                    from toil.batchSystems.singleMachine import \
                        SingleMachineBatchSystem
                    if not isinstance(self._batchSystem, SingleMachineBatchSystem):
                        logger.warning('Batch system does not support auto-deployment. The user '
                                    'script %s will have to be present at the same location on '
                                    'every worker.', userScript)
                    userScript = None
        else:
            # This branch is hit on restarts
            if (self._batchSystem.supportsAutoDeployment() and
                not self.config.disableAutoDeployment):
                # We could deploy a user script
                from toil.jobStores.abstractJobStore import NoSuchFileException
                try:
                    with self._jobStore.readSharedFileStream('userScript') as f:
                        userScript = safeUnpickleFromStream(f)
                except NoSuchFileException:
                    logger.debug('User script neither set explicitly nor present in the job store.')
                    userScript = None
        if userScript is None:
            logger.debug('No user script to auto-deploy.')
        else:
            logger.debug('Saving user script %s as a resource', userScript)
            userScriptResource = userScript.saveAsResourceTo(self._jobStore)
            logger.debug('Injecting user script %s into batch system.', userScriptResource)
            self._batchSystem.setUserScript(userScriptResource)

    def importFile(self, srcUrl, sharedFileName=None, symlink=False):
        """
        Imports the file at the given URL into job store.

        See :func:`toil.jobStores.abstractJobStore.AbstractJobStore.importFile` for a
        full description
        """
        self._assertContextManagerUsed()
        return self._jobStore.importFile(srcUrl, sharedFileName=sharedFileName, symlink=symlink)

    def exportFile(self, jobStoreFileID, dstUrl):
        """
        Exports file to destination pointed at by the destination URL.

        See :func:`toil.jobStores.abstractJobStore.AbstractJobStore.exportFile` for a
        full description
        """
        self._assertContextManagerUsed()
        self._jobStore.exportFile(jobStoreFileID, dstUrl)

    def _setBatchSystemEnvVars(self):
        """
        Sets the environment variables required by the job store and those passed on command line.
        """
        for envDict in (self._jobStore.getEnv(), self.config.environment):
            for k, v in envDict.items():
                self._batchSystem.setEnv(k, v)

    def _serialiseEnv(self):
        """
        Puts the environment in a globally accessible pickle file.
        """
        # Dump out the environment of this process in the environment pickle file.
        with self._jobStore.writeSharedFileStream("environment.pickle") as fileHandle:
            pickle.dump(dict(os.environ), fileHandle, pickle.HIGHEST_PROTOCOL)
        logger.debug("Written the environment for the jobs to the environment file")

    def _cacheAllJobs(self):
        """
        Downloads all jobs in the current job store into self.jobCache.
        """
        logger.debug('Caching all jobs in job store')
        self._jobCache = {jobDesc.jobStoreID: jobDesc for jobDesc in self._jobStore.jobs()}
        logger.debug('{} jobs downloaded.'.format(len(self._jobCache)))

    def _cacheJob(self, job):
        """
        Adds given job to current job cache.

        :param toil.job.JobDescription job: job to be added to current job cache
        """
        self._jobCache[job.jobStoreID] = job

    @staticmethod
    def getToilWorkDir(configWorkDir: Optional[str] = None) -> str:
        """
        Returns a path to a writable directory under which per-workflow
        directories exist.  This directory is always required to exist on a
        machine, even if the Toil worker has not run yet.  If your workers and
        leader have different temp directories, you may need to set
        TOIL_WORKDIR.

        :param str configWorkDir: Value passed to the program using the --workDir flag
        :return: Path to the Toil work directory, constant across all machines
        :rtype: str
        """
        workDir = os.getenv('TOIL_WORKDIR_OVERRIDE') or configWorkDir or os.getenv('TOIL_WORKDIR') or tempfile.gettempdir()
        if not os.path.exists(workDir):
            raise RuntimeError(f'The directory specified by --workDir or TOIL_WORKDIR ({workDir}) does not exist.')
        return workDir

    @classmethod
    def getLocalWorkflowDir(cls, workflowID, configWorkDir=None):
        """
        Returns a path to the directory where worker directories and the cache will be located
        for this workflow on this machine.

        :param str configWorkDir: Value passed to the program using the --workDir flag
        :return: Path to the local workflow directory on this machine
        :rtype: str
        """
        # Get the global Toil work directory. This ensures that it exists.
        base = cls.getToilWorkDir(configWorkDir=configWorkDir)

        # Create a directory unique to each host in case workDir is on a shared FS.
        # This prevents workers on different nodes from erasing each other's directories.
        workflowDir: str = os.path.join(base, str(uuid.uuid5(uuid.UUID(getNodeID()), workflowID)).replace('-', ''))
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

    def _runMainLoop(self, rootJob):
        """
        Runs the main loop with the given job.
        :param toil.job.Job rootJob: The root job for the workflow.
        :rtype: Any
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

    def _shutdownBatchSystem(self):
        """
        Shuts down current batch system if it has been created.
        """
        assert self._batchSystem is not None

        startTime = time.time()
        logger.debug('Shutting down batch system ...')
        self._batchSystem.shutdown()
        logger.debug('... finished shutting down the batch system in %s seconds.'
                     % (time.time() - startTime))

    def _assertContextManagerUsed(self):
        if not self._inContextManager:
            raise ToilContextManagerException()

    def writePIDFile(self):
        """
        Write a the pid of this process to a file in the jobstore.

        Overwriting the current contents of pid.log is a feature, not a bug of this method.
        Other methods will rely on always having the most current pid available.
        So far there is no reason to store any old pids.
        """
        with self._jobStore.writeSharedFileStream('pid.log') as f:
            f.write(str(os.getpid()).encode('utf-8'))


class ToilRestartException(Exception):
    def __init__(self, message):
        super(ToilRestartException, self).__init__(message)


class ToilContextManagerException(Exception):
    def __init__(self):
        super(ToilContextManagerException, self).__init__(
            'This method cannot be called outside the "with Toil(...)" context manager.')


class ToilMetrics:
    def __init__(self, provisioner=None):
        clusterName = 'none'
        region = 'us-west-2'
        if provisioner is not None:
            clusterName = provisioner.clusterName
            if provisioner._zone is not None:
                if provisioner.cloud == 'aws':
                    # Remove AZ name
                    region = zone_to_region(provisioner._zone)
                else:
                    region = provisioner._zone

        registry = lookupEnvVar(name='docker registry',
                                envName='TOIL_DOCKER_REGISTRY',
                                defaultValue=dockerRegistry)

        self.mtailImage = "%s/toil-mtail:%s" % (registry, dockerTag)
        self.grafanaImage = "%s/toil-grafana:%s" % (registry, dockerTag)
        self.prometheusImage = "%s/toil-prometheus:%s" % (registry, dockerTag)

        self.startDashboard(clusterName=clusterName, zone=region)

        # Always restart the mtail container, because metrics should start from scratch
        # for each workflow
        try:
            subprocess.check_call(["docker", "rm", "-f", "toil_mtail"])
        except subprocess.CalledProcessError:
            pass

        try:
            self.mtailProc = subprocess.Popen(["docker", "run", "--rm", "--interactive",
                                               "--net=host",
                                               "--name", "toil_mtail",
                                               "-p", "3903:3903",
                                               self.mtailImage],
                                              stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError:
            logger.warning("Could not start toil metrics server.")
            self.mtailProc = None
        except KeyboardInterrupt:
            self.mtailProc.terminate()

        # On single machine, launch a node exporter instance to monitor CPU/RAM usage.
        # On AWS this is handled by the EC2 init script
        self.nodeExporterProc = None
        if not provisioner:
            try:
                self.nodeExporterProc = subprocess.Popen(["docker", "run", "--rm",
                                                          "--net=host",
                                                          "-p", "9100:9100",
                                                          "-v", "/proc:/host/proc",
                                                          "-v", "/sys:/host/sys",
                                                          "-v", "/:/rootfs",
                                                          "quay.io/prometheus/node-exporter:0.15.2",
                                                          "-collector.procfs", "/host/proc",
                                                          "-collector.sysfs", "/host/sys",
                                                          "-collector.filesystem.ignored-mount-points",
                                                          "^/(sys|proc|dev|host|etc)($|/)"])
            except subprocess.CalledProcessError:
                logger.warning(
                    "Couldn't start node exporter, won't get RAM and CPU usage for dashboard.")
                self.nodeExporterProc = None
            except KeyboardInterrupt:
                self.nodeExporterProc.terminate()

    @staticmethod
    def _containerRunning(containerName):
        try:
            result = subprocess.check_output(["docker", "inspect", "-f",
                                              "'{{.State.Running}}'", containerName]).decode('utf-8') == "true"
        except subprocess.CalledProcessError:
            result = False
        return result

    def startDashboard(self, clusterName, zone):
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
    def add_prometheus_data_source(self):
        requests.post(
            'http://localhost:3000/api/datasources',
            auth=('admin', 'admin'),
            data='{"name":"DS_PROMETHEUS","type":"prometheus", "url":"http://localhost:9090", "access":"direct"}',
            headers={'content-type': 'application/json', "access": "direct"}
        )

    def log(self, message):
        if self.mtailProc:
            self.mtailProc.stdin.write((message + "\n").encode("utf-8"))
            self.mtailProc.stdin.flush()

    # Note: The mtail configuration (dashboard/mtail/toil.mtail) depends on these messages
    # remaining intact

    def logMissingJob(self):
        self.log("missing_job")

    def logClusterSize(self, nodeType, currentSize, desiredSize):
        self.log("current_size '%s' %i" % (nodeType, currentSize))
        self.log("desired_size '%s' %i" % (nodeType, desiredSize))

    def logQueueSize(self, queueSize):
        self.log("queue_size %i" % queueSize)

    def logIssuedJob(self, jobType):
        self.log("issued_job %s" % jobType)

    def logFailedJob(self, jobType):
        self.log("failed_job %s" % jobType)

    def logCompletedJob(self, jobType):
        self.log("completed_job %s" % jobType)

    def shutdown(self):
        if self.mtailProc:
            self.mtailProc.kill()
        if self.nodeExporterProc:
            self.nodeExporterProc.kill()


def parseSetEnv(l):
    """
    Parses a list of strings of the form "NAME=VALUE" or just "NAME" into a dictionary. Strings
    of the latter from will result in dictionary entries whose value is None.

    :type l: list[str]
    :rtype: dict[str,str]

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
    d = dict()
    for i in l:
        try:
            k, v = i.split('=', 1)
        except ValueError:
            k, v = i, None
        if not k:
            raise ValueError('Empty name')
        d[k] = v
    return d


def iC(minValue, maxValue=sys.maxsize):
    # Returns function that checks if a given int is in the given half-open interval
    assert isinstance(minValue, int) and isinstance(maxValue, int)
    return lambda x: minValue <= x < maxValue


def fC(minValue, maxValue=None):
    # Returns function that checks if a given float is in the given half-open interval
    assert isinstance(minValue, float)
    if maxValue is None:
        return lambda x: minValue <= x
    else:
        assert isinstance(maxValue, float)
        return lambda x: minValue <= x < maxValue


def cacheDirName(workflowID):
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

    :param str dirPath: A valid path to a directory.
    :return: free space and total size of file system
    :rtype: tuple
    """
    assert os.path.exists(dirPath)
    diskStats = os.statvfs(dirPath)
    freeSpace = diskStats.f_frsize * diskStats.f_bavail
    diskSize = diskStats.f_frsize * diskStats.f_blocks
    return freeSpace, diskSize


def safeUnpickleFromStream(stream):
    string = stream.read()
    return pickle.loads(string)
