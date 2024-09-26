import os
from argparse import ArgumentParser, Action, _AppendAction
from typing import Any, Optional, Union, Type, Callable, List, Dict, TYPE_CHECKING

from configargparse import SUPPRESS
import logging

from ruamel.yaml import YAML

from toil.lib.conversions import bytes2human, human2bytes, strtobool, opt_strtobool

from toil.batchSystems.options import add_all_batchsystem_options
from toil.provisioners import parse_node_types
from toil.statsAndLogging import add_logging_options
if TYPE_CHECKING:
    from toil.job import AcceleratorRequirement

logger = logging.getLogger(__name__)

# aim to pack autoscaling jobs within a 30 minute block before provisioning a new node
defaultTargetTime = 1800
SYS_MAX_SIZE = 9223372036854775807
# sys.max_size on 64 bit systems is 9223372036854775807, so that 32-bit systems
# use the same number

def parse_set_env(l: List[str]) -> Dict[str, Optional[str]]:
    """
    Parse a list of strings of the form "NAME=VALUE" or just "NAME" into a dictionary.

    Strings of the latter from will result in dictionary entries whose value is None.

    >>> parse_set_env([])
    {}
    >>> parse_set_env(['a'])
    {'a': None}
    >>> parse_set_env(['a='])
    {'a': ''}
    >>> parse_set_env(['a=b'])
    {'a': 'b'}
    >>> parse_set_env(['a=a', 'a=b'])
    {'a': 'b'}
    >>> parse_set_env(['a=b', 'c=d'])
    {'a': 'b', 'c': 'd'}
    >>> parse_set_env(['a=b=c'])
    {'a': 'b=c'}
    >>> parse_set_env([''])
    Traceback (most recent call last):
    ...
    ValueError: Empty name
    >>> parse_set_env(['=1'])
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


def parse_str_list(s: str) -> List[str]:
    return [str(x) for x in s.split(",")]


def parse_int_list(s: str) -> List[int]:
    return [int(x) for x in s.split(",")]


def iC(min_value: int, max_value: Optional[int] = None) -> Callable[[int], bool]:
    """Returns a function that checks if a given int is in the given half-open interval."""
    assert isinstance(min_value, int)
    if max_value is None:
        return lambda x: min_value <= x
    assert isinstance(max_value, int)
    return lambda x: min_value <= x < max_value


def fC(minValue: float, maxValue: Optional[float] = None) -> Callable[[float], bool]:
    """Returns a function that checks if a given float is in the given half-open interval."""
    assert isinstance(minValue, float)
    if maxValue is None:
        return lambda x: minValue <= x
    assert isinstance(maxValue, float)
    return lambda x: minValue <= x < maxValue


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


def parseBool(val: str) -> bool:
    if val.lower() in ['true', 't', 'yes', 'y', 'on', '1']:
        return True
    elif val.lower() in ['false', 'f', 'no', 'n', 'off', '0']:
        return False
    else:
        raise RuntimeError("Could not interpret \"%s\" as a boolean value" % val)


# This is kept in the outer scope as multiple batchsystem files use this
def make_open_interval_action(min: Union[int, float], max: Optional[Union[int, float]] = None) -> Type[Action]:
    """
    Returns an argparse action class to check if the input is within the given half-open interval.
    ex:
    Provided value to argparse must be within the interval [min, max)
    Types of min and max must be the same (max may be None)

    :param min: float/int
    :param max: optional float/int
    :return: argparse action class
    """

    class IntOrFloatOpenAction(Action):
        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            if isinstance(min, int):
                if max is not None:  # for mypy
                    assert isinstance(max, int)
                func = iC(min, max)
            else:
                func = fC(min, max)
            try:
                if not func(values):
                    if max is None:
                        raise parser.error(
                            f"{option_string} ({values}) must be at least {min}"
                        )
                    else:
                        raise parser.error(
                            f"{option_string} ({values}) must be at least {min} and strictly less than {max})"
                        )
            except AssertionError:
                raise RuntimeError(f"The {option_string} option has an invalid value: {values}")
            setattr(namespace, self.dest, values)

    return IntOrFloatOpenAction


def parse_jobstore(jobstore_uri: str) -> str:
    """
    Turn the jobstore string into it's corresponding URI
    ex:
    /path/to/jobstore -> file:/path/to/jobstore

    If the jobstore string already is a URI, return the jobstore:
    aws:/path/to/jobstore -> aws:/path/to/jobstore
    :param jobstore_uri: string of the jobstore
    :return: URI of the jobstore
    """
    from toil.common import Toil
    name, rest = Toil.parseLocator(jobstore_uri)
    if name == 'file':
        # We need to resolve relative paths early, on the leader, because the worker process
        # may have a different working directory than the leader, e.g. under Mesos.
        return Toil.buildLocator(name, os.path.abspath(rest))
    else:
        return jobstore_uri


JOBSTORE_HELP = ("The location of the job store for the workflow.  "
                 "A job store holds persistent information about the jobs, stats, and files in a "
                 "workflow. If the workflow is run with a distributed batch system, the job "
                 "store must be accessible by all worker nodes. Depending on the desired "
                 "job store implementation, the location should be formatted according to "
                 "one of the following schemes:\n\n"
                 "file:<path> where <path> points to a directory on the file system\n\n"
                 "aws:<region>:<prefix> where <region> is the name of an AWS region like "
                 "us-west-2 and <prefix> will be prepended to the names of any top-level "
                 "AWS resources in use by job store, e.g. S3 buckets.\n\n "
                 "google:<project_id>:<prefix> TODO: explain\n\n"
                 "For backwards compatibility, you may also specify ./foo (equivalent to "
                 "file:./foo or just file:foo) or /bar (equivalent to file:/bar).")


def add_base_toil_options(parser: ArgumentParser, jobstore_as_flag: bool = False, cwl: bool = False) -> None:
    """
    Add base Toil command line options to the parser.
    :param parser: Argument parser to add options to
    :param jobstore_as_flag: make the job store option a --jobStore flag instead of a required jobStore positional argument.
    :param cwl: whether CWL should be included or not
    """

    # This is necessary as the config file must have at least one valid key to parse properly and we want to use a
    # dummy key
    config = parser.add_argument_group()
    config.add_argument("--config_version", default=None, help=SUPPRESS)

    # If using argparse instead of configargparse, this should just not parse when calling parse_args()
    # default config value is set to none as defaults should already be populated at config init
    config.add_argument('--config', dest='config', is_config_file_arg=True, default=None, metavar="PATH",
                        help="Get options from a config file.")

    add_logging_options(parser)
    parser.register("type", "bool", parseBool)  # Custom type for arg=True/False.

    # Core options
    core_options = parser.add_argument_group(
        title="Toil core options.",
        description="Options to specify the location of the Toil workflow and "
                    "turn on stats collation about the performance of jobs."
    )
    if jobstore_as_flag:
        core_options.add_argument('--jobstore', '--jobStore', dest='jobStore', type=parse_jobstore, default=None,
                                  help=JOBSTORE_HELP)
    else:
        core_options.add_argument('jobStore', type=parse_jobstore, help=JOBSTORE_HELP)

    class WorkDirAction(Action):
        """
        Argparse action class to check that the provided --workDir exists
        """

        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            workDir = values
            if workDir is not None:
                workDir = os.path.abspath(workDir)
                if not os.path.exists(workDir):
                    raise RuntimeError(f"The path provided to --workDir ({workDir}) does not exist.")

                if len(workDir) > 80:
                    logger.warning(f'Length of workDir path "{workDir}" is {len(workDir)} characters.  '
                                   f'Consider setting a shorter path with --workPath or setting TMPDIR to something '
                                   f'like "/tmp" to avoid overly long paths.')
            setattr(namespace, self.dest, workDir)

    class CoordinationDirAction(Action):
        """
        Argparse action class to check that the provided --coordinationDir exists
        """

        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            coordination_dir = values
            if coordination_dir is not None:
                coordination_dir = os.path.abspath(coordination_dir)
                if not os.path.exists(coordination_dir):
                    raise RuntimeError(
                        f"The path provided to --coordinationDir ({coordination_dir}) does not exist.")
            setattr(namespace, self.dest, coordination_dir)

    def make_closed_interval_action(min: Union[int, float], max: Optional[Union[int, float]] = None) -> Type[Action]:
        """
        Returns an argparse action class to check if the input is within the given half-open interval.
        ex:
        Provided value to argparse must be within the interval [min, max]

        :param min: int/float
        :param max: optional int/float
        :return: argparse action
        """

        class ClosedIntOrFloatAction(Action):
            def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
                def is_within(x: Union[int, float]) -> bool:
                    if max is None:
                        return min <= x
                    else:
                        return min <= x <= max

                try:
                    if not is_within(values):
                        raise parser.error(
                            f"{option_string} ({values}) must be within the range: [{min}, {'infinity' if max is None else max}]")
                except AssertionError:
                    raise RuntimeError(f"The {option_string} option has an invalid value: {values}")
                setattr(namespace, self.dest, values)

        return ClosedIntOrFloatAction

    core_options.add_argument("--workDir", dest="workDir", default=None, env_var="TOIL_WORKDIR", action=WorkDirAction,
                              metavar="PATH",
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
                              env_var="TOIL_COORDINATION_DIR", action=CoordinationDirAction, metavar="PATH",
                              help="Absolute path to directory where Toil will keep state and lock files."
                                   "When sharing a cache between containers on a host, this directory must be "
                                   "shared between the containers.")
    core_options.add_argument("--noStdOutErr", dest="noStdOutErr", default=False, action="store_true",
                              help="Do not capture standard output and error from batch system jobs.")
    core_options.add_argument("--stats", dest="stats", default=False, action="store_true",
                              help="Records statistics about the toil workflow to be used by 'toil stats'.")
    clean_choices = ['always', 'onError', 'never', 'onSuccess']
    core_options.add_argument("--clean", dest="clean", choices=clean_choices, default="onSuccess",
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
                              metavar="OPT_PATH", const=os.getcwd(),
                              help="If enabled, writes out JSON resource usage statistics to a file.  "
                                   "The default location for this file is the current working directory, but an "
                                   "absolute path can also be passed to specify where this file should be written. "
                                   "This options only applies when using scalable batch systems.")

    # Restarting the workflow options
    restart_options = parser.add_argument_group(
        title="Toil options for restarting an existing workflow.",
        description="Allows the restart of an existing workflow"
    )
    restart_options.add_argument("--restart", dest="restart", default=False, action="store_true",
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
                         "Setting this option to True instead copies the files into the job store, which may protect "
                         "them from being modified externally.  When set to False, as long as caching is enabled, "
                         "Toil will protect the file automatically by changing the permissions to read-only. "
                         "default=%(default)s")
    link_imports.add_argument("--symlinkImports", dest="symlinkImports", type=strtobool, default=True,
                              metavar="BOOL", help=link_imports_help)
    move_exports = file_store_options.add_mutually_exclusive_group()
    move_exports_help = ('When using a filesystem based job store, output files are by default moved to the '
                         'output directory, and a symlink to the moved exported file is created at the initial '
                         'location.  Setting this option to True instead copies the files into the output directory.  '
                         'Applies to filesystem-based job stores only. '
                         'default=%(default)s')
    move_exports.add_argument("--moveOutputs", dest="moveOutputs", type=strtobool, default=False, metavar="BOOL",
                              help=move_exports_help)

    caching = file_store_options.add_mutually_exclusive_group()
    caching_help = "Enable or disable caching for your workflow, specifying this overrides default from job store"
    caching.add_argument('--caching', dest='caching', type=opt_strtobool, default=None, metavar="BOOL",
                         help=caching_help)
    # default is None according to PR 4299, seems to be generated at runtime

    file_store_options.add_argument("--symlinkJobStoreReads", dest="symlink_job_store_reads", type=strtobool, default=True,
                                    metavar="BOOL",
                                    help="Allow reads and container mounts from a JobStore's shared filesystem directly "
                                         "via symlink. default=%(default)s")

    # Auto scaling options
    autoscaling_options = parser.add_argument_group(
        title="Toil options for autoscaling the cluster of worker nodes.",
        description="Allows the specification of the minimum and maximum number of nodes in an autoscaled cluster, "
                    "as well as parameters to control the level of provisioning."
    )
    provisioner_choices = ['aws', 'gce', None]

    # TODO: Better consolidate this provisioner arg and the one in provisioners/__init__.py?
    autoscaling_options.add_argument('--provisioner', '-p', dest="provisioner", choices=provisioner_choices,
                                     default=None,
                                     help=f"The provisioner for cluster auto-scaling.  This is the main Toil "
                                          f"'--provisioner' option, and defaults to None for running on single "
                                          f"machine and non-auto-scaling batch systems.  The currently supported "
                                          f"choices are {provisioner_choices}.  The default is %(default)s.")
    autoscaling_options.add_argument('--nodeTypes', default=[], dest="nodeTypes", type=parse_node_types,
                                     action="extend",
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
                                          "\tand buy t2.large instances at full price.\n"
                                          "default=%(default)s")

    class NodeExtendAction(_AppendAction):
        """
        argparse Action class to remove the default value on first call, and act as an extend action after
        """

        # with action=append/extend, the argparse default is always prepended to the option
        # so make the CLI have priority by rewriting the option on the first run
        def __init__(self, option_strings: Any, dest: Any, **kwargs: Any):
            super().__init__(option_strings, dest, **kwargs)
            self.is_default = True

        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            if self.is_default:
                setattr(namespace, self.dest, values)
                self.is_default = False
            else:
                super().__call__(parser, namespace, values, option_string)

    autoscaling_options.add_argument('--maxNodes', default=[10], dest="maxNodes", type=parse_int_list,
                                     action=NodeExtendAction, metavar="INT[,INT...]",
                                     help=f"Maximum number of nodes of each type in the cluster, if using autoscaling, "
                                          f"provided as a comma-separated list.  The first value is used as a default "
                                          f"if the list length is less than the number of nodeTypes.  "
                                          f"default=%(default)s")
    autoscaling_options.add_argument('--minNodes', default=[0], dest="minNodes", type=parse_int_list,
                                     action=NodeExtendAction, metavar="INT[,INT...]",
                                     help="Mininum number of nodes of each type in the cluster, if using "
                                          "auto-scaling.  This should be provided as a comma-separated list of the "
                                          "same length as the list of node types. default=%(default)s")
    autoscaling_options.add_argument("--targetTime", dest="targetTime", default=defaultTargetTime, type=int,
                                     action=make_closed_interval_action(0), metavar="INT",
                                     help=f"Sets how rapidly you aim to complete jobs in seconds. Shorter times mean "
                                          f"more aggressive parallelization. The autoscaler attempts to scale up/down "
                                          f"so that it expects all queued jobs will complete within targetTime "
                                          f"seconds.  default=%(default)s")
    autoscaling_options.add_argument("--betaInertia", dest="betaInertia", default=0.1, type=float,
                                     action=make_closed_interval_action(0.0, 0.9), metavar="FLOAT",
                                     help=f"A smoothing parameter to prevent unnecessary oscillations in the number "
                                          f"of provisioned nodes. This controls an exponentially weighted moving "
                                          f"average of the estimated number of nodes. A value of 0.0 disables any "
                                          f"smoothing, and a value of 0.9 will smooth so much that few changes will "
                                          f"ever be made.  Must be between 0.0 and 0.9. default=%(default)s")
    autoscaling_options.add_argument("--scaleInterval", dest="scaleInterval", default=60, type=int, metavar="INT",
                                     help=f"The interval (seconds) between assessing if the scale of "
                                          f"the cluster needs to change. default=%(default)s")
    autoscaling_options.add_argument("--preemptibleCompensation", "--preemptableCompensation",
                                     dest="preemptibleCompensation", default=0.0, type=float,
                                     action=make_closed_interval_action(0.0, 1.0), metavar="FLOAT",
                                     help=f"The preference of the autoscaler to replace preemptible nodes with "
                                          f"non-preemptible nodes, when preemptible nodes cannot be started for some "
                                          f"reason. This value must be between 0.0 and 1.0, inclusive.  "
                                          f"A value of 0.0 disables such "
                                          f"compensation, a value of 0.5 compensates two missing preemptible nodes "
                                          f"with a non-preemptible one. A value of 1.0 replaces every missing "
                                          f"pre-emptable node with a non-preemptible one. default=%(default)s")
    autoscaling_options.add_argument("--nodeStorage", dest="nodeStorage", default=50, type=int, metavar="INT",
                                     help="Specify the size of the root volume of worker nodes when they are launched "
                                          "in gigabytes. You may want to set this if your jobs require a lot of disk "
                                          f"space.  (default=%(default)s).")
    autoscaling_options.add_argument('--nodeStorageOverrides', dest="nodeStorageOverrides", default=[],
                                     type=parse_str_list, action="extend",
                                     metavar="NODETYPE:NODESTORAGE[,NODETYPE:NODESTORAGE...]",
                                     help="Comma-separated list of nodeType:nodeStorage that are used to override "
                                          "the default value from --nodeStorage for the specified nodeType(s).  "
                                          "This is useful for heterogeneous jobs where some tasks require much more "
                                          "disk than others.")

    autoscaling_options.add_argument("--metrics", dest="metrics", default=False, type=strtobool, metavar="BOOL",
                                     help="Enable the prometheus/grafana dashboard for monitoring CPU/RAM usage, "
                                          "queue size, and issued jobs.")
    autoscaling_options.add_argument("--assumeZeroOverhead", dest="assume_zero_overhead", default=False,
                                     type=strtobool, metavar="BOOL",
                                     help="Ignore scheduler and OS overhead and assume jobs can use every last byte "
                                          "of memory and disk on a node when autoscaling.")

    # Parameters to limit service jobs / detect service deadlocks
    service_options = parser.add_argument_group(
        title="Toil options for limiting the number of service jobs and detecting service deadlocks",
        description="Allows the specification of the maximum number of service jobs in a cluster.  By keeping "
                    "this limited we can avoid nodes occupied with services causing deadlocks."
    )
    service_options.add_argument("--maxServiceJobs", dest="maxServiceJobs", default=SYS_MAX_SIZE, type=int,
                                 metavar="INT",
                                 help=SUPPRESS if cwl else f"The maximum number of service jobs that can be run "
                                                           f"concurrently, excluding service jobs running on "
                                                           f"preemptible nodes.  default=%(default)s")
    service_options.add_argument("--maxPreemptibleServiceJobs", dest="maxPreemptibleServiceJobs",
                                 default=SYS_MAX_SIZE,
                                 type=int, metavar="INT",
                                 help=SUPPRESS if cwl else "The maximum number of service jobs that can run "
                                                           "concurrently on preemptible nodes.  default=%(default)s")
    service_options.add_argument("--deadlockWait", dest="deadlockWait", default=60, type=int, metavar="INT",
                                 help=SUPPRESS if cwl else f"Time, in seconds, to tolerate the workflow running only "
                                                           f"the same service jobs, with no jobs to use them, "
                                                           f"before declaring the workflow to be deadlocked and "
                                                           f"stopping.  default=%(default)s")
    service_options.add_argument("--deadlockCheckInterval", dest="deadlockCheckInterval", default=30, type=int,
                                 metavar="INT",
                                 help=SUPPRESS if cwl else "Time, in seconds, to wait between checks to see if the "
                                                           "workflow is stuck running only service jobs, with no jobs "
                                                           "to use them. Should be shorter than --deadlockWait. May "
                                                           "need to be increased if the batch system cannot enumerate "
                                                           "running jobs quickly enough, or if polling for running "
                                                           "jobs is placing an unacceptable load on a shared cluster."
                                                           f"default=%(default)s")

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
    accelerators_note = (
        'Each accelerator specification can have a type (gpu [default], nvidia, amd, cuda, rocm, opencl, '
        'or a specific model like nvidia-tesla-k80), and a count [default: 1]. If both a type and a count '
        'are used, they must be separated by a colon. If multiple types of accelerators are '
        'used, the specifications are separated by commas')

    h2b = lambda x: human2bytes(str(x))

    resource_options.add_argument('--defaultMemory', dest='defaultMemory', default="2.0 Gi", type=h2b,
                                  action=make_open_interval_action(1),
                                  help=resource_help_msg.format('default', 'memory', disk_mem_note,
                                                                bytes2human(2147483648)))
    resource_options.add_argument('--defaultCores', dest='defaultCores', default=1, metavar='FLOAT', type=float,
                                  action=make_open_interval_action(1.0),
                                  help=resource_help_msg.format('default', 'cpu', cpu_note, str(1)))
    resource_options.add_argument('--defaultDisk', dest='defaultDisk', default="2.0 Gi", metavar='INT', type=h2b,
                                  action=make_open_interval_action(1),
                                  help=resource_help_msg.format('default', 'disk', disk_mem_note,
                                                                bytes2human(2147483648)))
    resource_options.add_argument('--defaultAccelerators', dest='defaultAccelerators', default=[],
                                  metavar='ACCELERATOR[,ACCELERATOR...]', type=parse_accelerator_list, action="extend",
                                  help=resource_help_msg.format('default', 'accelerators', accelerators_note, []))
    resource_options.add_argument('--defaultPreemptible', '--defaultPreemptable', dest='defaultPreemptible',
                                  metavar='BOOL',
                                  type=strtobool, nargs='?', const=True, default=False,
                                  help='Make all jobs able to run on preemptible (spot) nodes by default.')
    resource_options.add_argument('--maxCores', dest='maxCores', default=SYS_MAX_SIZE, metavar='INT', type=int,
                                  action=make_open_interval_action(1),
                                  help=resource_help_msg.format('max', 'cpu', cpu_note, str(SYS_MAX_SIZE)))
    resource_options.add_argument('--maxMemory', dest='maxMemory', default=SYS_MAX_SIZE, metavar='INT', type=h2b,
                                  action=make_open_interval_action(1),
                                  help=resource_help_msg.format('max', 'memory', disk_mem_note,
                                                                bytes2human(SYS_MAX_SIZE)))
    resource_options.add_argument('--maxDisk', dest='maxDisk', default=SYS_MAX_SIZE, metavar='INT', type=h2b,
                                  action=make_open_interval_action(1),
                                  help=resource_help_msg.format('max', 'disk', disk_mem_note,
                                                                bytes2human(SYS_MAX_SIZE)))

    # Retrying/rescuing jobs
    job_options = parser.add_argument_group(
        title="Toil options for rescuing/killing/restarting jobs.",
        description="The options for jobs that either run too long/fail or get lost (some batch systems have issues!)."
    )
    job_options.add_argument("--retryCount", dest="retryCount", default=1, type=int,
                             action=make_open_interval_action(0), metavar="INT",
                             help=f"Number of times to retry a failing job before giving up and "
                                  f"labeling job failed. default={1}")
    job_options.add_argument("--enableUnlimitedPreemptibleRetries", "--enableUnlimitedPreemptableRetries",
                             dest="enableUnlimitedPreemptibleRetries",
                             type=strtobool, default=False, metavar="BOOL",
                             help="If set, preemptible failures (or any failure due to an instance getting "
                                  "unexpectedly terminated) will not count towards job failures and --retryCount.")
    job_options.add_argument("--doubleMem", dest="doubleMem", type=strtobool, default=False, metavar="BOOL",
                             help="If set, batch jobs which die to reaching memory limit on batch schedulers "
                                  "will have their memory doubled and they will be retried. The remaining "
                                  "retry count will be reduced by 1. Currently supported by LSF.")
    job_options.add_argument("--maxJobDuration", dest="maxJobDuration", default=SYS_MAX_SIZE, type=int,
                             action=make_open_interval_action(1), metavar="INT",
                             help=f"Maximum runtime of a job (in seconds) before we kill it (this is a lower bound, "
                                  f"and the actual time before killing the job may be longer).  "
                                  f"default=%(default)s")
    job_options.add_argument("--rescueJobsFrequency", dest="rescueJobsFrequency", default=60, type=int,
                             action=make_open_interval_action(1), metavar="INT",
                             help=f"Period of time to wait (in seconds) between checking for missing/overlong jobs, "
                                  f"that is jobs which get lost by the batch system. Expert parameter.  "
                                  f"default=%(default)s")
    job_options.add_argument("--jobStoreTimeout", dest="job_store_timeout", default=30, type=float,
                             action=make_open_interval_action(0), metavar="FLOAT",
                             help=f"Maximum time (in seconds) to wait for a job's update to the job store "
                                  f"before declaring it failed. default=%(default)s")


    # Log management options
    log_options = parser.add_argument_group(
        title="Toil log management options.",
        description="Options for how Toil should manage its logs."
    )
    log_options.add_argument("--maxLogFileSize", dest="maxLogFileSize", default=100 * 1024 * 1024, type=h2b,
                             action=make_open_interval_action(1),
                             help=f"The maximum size of a job log file to keep (in bytes), log files larger than "
                                  f"this will be truncated to the last X bytes. Setting this option to zero will "
                                  f"prevent any truncation. Setting this option to a negative value will truncate "
                                  f"from the beginning.  Default={bytes2human(100 * 1024 * 1024)}")
    log_options.add_argument("--writeLogs", dest="writeLogs", nargs='?', action='store', default=None,
                             const=os.getcwd(), metavar="OPT_PATH",
                             help="Write worker logs received by the leader into their own files at the specified "
                                  "path. Any non-empty standard output and error from failed batch system jobs will "
                                  "also be written into files at this path.  The current working directory will be "
                                  "used if a path is not specified explicitly. Note: By default only the logs of "
                                  "failed jobs are returned to leader. Set log level to 'debug' or enable "
                                  "'--writeLogsFromAllJobs' to get logs back from successful jobs, and adjust "
                                  "'maxLogFileSize' to control the truncation limit for worker logs.")
    log_options.add_argument("--writeLogsGzip", dest="writeLogsGzip", nargs='?', action='store', default=None,
                             const=os.getcwd(), metavar="OPT_PATH",
                             help="Identical to --writeLogs except the logs files are gzipped on the leader.")
    log_options.add_argument("--writeLogsFromAllJobs", dest="writeLogsFromAllJobs", type=strtobool,
                             default=False, metavar="BOOL",
                             help="Whether to write logs from all jobs (including the successful ones) without "
                                  "necessarily setting the log level to 'debug'. Ensure that either --writeLogs "
                                  "or --writeLogsGzip is set if enabling this option.")
    log_options.add_argument("--writeMessages", dest="write_messages", default=None,
                             type=lambda x: None if x is None else os.path.abspath(x), metavar="PATH",
                             help="File to send messages from the leader's message bus to.")
    log_options.add_argument("--realTimeLogging", dest="realTimeLogging", type=strtobool, default=False, metavar="BOOL",
                             help="Enable real-time logging from workers to leader")

    # Misc options
    misc_options = parser.add_argument_group(
        title="Toil miscellaneous options.",
        description="Everything else."
    )
    misc_options.add_argument('--disableChaining', dest='disableChaining', type=strtobool, default=False,
                              metavar="BOOL",
                              help="Disables chaining of jobs (chaining uses one job's resource allocation "
                                   "for its successor job if possible).")
    misc_options.add_argument("--disableJobStoreChecksumVerification", dest="disableJobStoreChecksumVerification",
                              default=False, type=strtobool, metavar="BOOL",
                              help="Disables checksum verification for files transferred to/from the job store.  "
                                   "Checksum verification is a safety check to ensure the data is not corrupted "
                                   "during transfer. Currently only supported for non-streaming AWS files.")

    class SSEKeyAction(Action):
        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            if values is not None:
                sse_key = values
                if sse_key is None:
                    return
                with open(sse_key) as f:
                    assert len(f.readline().rstrip()) == 32, 'SSE key appears to be invalid.'
            setattr(namespace, self.dest, values)

    misc_options.add_argument("--sseKey", dest="sseKey", default=None, action=SSEKeyAction, metavar="PATH",
                              help="Path to file containing 32 character key to be used for server-side encryption on "
                                   "awsJobStore or googleJobStore. SSE will not be used if this flag is not passed.")

    # yaml.safe_load is being deprecated, this is the suggested workaround
    def yaml_safe_load(stream: Any) -> Any:
        yaml = YAML(typ='safe', pure=True)
        d = yaml.load(stream)
        if isinstance(d, dict):
            # this means the argument was a dictionary and is valid yaml (for configargparse)
            return d
        else:
            # this means the argument is likely in it's string format (for CLI)
            return parse_set_env(parse_str_list(stream))

    class ExtendActionDict(Action):
        """
        Argparse action class to implement the action="extend" functionality on dictionaries
        """

        def __call__(self, parser: Any, namespace: Any, values: Any, option_string: Any = None) -> None:
            items = getattr(namespace, self.dest, None)
            assert items is not None  # for mypy. This should never be None, esp. if called in setEnv
            # note: this will overwrite existing entries
            items.update(values)

    misc_options.add_argument("--setEnv", '-e', metavar='NAME=VALUE or NAME', dest="environment",
                              default={}, type=yaml_safe_load, action=ExtendActionDict,
                              help="Set an environment variable early on in the worker. If VALUE is null, it will "
                                   "be looked up in the current environment. Independently of this option, the worker "
                                   "will try to emulate the leader's environment before running a job, except for "
                                   "some variables known to vary across systems.  Using this option, a variable can "
                                   "be injected into the worker process itself before it is started.")
    misc_options.add_argument("--servicePollingInterval", dest="servicePollingInterval", default=60.0, type=float,
                              action=make_open_interval_action(0.0), metavar="FLOAT",
                              help=f"Interval of time service jobs wait between polling for the existence of the "
                                   f"keep-alive flag.  Default: {60.0}")
    misc_options.add_argument('--forceDockerAppliance', dest='forceDockerAppliance', type=strtobool, default=False,
                              metavar="BOOL",
                              help='Disables sanity checking the existence of the docker image specified by '
                                   'TOIL_APPLIANCE_SELF, which Toil uses to provision mesos for autoscaling.')
    misc_options.add_argument('--statusWait', dest='statusWait', type=int, default=3600, metavar="INT",
                              help="Seconds to wait between reports of running jobs.")
    misc_options.add_argument('--disableProgress', dest='disableProgress', action="store_true", default=False,
                              help="Disables the progress bar shown when standard error is a terminal.")

    # Debug options
    debug_options = parser.add_argument_group(
        title="Toil debug options.",
        description="Debug options for finding problems or helping with testing."
    )
    debug_options.add_argument("--debugWorker", dest="debugWorker", default=False, action="store_true",
                               help="Experimental no forking mode for local debugging.  Specifically, workers "
                                    "are not forked and stderr/stdout are not redirected to the log.")
    debug_options.add_argument("--disableWorkerOutputCapture", dest="disableWorkerOutputCapture", default=False,
                               action="store_true",
                               help="Let worker output go to worker's standard out/error instead of per-job logs.")
    debug_options.add_argument("--badWorker", dest="badWorker", default=0.0, type=float,
                               action=make_closed_interval_action(0.0, 1.0), metavar="FLOAT",
                               help=f"For testing purposes randomly kill --badWorker proportion of jobs using "
                                    f"SIGKILL.  default={0.0}")
    debug_options.add_argument("--badWorkerFailInterval", dest="badWorkerFailInterval", default=0.01, type=float,
                               action=make_open_interval_action(0.0), metavar="FLOAT", # might be cyclical?
                               help=f"When killing the job pick uniformly within the interval from 0.0 to "
                                    f"--badWorkerFailInterval seconds after the worker starts.  "
                                    f"default={0.01}")

    # All deprecated options:

    # These are deprecated in favor of a simpler option
    #   ex: noLinkImports and linkImports can be simplified into a single link_imports argument
    link_imports.add_argument("--noLinkImports", dest="linkImports", action="store_false",
                              help=SUPPRESS)
    link_imports.add_argument("--linkImports", dest="linkImports", action="store_true",
                              help=SUPPRESS)
    link_imports.set_defaults(linkImports=None)

    move_exports.add_argument("--moveExports", dest="moveExports", action="store_true",
                              help=SUPPRESS)
    move_exports.add_argument("--noMoveExports", dest="moveExports", action="store_false",
                              help=SUPPRESS)
    link_imports.set_defaults(moveExports=None)

    # dest is set to enableCaching to not conflict with the current --caching destination
    caching.add_argument('--disableCaching', dest='enableCaching', action='store_false', help=SUPPRESS)
    caching.set_defaults(enableCaching=None)
