# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import

from future import standard_library

standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
import logging
import os
import re
import sys
import tempfile
import time
import uuid
import requests
from argparse import ArgumentParser
from six import iteritems

from toil.lib.humanize import bytes2human
from toil.lib.retry import retry
from toil import subprocess
from toil import pickle
from toil import logProcessContext
from toil.lib.bioio import addLoggingOptions, getLogLevelString, setLoggingFromOptions
from toil.realtimeLogger import RealtimeLogger
from toil.batchSystems.options import addOptions as addBatchOptions
from toil.batchSystems.options import setDefaultOptions as setDefaultBatchOptions
from toil.batchSystems.options import setOptions as setBatchOptions
from toil.provisioners import clusterFactory
from toil.provisioners.aws import checkValidNodeTypes, zoneToRegion
from toil import lookupEnvVar
from toil.version import dockerRegistry, dockerTag

# aim to pack autoscaling jobs within a 30 minute block before provisioning a new node
defaultTargetTime = 1800
logger = logging.getLogger(__name__)


class Config(object):
    """
    Class to represent configuration operations for a toil workflow run.
    """

    def __init__(self):
        # Core options
        self.workflowID = None
        """This attribute uniquely identifies the job store and therefore the workflow. It is
        necessary in order to distinguish between two consecutive workflows for which
        self.jobStore is the same, e.g. when a job store name is reused after a previous run has
        finished sucessfully and its job store has been clean up."""
        self.workflowAttemptNumber = None
        self.jobStore = None
        self.logLevel = getLogLevelString()
        self.workDir = None
        self.noStdOutErr = False
        self.stats = False

        # Because the stats option needs the jobStore to persist past the end of the run,
        # the clean default value depends the specified stats option and is determined in setOptions
        self.clean = None
        self.cleanWorkDir = None
        self.clusterStats = None

        # Restarting the workflow options
        self.restart = False

        # Batch system options
        setDefaultBatchOptions(self)

        # Autoscaling options
        self.provisioner = None
        self.nodeTypes = []
        checkValidNodeTypes(self.provisioner, self.nodeTypes)
        self.nodeOptions = None
        self.minNodes = None
        self.maxNodes = [10]
        self.targetTime = defaultTargetTime
        self.betaInertia = 0.1
        self.scaleInterval = 60
        self.preemptableCompensation = 0.0
        self.nodeStorage = 50
        self.metrics = False

        # Parameters to limit service jobs, so preventing deadlock scheduling scenarios
        self.maxPreemptableServiceJobs = sys.maxsize
        self.maxServiceJobs = sys.maxsize
        self.deadlockWait = 60  # Number of seconds to wait before declaring a deadlock
        self.statePollingWait = 1  # Number of seconds to wait before querying job state

        # Resource requirements
        self.defaultMemory = 2147483648
        self.defaultCores = 1
        self.defaultDisk = 2147483648
        self.readGlobalFileMutableByDefault = False
        self.defaultPreemptable = False
        self.maxCores = sys.maxsize
        self.maxMemory = sys.maxsize
        self.maxDisk = sys.maxsize

        # Retrying/rescuing jobs
        self.retryCount = 1
        self.maxJobDuration = sys.maxsize
        self.rescueJobsFrequency = 3600

        # Misc
        self.disableCaching = True
        self.disableChaining = False
        self.maxLogFileSize = 64000
        self.writeLogs = None
        self.writeLogsGzip = None
        self.writeLogsFromAllJobs = False
        self.sseKey = None
        self.servicePollingInterval = 60
        self.useAsync = True
        self.forceDockerAppliance = False
        self.runCwlInternalJobsOnWorkers = False

        # Debug options
        self.debugWorker = False
        self.disableWorkerOutputCapture = False
        self.badWorker = 0.0
        self.badWorkerFailInterval = 0.01

        # CWL
        self.cwl = False

    def setOptions(self, options):
        """
        Creates a config object from the options object.
        """
        from toil.lib.humanize import human2bytes  # This import is used to convert
        # from human readable quantites to integers
        def setOption(varName, parsingFn=None, checkFn=None, default=None):
            # If options object has the option "varName" specified
            # then set the "varName" attrib to this value in the config object
            x = getattr(options, varName, None)
            if x is None:
                x = default

            if x is not None:
                if parsingFn is not None:
                    x = parsingFn(x)
                if checkFn is not None:
                    try:
                        checkFn(x)
                    except AssertionError:
                        raise RuntimeError("The %s option has an invalid value: %s"
                                           % (varName, x))
                setattr(self, varName, x)

        # Function to parse integer from string expressed in different formats
        h2b = lambda x: human2bytes(str(x))

        def parseJobStore(s):
            name, rest = Toil.parseLocator(s)
            if name == 'file':
                # We need to resolve relative paths early, on the leader, because the worker process
                # may have a different working directory than the leader, e.g. under Mesos.
                return Toil.buildLocator(name, os.path.abspath(rest))
            else:
                return s

        def parseStrList(s):
            s = s.split(",")
            s = [str(x) for x in s]
            return s

        def parseIntList(s):
            s = s.split(",")
            s = [int(x) for x in s]
            return s

        # Core options
        setOption("jobStore", parsingFn=parseJobStore)
        # TODO: LOG LEVEL STRING
        setOption("workDir")
        if self.workDir is not None:
            self.workDir = os.path.abspath(self.workDir)
            if not os.path.exists(self.workDir):
                raise RuntimeError("The path provided to --workDir (%s) does not exist."
                                   % self.workDir)
        setOption("noStdOutErr")
        setOption("stats")
        setOption("cleanWorkDir")
        setOption("clean")
        if self.stats:
            if self.clean != "never" and self.clean is not None:
                raise RuntimeError("Contradicting options passed: Clean flag is set to %s "
                                   "despite the stats flag requiring "
                                   "the jobStore to be intact at the end of the run. "
                                   "Set clean to \'never\'" % self.clean)
            self.clean = "never"
        elif self.clean is None:
            self.clean = "onSuccess"
        setOption('clusterStats')
        setOption("restart")

        # Batch system options
        setOption("batchSystem")
        setBatchOptions(self, setOption)
        setOption("disableAutoDeployment")
        setOption("scale", float, fC(0.0))
        setOption("mesosMasterAddress")
        setOption("parasolCommand")
        setOption("parasolMaxBatches", int, iC(1))
        setOption("linkImports")
        setOption("environment", parseSetEnv)

        # Autoscaling options
        setOption("provisioner")
        setOption("nodeTypes", parseStrList)
        setOption("nodeOptions")
        setOption("minNodes", parseIntList)
        setOption("maxNodes", parseIntList)
        setOption("targetTime", int)
        if self.targetTime <= 0:
            raise RuntimeError('targetTime (%s) must be a positive integer!'
                               '' % self.targetTime)
        setOption("betaInertia", float)
        if not 0.0 <= self.betaInertia <= 0.9:
            raise RuntimeError('betaInertia (%f) must be between 0.0 and 0.9!'
                               '' % self.betaInertia)
        setOption("scaleInterval", float)
        setOption("metrics")
        setOption("preemptableCompensation", float)
        if not 0.0 <= self.preemptableCompensation <= 1.0:
            raise RuntimeError('preemptableCompensation (%f) must be between 0.0 and 1.0!'
                               '' % self.preemptableCompensation)
        setOption("nodeStorage", int)

        # Parameters to limit service jobs / detect deadlocks
        setOption("maxServiceJobs", int)
        setOption("maxPreemptableServiceJobs", int)
        setOption("deadlockWait", int)
        setOption("statePollingWait", int)

        # Resource requirements
        setOption("defaultMemory", h2b, iC(1))
        setOption("defaultCores", float, fC(1.0))
        setOption("defaultDisk", h2b, iC(1))
        setOption("readGlobalFileMutableByDefault")
        setOption("maxCores", int, iC(1))
        setOption("maxMemory", h2b, iC(1))
        setOption("maxDisk", h2b, iC(1))
        setOption("defaultPreemptable")

        # Retrying/rescuing jobs
        setOption("retryCount", int, iC(1))
        setOption("maxJobDuration", int, iC(1))
        setOption("rescueJobsFrequency", int, iC(1))

        # Misc
        setOption("maxLocalJobs", int)
        setOption("disableCaching")
        setOption("disableChaining")
        setOption("maxLogFileSize", h2b, iC(1))
        setOption("writeLogs")
        setOption("writeLogsGzip")
        setOption("writeLogsFromAllJobs")
        setOption("runCwlInternalJobsOnWorkers")

        assert not (self.writeLogs and self.writeLogsGzip), \
            "Cannot use both --writeLogs and --writeLogsGzip at the same time."
        assert not self.writeLogsFromAllJobs or self.writeLogs or self.writeLogsGzip, \
            "To enable --writeLogsFromAllJobs, either --writeLogs or --writeLogsGzip must be set."

        def checkSse(sseKey):
            with open(sseKey) as f:
                assert (len(f.readline().rstrip()) == 32)

        setOption("sseKey", checkFn=checkSse)
        setOption("servicePollingInterval", float, fC(0.0))
        setOption("forceDockerAppliance")

        # Debug options
        setOption("debugWorker")
        setOption("disableWorkerOutputCapture")
        setOption("badWorker", float, fC(0.0, 1.0))
        setOption("badWorkerFailInterval", float, fC(0.0))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return self.__dict__.__hash__()


jobStoreLocatorHelp = ("A job store holds persistent information about the jobs and files in a "
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


def _addOptions(addGroupFn, config):
    #
    # Core options
    #
    addOptionFn = addGroupFn("toil core options",
                             "Options to specify the location of the Toil workflow and turn on "
                             "stats collation about the performance of jobs.")
    addOptionFn('jobStore', type=str,
                help="The location of the job store for the workflow. " + jobStoreLocatorHelp)
    addOptionFn("--workDir", dest="workDir", default=None,
                help="Absolute path to directory where temporary files generated during the Toil "
                     "run should be placed. Temp files and folders, as well as standard output "
                     "and error from batch system jobs (unless --noStdOutErr), will be placed in a "
                     "directory toil-<workflowID> within workDir. The workflowID is generated by "
                     "Toil and will be reported in the workflow logs. Default is determined by the "
                     "variables (TMPDIR, TEMP, TMP) via mkdtemp. This directory needs to exist on "
                     "all machines running jobs; if capturing standard output and error from batch "
                     "system jobs is desired, it will generally need to be on a shared file system.")
    addOptionFn("--noStdOutErr", dest="noStdOutErr", action="store_true", default=None,
                help="Do not capture standard output and error from batch system jobs.")
    addOptionFn("--stats", dest="stats", action="store_true", default=None,
                help="Records statistics about the toil workflow to be used by 'toil stats'.")
    addOptionFn("--clean", dest="clean", choices=['always', 'onError', 'never', 'onSuccess'],
                default=None,
                help=("Determines the deletion of the jobStore upon completion of the program. "
                      "Choices: 'always', 'onError','never', 'onSuccess'. The --stats option requires "
                      "information from the jobStore upon completion so the jobStore will never be deleted with"
                      "that flag. If you wish to be able to restart the run, choose \'never\' or \'onSuccess\'. "
                      "Default is \'never\' if stats is enabled, and \'onSuccess\' otherwise"))
    addOptionFn("--cleanWorkDir", dest="cleanWorkDir",
                choices=['always', 'never', 'onSuccess', 'onError'], default='always',
                help=(
                "Determines deletion of temporary worker directory upon completion of a job. Choices: 'always', "
                "'never', 'onSuccess'. Default = always. WARNING: This option should be changed for debugging "
                "only. Running a full pipeline with this option could fill your disk with intermediate data."))
    addOptionFn("--clusterStats", dest="clusterStats", nargs='?', action='store',
                default=None, const=os.getcwd(),
                help="If enabled, writes out JSON resource usage statistics to a file. "
                     "The default location for this file is the current working directory, "
                     "but an absolute path can also be passed to specify where this file "
                     "should be written. This options only applies when using scalable batch "
                     "systems.")
    #
    # Restarting the workflow options
    #
    addOptionFn = addGroupFn("toil options for restarting an existing workflow",
                             "Allows the restart of an existing workflow")
    addOptionFn("--restart", dest="restart", default=None, action="store_true",
                help="If --restart is specified then will attempt to restart existing workflow "
                     "at the location pointed to by the --jobStore option. Will raise an exception "
                     "if the workflow does not exist")

    #
    # Batch system options
    #

    addOptionFn = addGroupFn("toil options for specifying the batch system",
                             "Allows the specification of the batch system, and arguments to the "
                             "batch system/big batch system (see below).")
    addBatchOptions(addOptionFn, config)

    #
    # Auto scaling options
    #
    addOptionFn = addGroupFn("toil options for autoscaling the cluster of worker nodes",
                             "Allows the specification of the minimum and maximum number of nodes "
                             "in an autoscaled cluster, as well as parameters to control the "
                             "level of provisioning.")

    addOptionFn("--provisioner", dest="provisioner", choices=['aws', 'gce'],
                help="The provisioner for cluster auto-scaling. The currently supported choices are"
                     "'gce', or 'aws'. The default is %s." % config.provisioner)

    addOptionFn('--nodeTypes', default=None,
                help="List of node types separated by commas. The syntax for each node type "
                     "depends on the provisioner used. For the cgcloud and AWS provisioners "
                     "this is the name of an EC2 instance type, optionally followed by a "
                     "colon and the price in dollars "
                     "to bid for a spot instance of that type, for example 'c3.8xlarge:0.42'."
                     "If no spot bid is specified, nodes of this type will be non-preemptable."
                     "It is acceptable to specify an instance as both preemptable and "
                     "non-preemptable, including it twice in the list. In that case,"
                     "preemptable nodes of that type will be preferred when creating "
                     "new nodes once the maximum number of preemptable-nodes has been"
                     "reached.")

    addOptionFn('--nodeOptions', default=None,
                help = "Options for provisioning the nodes. The syntax "
                       "depends on the provisioner used. Neither the CGCloud nor the AWS "
                       "provisioner support any node options.")

    addOptionFn('--minNodes', default=None,
                help="Mininum number of nodes of each type in the cluster, if using "
                     "auto-scaling. This should be provided as a comma-separated "
                     "list of the same length as the list of node types. default=0")

    addOptionFn('--maxNodes', default=None,
                help="Maximum number of nodes of each type in the cluster, if using "
                     "autoscaling, provided as a comma-separated list. The first value is used "
                     "as a default if the list length is less than the number of nodeTypes. "
                     "default=%s" % config.maxNodes[0])

    addOptionFn("--targetTime", dest="targetTime", default=None,
                help=("Sets how rapidly you aim to complete jobs in seconds. Shorter times mean "
                      "more aggressive parallelization. The autoscaler attempts to scale up/down "
                      "so that it expects all queued jobs will complete within targetTime "
                      "seconds. default=%s" % config.targetTime))
    addOptionFn("--betaInertia", dest="betaInertia", default=None,
                help=("A smoothing parameter to prevent unnecessary oscillations in the "
                      "number of provisioned nodes. This controls an exponentially weighted "
                      "moving average of the estimated number of nodes. A value of 0.0 "
                      "disables any smoothing, and a value of 0.9 will smooth so much that "
                      "few changes will ever be made.  Must be between 0.0 and 0.9.  "
                      "default=%s" % config.betaInertia))
    addOptionFn("--scaleInterval", dest="scaleInterval", default=None,
                help=("The interval (seconds) between assessing if the scale of"
                      " the cluster needs to change. default=%s" % config.scaleInterval))
    addOptionFn("--preemptableCompensation", dest="preemptableCompensation",
                default=None,
                help=("The preference of the autoscaler to replace preemptable nodes with "
                      "non-preemptable nodes, when preemptable nodes cannot be started for some "
                      "reason. Defaults to %s. This value must be between 0.0 and 1.0, inclusive. "
                      "A value of 0.0 disables such compensation, a value of 0.5 compensates two "
                      "missing preemptable nodes with a non-preemptable one. A value of 1.0 "
                      "replaces every missing pre-emptable node with a non-preemptable one." %
                      config.preemptableCompensation))
    addOptionFn("--nodeStorage", dest="nodeStorage", default=50,
                help=("Specify the size of the root volume of worker nodes when they are launched "
                      "in gigabytes. You may want to set this if your jobs require a lot of disk "
                      "space. The default value is 50."))
    addOptionFn("--metrics", dest="metrics",
                default=False, action="store_true",
                help=(
                "Enable the prometheus/grafana dashboard for monitoring CPU/RAM usage, queue size, "
                "and issued jobs."))

    #
    # Parameters to limit service jobs / detect service deadlocks
    #
    if not config.cwl:
        addOptionFn = addGroupFn(
            "toil options for limiting the number of service jobs and detecting service deadlocks",
            "Allows the specification of the maximum number of service jobs "
            "in a cluster. By keeping this limited "
            " we can avoid all the nodes being occupied with services, so causing a deadlock")
        addOptionFn("--maxServiceJobs", dest="maxServiceJobs", default=None,
                    help=(
                    "The maximum number of service jobs that can be run concurrently, excluding service jobs running on preemptable nodes. default=%s" % config.maxServiceJobs))
        addOptionFn("--maxPreemptableServiceJobs", dest="maxPreemptableServiceJobs", default=None,
                    help=(
                    "The maximum number of service jobs that can run concurrently on preemptable nodes. default=%s" % config.maxPreemptableServiceJobs))
        addOptionFn("--deadlockWait", dest="deadlockWait", default=None,
                    help=(
                    "The minimum number of seconds to observe the cluster stuck running only the same service jobs before throwing a deadlock exception. default=%s" % config.deadlockWait))
        addOptionFn("--statePollingWait", dest="statePollingWait", default=1,
                    help=("Time, in seconds, to wait before doing a scheduler query for job state. "
                          "Return cached results if within the waiting period."))

    #
    # Resource requirements
    #
    addOptionFn = addGroupFn("toil options for cores/memory requirements",
                             "The options to specify default cores/memory requirements (if not "
                             "specified by the jobs themselves), and to limit the total amount of "
                             "memory/cores requested from the batch system.")
    addOptionFn('--defaultMemory', dest='defaultMemory', default=None, metavar='INT',
                help='The default amount of memory to request for a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Standard '
                     'suffixes like K, Ki, M, Mi, G or Gi are supported. Default is %s' %
                     bytes2human(config.defaultMemory, symbols='iec'))
    addOptionFn('--defaultCores', dest='defaultCores', default=None, metavar='FLOAT',
                help='The default number of CPU cores to dedicate a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Fractions of a '
                     'core (for example 0.1) are supported on some batch systems, namely Mesos '
                     'and singleMachine. Default is %.1f ' % config.defaultCores)
    addOptionFn('--defaultDisk', dest='defaultDisk', default=None, metavar='INT',
                help='The default amount of disk space to dedicate a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Standard '
                     'suffixes like K, Ki, M, Mi, G or Gi are supported. Default is %s' %
                     bytes2human(config.defaultDisk, symbols='iec'))
    assert not config.defaultPreemptable, 'User would be unable to reset config.defaultPreemptable'
    addOptionFn('--defaultPreemptable', dest='defaultPreemptable', action='store_true')
    addOptionFn('--maxCores', dest='maxCores', default=None, metavar='INT',
                help='The maximum number of CPU cores to request from the batch system at any one '
                     'time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. Default '
                     'is %s' % bytes2human(config.maxCores, symbols='iec'))
    addOptionFn('--maxMemory', dest='maxMemory', default=None, metavar='INT',
                help="The maximum amount of memory to request from the batch system at any one "
                     "time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. Default "
                     "is %s" % bytes2human(config.maxMemory, symbols='iec'))
    addOptionFn('--maxDisk', dest='maxDisk', default=None, metavar='INT',
                help='The maximum amount of disk space to request from the batch system at any '
                     'one time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. '
                     'Default is %s' % bytes2human(config.maxDisk, symbols='iec'))

    #
    # Retrying/rescuing jobs
    #
    addOptionFn = addGroupFn("toil options for rescuing/killing/restarting jobs", \
                             "The options for jobs that either run too long/fail or get lost \
                             (some batch systems have issues!)")
    addOptionFn("--retryCount", dest="retryCount", default=None,
                help=("Number of times to retry a failing job before giving up and "
                      "labeling job failed. default=%s" % config.retryCount))
    addOptionFn("--maxJobDuration", dest="maxJobDuration", default=None,
                help=("Maximum runtime of a job (in seconds) before we kill it "
                      "(this is a lower bound, and the actual time before killing "
                      "the job may be longer). default=%s" % config.maxJobDuration))
    addOptionFn("--rescueJobsFrequency", dest="rescueJobsFrequency", default=None,
                help=("Period of time to wait (in seconds) between checking for "
                      "missing/overlong jobs, that is jobs which get lost by the batch "
                      "system. Expert parameter. default=%s" % config.rescueJobsFrequency))

    #
    # Misc options
    #
    addOptionFn = addGroupFn("Toil Miscellaneous Options", "Miscellaneous Options")
    addOptionFn('--disableCaching', dest='disableCaching',
                type='bool', nargs='?', const=True, default=True,
                help='Disables caching in the file store. This flag must be set to use '
                     'a batch system that does not support caching such as Grid Engine, Parasol, '
                     'LSF, or Slurm')
    addOptionFn('--disableChaining', dest='disableChaining', action='store_true', default=False,
                help="Disables chaining of jobs (chaining uses one job's resource allocation "
                "for its successor job if possible).")
    addOptionFn("--maxLogFileSize", dest="maxLogFileSize", default=None,
                help=("The maximum size of a job log file to keep (in bytes), log files "
                      "larger than this will be truncated to the last X bytes. Setting "
                      "this option to zero will prevent any truncation. Setting this "
                      "option to a negative value will truncate from the beginning."
                      "Default=%s" % bytes2human(config.maxLogFileSize)))
    addOptionFn("--writeLogs", dest="writeLogs", nargs='?', action='store',
                default=None, const=os.getcwd(),
                help="Write worker logs received by the leader into their own files at the "
                     "specified path. Any non-empty standard output and error from failed batch "
                     "system jobs will also be written into files at this path. "
                     "The current working directory will be used if a path is "
                     "not specified explicitly. Note: By default only the logs of failed jobs are "
                     "returned to leader. Set log level to 'debug' or enable "
                     "'--writeLogsFromAllJobs' to get logs back from successful jobs, and adjust "
                     "'maxLogFileSize' to control the truncation limit for worker logs.")
    addOptionFn("--writeLogsGzip", dest="writeLogsGzip", nargs='?', action='store',
                default=None, const=os.getcwd(),
                help="Identical to --writeLogs except the logs files are gzipped on the leader.")
    addOptionFn("--writeLogsFromAllJobs", dest="writeLogsFromAllJobs", action='store_true',
                default=False,
                help="Whether to write logs from all jobs (including the successful ones) without "
                     "necessarily setting the log level to 'debug'. Ensure that either --writeLogs "
                     "or --writeLogsGzip is set if enabling this option.")
    addOptionFn("--realTimeLogging", dest="realTimeLogging", action="store_true", default=False,
                help="Enable real-time logging from workers to masters")

    addOptionFn("--sseKey", dest="sseKey", default=None,
                help="Path to file containing 32 character key to be used for server-side encryption on "
                     "awsJobStore or googleJobStore. SSE will not be used if this flag is not passed.")
    addOptionFn("--setEnv", '-e', metavar='NAME=VALUE or NAME',
                dest="environment", default=[], action="append",
                help="Set an environment variable early on in the worker. If VALUE is omitted, "
                     "it will be looked up in the current environment. Independently of this "
                     "option, the worker will try to emulate the leader's environment before "
                     "running a job, except for some variables known to vary across systems. "
                     "Using this option, a variable can be injected into the worker process "
                     "itself before it is started.")
    addOptionFn("--servicePollingInterval", dest="servicePollingInterval", default=None,
                help="Interval of time service jobs wait between polling for the existence"
                " of the keep-alive flag (defailt=%s)" % config.servicePollingInterval)
    addOptionFn('--forceDockerAppliance', dest='forceDockerAppliance', action='store_true',
                default=False,
                help='Disables sanity checking the existence of the docker image specified by '
                'TOIL_APPLIANCE_SELF, which Toil uses to provision mesos for autoscaling.')
    #
    # Debug options
    #
    addOptionFn = addGroupFn("toil debug options", "Debug options")
    addOptionFn("--debugWorker", default=False, action="store_true",
                help="Experimental no forking mode for local debugging."
                     " Specifically, workers are not forked and"
                     " stderr/stdout are not redirected to the log.")
    addOptionFn("--disableWorkerOutputCapture", default=False, action="store_true",
                help="Let worker output go to worker's standard"
                     " out/error instead of per-job logs.")
    addOptionFn("--badWorker", dest="badWorker", default=None,
                help=(
                "For testing purposes randomly kill 'badWorker' proportion of jobs using SIGKILL, default=%s" % config.badWorker))
    addOptionFn("--badWorkerFailInterval", dest="badWorkerFailInterval", default=None,
                help=("When killing the job pick uniformly within the interval from 0.0 to "
                      "'badWorkerFailInterval' seconds after the worker starts, default=%s" % config.badWorkerFailInterval))


def addOptions(parser, config=Config()):
    """
    Adds toil options to a parser object, either optparse or argparse.
    """
    # Wrapper function that allows toil to be used with both the optparse and
    # argparse option parsing modules
    addLoggingOptions(parser)  # This adds the logging stuff.
    if isinstance(parser, ArgumentParser):
        def addGroup(headingString, bodyString):
            return parser.add_argument_group(headingString, bodyString).add_argument

        parser.register("type", "bool", lambda v: v.lower() == "true")  # Custom type for arg=True/False.
        _addOptions(addGroup, config)
    else:
        raise RuntimeError("Unanticipated class passed to addOptions(), %s. Expecting "
                           "argparse.ArgumentParser" % parser.__class__)


def getNodeID():
    """
    Return unique ID of the current node (host).

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
                logger.warning(("Exception when trying to read ID file {}. Will try next method to get node ID.").format(idSourceFile), exc_info=True)
            else:
                if len(nodeID.split()) == 1:
                    logger.debug("Obtained node ID {} from file {}".format(nodeID, idSourceFile))
                    break
                else:
                    logger.warning(("Node ID {} from file {} contains spaces. Will try next method to get node ID.").format(nodeID, idSourceFile))
    else:
        nodeIDs = []
        for i_call in range(2):
            nodeID = str(uuid.getnode()).strip()
            if len(nodeID.split()) == 1:
                nodeIDs.append(nodeID)
            else:
                logger.warning("Node ID {} from uuid.getnode() contains spaces".format(nodeID))
        nodeID = ""
        if len(nodeIDs) == 2:
            if nodeIDs[0] == nodeIDs[1]:
                nodeID = nodeIDs[0]
            else:
                logger.warning(
                    "Different node IDs {} received from repeated calls to uuid.getnode(). You should use " \
                    "another method to generate node ID.".format(nodeIDs))

            logger.debug("Obtained node ID {} from uuid.getnode()".format(nodeID))
    if not nodeID:
        logger.warning(
            "Failed to generate stable node ID, returning empty string. If you see this message with a " \
            "work dir on a shared file system when using workers running on multiple nodes, you might experience " \
            "cryptic job failures")
    return nodeID


class Toil(object):
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

    def __enter__(self):
        """
        Derive configuration from the command line options, load the job store and, on restart,
        consolidate the derived configuration with the one from the previous invocation of the
        workflow.
        """
        setLoggingFromOptions(self.options)
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

            # Setup the first wrapper and cache it
            rootJobGraph = rootJob._serialiseFirstJob(self._jobStore)
            self._cacheJob(rootJobGraph)

            self._setProvisioner()
            return self._runMainLoop(rootJobGraph)
        finally:
            self._shutdownBatchSystem()

    def restart(self):
        """
        Restarts a workflow that has been interrupted.

        :return: The root job's return value
        """
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
            rootJobGraph = self._jobStore.clean(jobCache=self._jobCache)
            return self._runMainLoop(rootJobGraph)
        finally:
            self._shutdownBatchSystem()

    def _setProvisioner(self):
        if self.config.provisioner is None:
            self._provisioner = None
        else:
            self._provisioner = clusterFactory(provisioner=self.config.provisioner,
                                               clusterName=None,
                                               zone=None, # read from instance meta-data
                                               nodeStorage=self.config.nodeStorage,
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
        return name + ':' + rest

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

        from toil.batchSystems.registry import batchSystemFactoryFor

        try:
            factory = batchSystemFactoryFor(config.batchSystem)
            batchSystemClass = factory()
        except:
            raise RuntimeError('Unrecognised batch system: %s' % config.batchSystem)

        if not config.disableCaching and not batchSystemClass.supportsWorkerCleanup():
            raise RuntimeError('%s currently does not support shared caching.  Set the '
                               '--disableCaching flag if you want to '
                               'use this batch system.' % config.batchSystem)
        logger.debug('Using the %s' %
                    re.sub("([a-z])([A-Z])", "\g<1> \g<2>", batchSystemClass.__name__).lower())

        return batchSystemClass(**kwargs)

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
                    from toil.batchSystems.singleMachine import SingleMachineBatchSystem
                    if not isinstance(self._batchSystem, SingleMachineBatchSystem):
                        logger.warn('Batch system does not support auto-deployment. The user '
                                    'script %s will have to be present at the same location on '
                                    'every worker.', userScript)
                    userScript = None
        else:
            # This branch is hit on restarts
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

    def importFile(self, srcUrl, sharedFileName=None):
        """
        Imports the file at the given URL into job store.

        See :func:`toil.jobStores.abstractJobStore.AbstractJobStore.importFile` for a
        full description
        """
        self._assertContextManagerUsed()
        return self._jobStore.importFile(srcUrl, sharedFileName=sharedFileName)

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
            for k, v in iteritems(envDict):
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
        self._jobCache = {jobGraph.jobStoreID: jobGraph for jobGraph in self._jobStore.jobs()}
        logger.debug('{} jobs downloaded.'.format(len(self._jobCache)))

    def _cacheJob(self, job):
        """
        Adds given job to current job cache.

        :param toil.jobGraph.JobGraph job: job to be added to current job cache
        """
        self._jobCache[job.jobStoreID] = job

    @staticmethod
    def getWorkflowDir(workflowID, configWorkDir=None):
        """
        Returns a path to the directory where worker directories and the cache will be located
        for this workflow.

        :param str workflowID: Unique identifier for the workflow
        :param str configWorkDir: Value passed to the program using the --workDir flag
        :return: Path to the workflow directory
        :rtype: str
        """
        workDir = configWorkDir or os.getenv('TOIL_WORKDIR') or tempfile.gettempdir()
        if not os.path.exists(workDir):
            raise RuntimeError("The directory specified by --workDir or TOIL_WORKDIR (%s) does not "
                               "exist." % workDir)
        # Create the workflow dir, make it unique to each host in case workDir is on a shared FS.
        # This prevents workers on different nodes from erasing each other's directories.
        workflowDir = os.path.join(workDir, 'toil-%s-%s' % (workflowID, getNodeID()))
        try:
            # Directory creation is atomic
            os.mkdir(workflowDir)
        except OSError as err:
            if err.errno != 17:
                # The directory exists if a previous worker set it up.
                raise
        else:
            logger.debug('Created the workflow directory at %s' % workflowDir)
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
                    region = zoneToRegion(provisioner._zone)
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
            logger.warn("Could not start toil metrics server.")
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
                                                          "prom/node-exporter:0.12.0",
                                                          "-collector.procfs", "/host/proc",
                                                          "-collector.sysfs", "/host/sys",
                                                          "-collector.filesystem.ignored-mount-points",
                                                          "^/(sys|proc|dev|host|etc)($|/)"])
            except subprocess.CalledProcessError:
                logger.warn(
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
            logger.warn("Could not start prometheus/grafana dashboard.")
            return

        # Add prometheus data source
        def requestPredicate(e):
            if isinstance(e, requests.exceptions.ConnectionError):
                return True
            return False

        try:
            for attempt in retry(delays=(0, 1, 1, 4, 16), predicate=requestPredicate):
                with attempt:
                    requests.post('http://localhost:3000/api/datasources', auth=('admin', 'admin'),
                                  data='{"name":"DS_PROMETHEUS","type":"prometheus", \
                                  "url":"http://localhost:9090", "access":"direct"}',
                                  headers={'content-type': 'application/json', "access": "direct"})
        except requests.exceptions.ConnectionError:
            logger.debug(
                "Could not add data source to Grafana dashboard - no metrics will be displayed.")

    def log(self, message):
        if self.mtailProc:
            self.mtailProc.stdin.write(message + "\n")

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


# Nested functions can't have doctests so we have to make this global


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
    return 'cache-' + workflowID


def getDirSizeRecursively(dirPath):
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


def getFileSystemSize(dirPath):
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
