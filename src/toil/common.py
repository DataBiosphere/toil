# Copyright (C) 2015-2016 Regents of the University of California
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

import cPickle
import logging
import os
import re
import sys
import tempfile
import time
from argparse import ArgumentParser

from bd2k.util.exceptions import require
from bd2k.util.humanize import bytes2human

from toil.lib.bioio import addLoggingOptions, getLogLevelString, setLoggingFromOptions
from toil.realtimeLogger import RealtimeLogger
from toil.resource import Resource

logger = logging.getLogger(__name__)


class Config(object):
    """
    Class to represent configuration operations for a toil workflow run. 
    """
    def __init__(self):
        # Core options
        self.workflowID = None
        """This attribute uniquely identifies the job store and therefore the workflow. It is
        necessary in order to distinguish between two consequitive workflows for which
        self.jobStore is the same, e.g. when a job store name is reused after a previous run has
        finished sucessfully and its job store has been clean up."""
        self.workflowAttemptNumber = None
        self.jobStore = None
        self.logLevel = getLogLevelString()
        self.workDir = None
        self.stats = False

        # Because the stats option needs the jobStore to persist past the end of the run,
        # the clean default value depends the specified stats option and is determined in setOptions
        self.clean = None
        self.cleanWorkDir = None

        #Restarting the workflow options
        self.restart = False

        #Batch system options
        self.batchSystem = "singleMachine"
        self.scale = 1
        self.mesosMasterAddress = 'localhost:5050'
        self.parasolCommand = "parasol"
        self.parasolMaxBatches = 10000
        self.environment = {}

        #Autoscaling options
        self.provisioner = None
        self.nodeType = None
        self.nodeOptions = None
        self.minNodes = 0
        self.maxNodes = 10
        self.preemptableNodeType = None
        self.preemptableNodeOptions = None
        self.minPreemptableNodes = 0
        self.maxPreemptableNodes = 0
        self.alphaPacking = 0.8
        self.betaInertia = 1.2
        self.scaleInterval = 10
        self.preemptableCompensation = 0.0

        #Resource requirements
        self.defaultMemory = 2147483648
        self.defaultCores = 1
        self.defaultDisk = 2147483648
        self.readGlobalFileMutableByDefault = False
        self.defaultPreemptable = False
        self.maxCores = sys.maxint
        self.maxMemory = sys.maxint
        self.maxDisk = sys.maxint

        #Retrying/rescuing jobs
        self.retryCount = 0
        self.maxJobDuration = sys.maxint
        self.rescueJobsFrequency = 3600

        #Misc
        self.disableCaching = False
        self.maxLogFileSize=50120
        self.sseKey = None
        self.cseKey = None
        self.servicePollingInterval = 60
        self.useAsync = True


        #Debug options
        self.badWorker = 0.0
        self.badWorkerFailInterval = 0.01

    def setOptions(self, options):
        """
        Creates a config object from the options object.
        """
        from bd2k.util.humanize import human2bytes #This import is used to convert
        #from human readable quantites to integers
        def setOption(varName, parsingFn=None, checkFn=None):
            #If options object has the option "varName" specified
            #then set the "varName" attrib to this value in the config object
            x = getattr(options, varName, None)
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
        h2b = lambda x : human2bytes(str(x))

        def iC(minValue, maxValue=sys.maxint):
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

        def parseJobStore(s):
            name, rest = Toil.parseLocator(s)
            if name == 'file':
                # We need to resolve relative paths early, on the leader, because the worker process
                # may have a different working directory than the leader, e.g. under Mesos.
                return Toil.buildLocator(name, os.path.abspath(rest))
            else:
                return s

        #Core options
        setOption("jobStore", parsingFn=parseJobStore)
        #TODO: LOG LEVEL STRING
        setOption("workDir")
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

        #Restarting the workflow options
        setOption("restart")

        #Batch system options
        setOption("batchSystem")
        setOption("scale", float, fC(0.0))
        setOption("mesosMasterAddress")
        setOption("parasolCommand")
        setOption("parasolMaxBatches", int, iC(1))

        setOption("environment", parseSetEnv)

        #Autoscaling options
        setOption("provisioner")
        setOption("nodeType")
        setOption("nodeOptions")
        setOption("minNodes", int)
        setOption("maxNodes", int)
        setOption("preemptableNodeType")
        setOption("preemptableNodeOptions")
        setOption("minPreemptableNodes", int)
        setOption("maxPreemptableNodes", int)
        setOption("alphaPacking", float)
        setOption("betaInertia", float)
        setOption("scaleInterval", float)

        setOption("preemptableCompensation", float)
        require(0.0 <= self.preemptableCompensation <= 1.0,
                '--preemptableCompensation (%f) must be >= 0.0 and <= 1.0',
                self.preemptableCompensation)

        # Resource requirements
        setOption("defaultMemory", h2b, iC(1))
        setOption("defaultCores", float, fC(1.0))
        setOption("defaultDisk", h2b, iC(1))
        setOption("readGlobalFileMutableByDefault")
        setOption("maxCores", int, iC(1))
        setOption("maxMemory", h2b, iC(1))
        setOption("maxDisk", h2b, iC(1))
        setOption("defaultPreemptable")

        #Retrying/rescuing jobs
        setOption("retryCount", int, iC(0))
        setOption("maxJobDuration", int, iC(1))
        setOption("rescueJobsFrequency", int, iC(1))

        #Misc
        setOption("disableCaching")
        setOption("maxLogFileSize", h2b, iC(1))
        def checkSse(sseKey):
            with open(sseKey) as f:
                assert(len(f.readline().rstrip()) == 32)
        setOption("sseKey", checkFn=checkSse)
        setOption("cseKey", checkFn=checkSse)
        setOption("servicePollingInterval", float, fC(0.0))

        #Debug options
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
                       "azure:<account>:<prefix>\n\n"
                       "google:<project_id>:<prefix> TODO: explain\n\n"
                       "For backwards compatibility, you may also specify ./foo (equivalent to "
                       "file:./foo or just file:foo) or /bar (equivalent to file:/bar).")

def _addOptions(addGroupFn, config):
    #
    #Core options
    #
    addOptionFn = addGroupFn("toil core options",
                             "Options to specify the location of the Toil workflow and turn on "
                             "stats collation about the performance of jobs.")
    addOptionFn('jobStore', type=str,
                help="The location of the job store for the workflow. " + jobStoreLocatorHelp)
    addOptionFn("--workDir", dest="workDir", default=None,
                help="Absolute path to directory where temporary files generated during the Toil "
                     "run should be placed. Temp files and folders will be placed in a directory "
                     "toil-<workflowID> within workDir (The workflowID is generated by Toil and "
                     "will be reported in the workflow logs. Default is determined by the "
                     "user-defined environmental variable TOIL_TEMPDIR, or the environment "
                     "variables (TMPDIR, TEMP, TMP) via mkdtemp. This directory needs to exist on "
                     "all machines running jobs.")
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
                help=("Determines deletion of temporary worker directory upon completion of a job. Choices: 'always', "
                      "'never', 'onSuccess'. Default = always. WARNING: This option should be changed for debugging "
                      "only. Running a full pipeline with this option could fill your disk with intermediate data."))
    #
    #Restarting the workflow options
    #
    addOptionFn = addGroupFn("toil options for restarting an existing workflow",
                             "Allows the restart of an existing workflow")
    addOptionFn("--restart", dest="restart", default=None, action="store_true",
                help="If --restart is specified then will attempt to restart existing workflow "
                "at the location pointed to by the --jobStore option. Will raise an exception if the workflow does not exist")

    #
    #Batch system options
    #

    addOptionFn = addGroupFn("toil options for specifying the batch system",
                             "Allows the specification of the batch system, and arguments to the batch system/big batch system (see below).")
    addOptionFn("--batchSystem", dest="batchSystem", default=None,
                      help=("The type of batch system to run the job(s) with, currently can be one "
                            "of singleMachine, parasol, gridEngine, lsf or mesos'. default=%s" % config.batchSystem))
    addOptionFn("--scale", dest="scale", default=None,
                help=("A scaling factor to change the value of all submitted tasks's submitted cores. "
                      "Used in singleMachine batch system. default=%s" % config.scale))
    addOptionFn("--mesosMaster", dest="mesosMasterAddress", default=None,
                help=("The host and port of the Mesos master separated by colon. default=%s" % config.mesosMasterAddress))
    addOptionFn("--parasolCommand", dest="parasolCommand", default=None,
                      help="The name or path of the parasol program. Will be looked up on PATH "
                           "unless it starts with a slashdefault=%s" % config.parasolCommand)
    addOptionFn("--parasolMaxBatches", dest="parasolMaxBatches", default=None,
                help="Maximum number of job batches the Parasol batch is allowed to create. One "
                     "batch is created for jobs with a a unique set of resource requirements. "
                     "default=%i" % config.parasolMaxBatches)

    #
    #Auto scaling options
    #
    addOptionFn = addGroupFn("toil options for autoscaling the cluster of worker nodes",
                             "Allows the specification of the minimum and maximum number of nodes "
                             "in an autoscaled cluster, as well as parameters to control the "
                             "level of provisioning.")

    addOptionFn("--provisioner", dest="provisioner", choices=['cgcloud'],
                help="The provisioner for cluster auto-scaling. Currently only the cgcloud "
                     "provisioner exists. The default is %s." % config.provisioner)

    for preemptable in (False, True):
        def _addOptionFn(*name, **kwargs):
            name = list(name)
            if preemptable:
                name.insert(-1, 'preemptable' )
            name = ''.join((s[0].upper() + s[1:]) if i else s for i, s in enumerate(name))
            terms = re.compile(r'\{([^{}]+)\}')
            _help = kwargs.pop('help')
            _help = ''.join((term.split('|') * 2)[int(preemptable)] for term in terms.split(_help))
            addOptionFn('--' + name, dest=name,
                        help=_help + ' The default is %s.' % getattr(config, name),
                        **kwargs)

        _addOptionFn('nodeType', metavar='TYPE',
                     help="Node type for {non-|}preemptable nodes. The syntax depends on the "
                          "provisioner used. For the cgcloud provisioner this is the name of an "
                          "EC2 instance type{|, followed by a colon and the price in dollar to "
                          "bid for a spot instance}, for example 'c3.8xlarge{|:0.42}'.")
        _addOptionFn('nodeOptions', metavar='OPTIONS',
                     help="Provisioning options for the {non-|}preemptable node type. The syntax "
                          "depends on the provisioner used. The CGCloud provisioner doesn't "
                          "currently support any node options.")
        for p, q in [('min', 'Minimum'), ('max', 'Maximum')]:
            _addOptionFn(p, 'nodes', default=None, metavar='NUM',
                         help=q + " number of {non-|}preemptable nodes in the cluster, if using "
                                  "auto-scaling.")

    # TODO: DESCRIBE THE FOLLOWING TWO PARAMETERS
    addOptionFn("--alphaPacking", dest="alphaPacking", default=None,
                help=(" default=%s" % config.alphaPacking))
    addOptionFn("--betaInertia", dest="betaInertia", default=None,
                help=(" default=%s" % config.betaInertia))
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

    #
    #Resource requirements
    #
    addOptionFn = addGroupFn("toil options for cores/memory requirements",
                             "The options to specify default cores/memory requirements (if not "
                             "specified by the jobs themselves), and to limit the total amount of "
                             "memory/cores requested from the batch system.")
    addOptionFn('--defaultMemory', dest='defaultMemory', default=None, metavar='INT',
                help='The default amount of memory to request for a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Standard '
                     'suffixes like K, Ki, M, Mi, G or Gi are supported. Default is %s' %
                     bytes2human( config.defaultMemory, symbols='iec' ))
    addOptionFn('--defaultCores', dest='defaultCores', default=None, metavar='FLOAT',
                help='The default number of CPU cores to dedicate a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Fractions of a '
                     'core (for example 0.1) are supported on some batch systems, namely Mesos '
                     'and singleMachine. Default is %.1f ' % config.defaultCores)
    addOptionFn('--defaultDisk', dest='defaultDisk', default=None, metavar='INT',
                help='The default amount of disk space to dedicate a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Standard '
                     'suffixes like K, Ki, M, Mi, G or Gi are supported. Default is %s' %
                     bytes2human( config.defaultDisk, symbols='iec' ))
    assert not config.defaultPreemptable, 'User would be unable to reset config.defaultPreemptable'
    addOptionFn('--defaultPreemptable', dest='defaultPreemptable', action='store_true')
    addOptionFn("--readGlobalFileMutableByDefault", dest="readGlobalFileMutableByDefault",
                action='store_true', default=None, help='Toil disallows modification of read '
                                                        'global files by default. This flag makes '
                                                        'it makes read file mutable by default, '
                                                        'however it also defeats the purpose of '
                                                        'shared caching via hard links to save '
                                                        'space. Default is False')
    addOptionFn('--maxCores', dest='maxCores', default=None, metavar='INT',
                help='The maximum number of CPU cores to request from the batch system at any one '
                     'time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. Default '
                     'is %s' % bytes2human(config.maxCores, symbols='iec'))
    addOptionFn('--maxMemory', dest='maxMemory', default=None, metavar='INT',
                help="The maximum amount of memory to request from the batch system at any one "
                     "time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. Default "
                     "is %s" % bytes2human( config.maxMemory, symbols='iec'))
    addOptionFn('--maxDisk', dest='maxDisk', default=None, metavar='INT',
                help='The maximum amount of disk space to request from the batch system at any '
                     'one time. Standard suffixes like K, Ki, M, Mi, G or Gi are supported. '
                     'Default is %s' % bytes2human(config.maxDisk, symbols='iec'))

    #
    #Retrying/rescuing jobs
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
                            "missing/overlong jobs, that is jobs which get lost by the batch system. Expert parameter. default=%s" % config.rescueJobsFrequency))

    #
    #Misc options
    #
    addOptionFn = addGroupFn("toil miscellaneous options", "Miscellaneous options")
    addOptionFn('--disableCaching', dest='disableCaching', action='store_true', default=False,
                help='Disables caching in the file store. This flag must be set to use '
                     'a batch system that does not support caching such as Grid Engine, Parasol, '
                     'LSF, or Slurm')
    addOptionFn("--maxLogFileSize", dest="maxLogFileSize", default=None,
                      help=("The maximum size of a job log file to keep (in bytes), log files larger "
                            "than this will be truncated to the last X bytes. Default is 50 "
                            "kilobytes, default=%s" % config.maxLogFileSize))
    addOptionFn("--realTimeLogging", dest="realTimeLogging", action="store_true", default=False,
                help="Enable real-time logging from workers to masters")

    addOptionFn("--sseKey", dest="sseKey", default=None,
            help="Path to file containing 32 character key to be used for server-side encryption on awsJobStore. SSE will "
                 "not be used if this flag is not passed.")
    addOptionFn("--cseKey", dest="cseKey", default=None,
                help="Path to file containing 256-bit key to be used for client-side encryption on "
                "azureJobStore. By default, no encryption is used.")
    addOptionFn("--setEnv", '-e', metavar='NAME=VALUE or NAME',
                dest="environment", default=[], action="append",
                help="Set an environment variable early on in the worker. If VALUE is omitted, "
                     "it will be looked up in the current environment. Independently of this "
                     "option, the worker will try to emulate the leader's environment before "
                     "running a job. Using this option, a variable can be injected into the "
                     "worker process itself before it is started.")
    addOptionFn("--servicePollingInterval", dest="servicePollingInterval", default=None,
                help="Interval of time service jobs wait between polling for the existence"
                " of the keep-alive flag (defailt=%s)" % config.servicePollingInterval)
    #
    #Debug options
    #
    addOptionFn = addGroupFn("toil debug options", "Debug options")
    addOptionFn("--badWorker", dest="badWorker", default=None,
                      help=("For testing purposes randomly kill 'badWorker' proportion of jobs using SIGKILL, default=%s" % config.badWorker))
    addOptionFn("--badWorkerFailInterval", dest="badWorkerFailInterval", default=None,
                      help=("When killing the job pick uniformly within the interval from 0.0 to "
                            "'badWorkerFailInterval' seconds after the worker starts, default=%s" % config.badWorkerFailInterval))

def addOptions(parser, config=Config()):
    """
    Adds toil options to a parser object, either optparse or argparse.
    """
    # Wrapper function that allows toil to be used with both the optparse and
    # argparse option parsing modules
    addLoggingOptions(parser) # This adds the logging stuff.
    if isinstance(parser, ArgumentParser):
        def addGroup(headingString, bodyString):
            return parser.add_argument_group(headingString, bodyString).add_argument
        _addOptions(addGroup, config)
    else:
        raise RuntimeError("Unanticipated class passed to addOptions(), %s. Expecting "
                           "argparse.ArgumentParser" % parser.__class__)


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
        self._jobStore = None
        self._batchSystem = None
        self._provisioner = None
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
                logger.info("Attempting to delete the job store")
                self._jobStore.destroy()
                logger.info("Successfully deleted the job store")
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
        if self.config.restart:
            raise ToilRestartException('A Toil workflow can only be started once. Use '
                                       'Toil.restart() to resume it.')

        self._batchSystem = self.createBatchSystem(self.config,
                                                   jobStore=self._jobStore,
                                                   userScript=rootJob.getUserScript())
        try:
            self._setBatchSystemEnvVars()
            self._serialiseEnv()
            self._cacheAllJobs()

            # Make a file to store the root job's return value in
            rootJobReturnValueID = self._jobStore.getEmptyFileStoreID()

            # Add the root job return value as a promise
            rootJob._rvs[()].append(rootJobReturnValueID)

            # Write the name of the promise file in a shared file
            with self._jobStore.writeSharedFileStream("rootJobReturnValue") as fH:
                fH.write(rootJobReturnValueID)

            # Setup the first wrapper and cache it
            job = rootJob._serialiseFirstJob(self._jobStore)
            self._cacheJob(job)

            self._setProvisioner()
            return self._runMainLoop(job)
        finally:
            self._shutdownBatchSystem()

    def restart(self):
        """
        Restarts a workflow that has been interrupted. This method should be called if and only
        if a workflow has previously been started and has not finished.

        :return: The root job's return value
        """
        self._assertContextManagerUsed()
        if not self.config.restart:
            raise ToilRestartException('A Toil workflow must be initiated with Toil.start(), '
                                       'not restart().')

        self._batchSystem = self.createBatchSystem(self.config, jobStore=self._jobStore)
        try:
            self._setBatchSystemEnvVars()
            self._serialiseEnv()
            self._cacheAllJobs()
            self._setProvisioner()
            rootJob = self._jobStore.clean(jobCache=self._jobCache)
            return self._runMainLoop(rootJob)
        finally:
            self._shutdownBatchSystem()

    def _setProvisioner(self):
        if self.config.provisioner is None:
            self._provisioner = None
        elif self.config.provisioner == 'cgcloud':
            logger.info('Using cgcloud provisioner.')
            from toil.provisioners.cgcloud.provisioner import CGCloudProvisioner
            self._provisioner = CGCloudProvisioner(self.config, self._batchSystem)
        else:
            # Command line parser shold have checked argument validity already
            assert False, self.config.provisioner

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
        elif name == 'azure':
            from toil.jobStores.azureJobStore import AzureJobStore
            return AzureJobStore(rest)
        elif name == 'google':
            from toil.jobStores.googleJobStore import GoogleJobStore
            projectID, namePrefix = rest.split(':', 1)
            return GoogleJobStore(namePrefix, projectID)
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
    def createBatchSystem(config, jobStore=None, userScript=None):
        """
        Creates an instance of the batch system specified in the given config. If a job store and
        a user script are given then the user script can be hot deployed into the workflow.

        :param toil.common.Config config: the current configuration

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: an instance of a jobStore

        :param ModuleDescriptor userScript: a handle to the Python module defining the root job

        :return: an instance of a concrete subclass of AbstractBatchSystem

        :rtype: batchSystems.abstractBatchSystem.AbstractBatchSystem
        """
        kwargs = dict(config=config,
                      maxCores=config.maxCores,
                      maxMemory=config.maxMemory,
                      maxDisk=config.maxDisk)

        if config.batchSystem == 'parasol':
            from toil.batchSystems.parasol import ParasolBatchSystem
            batchSystemClass = ParasolBatchSystem

        elif config.batchSystem == 'single_machine' or config.batchSystem == 'singleMachine':
            from toil.batchSystems.singleMachine import SingleMachineBatchSystem
            batchSystemClass = SingleMachineBatchSystem

        elif config.batchSystem == 'gridengine' or config.batchSystem == 'gridEngine':
            from toil.batchSystems.gridengine import GridengineBatchSystem
            batchSystemClass = GridengineBatchSystem

        elif config.batchSystem == 'lsf' or config.batchSystem == 'LSF':
            from toil.batchSystems.lsf import LSFBatchSystem
            batchSystemClass = LSFBatchSystem

        elif config.batchSystem == 'mesos' or config.batchSystem == 'Mesos':
            from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
            batchSystemClass = MesosBatchSystem

            kwargs['masterAddress'] = config.mesosMasterAddress

        elif config.batchSystem == 'slurm' or config.batchSystem == 'Slurm':
            from toil.batchSystems.slurm import SlurmBatchSystem
            batchSystemClass = SlurmBatchSystem

        else:
            raise RuntimeError('Unrecognised batch system: %s' % config.batchSystem)

        if not config.disableCaching and not batchSystemClass.supportsWorkerCleanup():
            raise RuntimeError('%s currently does not support shared caching.  Set the '
                               '--disableCaching flag if you want to '
                               'use this batch system.' % config.batchSystem)
        logger.info('Using the %s' %
                    re.sub("([a-z])([A-Z])", "\g<1> \g<2>", batchSystemClass.__name__).lower())

        if jobStore is not None:
            if userScript is not None:
                if not userScript.belongsToToil and batchSystemClass.supportsHotDeployment():
                    userScriptResource = userScript.saveAsResourceTo(jobStore)
                    with jobStore.writeSharedFileStream('userScript') as f:
                        f.write(userScriptResource.pickle())
                    kwargs['userScript'] = userScriptResource
            else:
                from toil.jobStores.abstractJobStore import NoSuchFileException
                try:
                    with jobStore.readSharedFileStream('userScript') as f:
                        userScriptResource = Resource.unpickle(f.read())
                    kwargs['userScript'] = userScriptResource
                except NoSuchFileException:
                    pass

        return batchSystemClass(**kwargs)

    def importFile(self, srcUrl, sharedFileName=None):
        self._assertContextManagerUsed()
        return self._jobStore.importFile(srcUrl, sharedFileName=sharedFileName)

    def exportFile(self, jobStoreFileID, dstUrl):
        self._assertContextManagerUsed()
        self._jobStore.exportFile(jobStoreFileID, dstUrl)

    def _setBatchSystemEnvVars(self):
        """
        Sets the environment variables required by the job store and those passed on command line.
        """
        for envDict in (self._jobStore.getEnv(), self.config.environment):
            for k, v in envDict.iteritems():
                self._batchSystem.setEnv(k, v)

    def _serialiseEnv(self):
        """
        Puts the environment in a globally accessible pickle file.
        """
        # Dump out the environment of this process in the environment pickle file.
        with self._jobStore.writeSharedFileStream("environment.pickle") as fileHandle:
            cPickle.dump(os.environ, fileHandle, cPickle.HIGHEST_PROTOCOL)
        logger.info("Written the environment for the jobs to the environment file")

    def _cacheAllJobs(self):
        """
        Downloads all jobs in the current job store into self.jobCache.
        """
        logger.info('Caching all jobs in job store')
        self._jobCache = {jobWrapper.jobStoreID: jobWrapper for jobWrapper in self._jobStore.jobs()}
        logger.info('{} jobs downloaded.'.format(len(self._jobCache)))

    def _cacheJob(self, job):
        """
        Adds given job to current job cache.

        :param toil.jobWrapper.JobWrapper job: job to be added to current job cache
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
        # Create the workflow dir
        workflowDir = os.path.join(workDir, 'toil-%s' % workflowID)
        try:
            # Directory creation is atomic
            os.mkdir(workflowDir)
        except OSError as err:
            if err.errno != 17:
                # The directory exists if a previous worker set it up.
                raise
        else:
            logger.info('Created the workflow directory at %s' % workflowDir)
        return workflowDir

    def _runMainLoop(self, rootJob):
        """
        Runs the main loop with the given job.
        :param toil.job.Job rootJob: The root job for the workflow.
        :rtype: Any
        """
        with RealtimeLogger(self._batchSystem,
                            level=self.options.logLevel if self.options.realTimeLogging else None):
            # FIXME: common should not import from leader
            from toil.leader import mainLoop
            return mainLoop(config=self.config,
                            batchSystem=self._batchSystem,
                            provisioner=self._provisioner,
                            jobStore=self._jobStore,
                            rootJobWrapper=rootJob,
                            jobCache=self._jobCache)

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


class ToilRestartException(Exception):
    def __init__(self, message):
        super(ToilRestartException, self).__init__(message)


class ToilContextManagerException(Exception):
    def __init__(self):
        super(ToilContextManagerException, self).__init__(
            'This method cannot be called outside the "with Toil(...)" context manager.')

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


def cacheDirName(workflowID):
    """
    :return: Name of the cache directory.
    """
    return 'cache-' + workflowID
