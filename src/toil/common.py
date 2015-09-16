# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from contextlib import contextmanager
import logging
import os
import sys
import cPickle
from argparse import ArgumentParser

from toil.lib.bioio import addLoggingOptions, getLogLevelString, absSymPath
from toil.batchSystems.parasol import ParasolBatchSystem
from toil.batchSystems.gridengine import GridengineBatchSystem
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.batchSystems.lsf import LSFBatchSystem

logger = logging.getLogger( __name__ )

def toilPackageDirPath():
    """
    Returns the absolute path of the directory that corresponds to the top-level toil package. The return value is
    guaranteed to end in '/toil'.
    """
    import toil.jobWrapper

    result = os.path.dirname(absSymPath(toil.jobWrapper.__file__))
    assert result.endswith('/toil')
    return result

class Config(object):
    """
    Class to represent configuration operations for a toil workflow run. 
    """
    def __init__(self):
        #Core options
        self.jobStore = os.path.abspath("./toil")
        self.logLevel = getLogLevelString()
        self.workDir = None
        self.stats = False

        # Because the stats option needs the jobStore to persist past the end of the run,
        # the clean default value depends the specified stats option and is determined in setOptions
        self.clean = None
        
        #Restarting the workflow options
        self.restart = False
        
        #Batch system options
        self.batchSystem = "singleMachine"
        self.scale = 1
        self.masterIP = '127.0.0.1:5050'
        self.parasolCommand = "parasol"
        
        #Resource requirements
        self.defaultMemory = 2147483648
        self.defaultCores = 1
        self.defaultDisk = 2147483648
        self.maxCores = sys.maxint
        self.maxMemory = sys.maxint
        self.maxDisk = sys.maxint
        
        #Retrying/rescuing jobs
        self.retryCount = 0
        self.maxJobDuration = sys.maxint
        self.rescueJobsFrequency = 3600
        
        #Misc
        self.maxLogFileSize=50120
        self.sseKey = None
        self.cseKey = None
        
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
            if x != None:
                if parsingFn != None:
                    x = parsingFn(x)
                if checkFn != None:
                    try:
                        checkFn(x)
                    except AssertionError:
                        raise RuntimeError("The %s option has an invalid value: %s" 
                                           % (varName, x))
                setattr(self, varName, x)
            
        h2b = lambda x : human2bytes(str(x)) #Function to parse integer from string expressed in different formats
        
        def iC(minValue, maxValue=sys.maxint):
            #Returns function to check the a parameter is in a valid range
            def f(x):
                assert x >= minValue and x < maxValue
            return f
        
        #Core options
        setOption("jobStore", parsingFn=lambda x : os.path.abspath(x) 
                  if options.jobStore.startswith('.') else x)
        #TODO: LOG LEVEL STRING
        setOption("workDir")
        setOption("stats")
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
        setOption("scale", float) 
        setOption("masterIP") 
        setOption("parasolCommand")
        
        #Resource requirements
        setOption("defaultMemory", h2b, iC(1))
        setOption("defaultCores", h2b, iC(1))
        setOption("defaultDisk", h2b, iC(1))
        setOption("maxCores", h2b, iC(1))
        setOption("maxMemory", h2b, iC(1))
        setOption("maxDisk", h2b, iC(1))
        
        #Retrying/rescuing jobs
        setOption("retryCount", int, iC(0))
        setOption("maxJobDuration", int, iC(1))
        setOption("rescueJobsFrequency", int, iC(1))
        
        #Misc
        setOption("maxLogFileSize", h2b, iC(1))
        def checkSse(sseKey):
            with open(sseKey) as f:
                assert(len(f.readline().rstrip()) == 32)
        setOption("sseKey", checkFn=checkSse)
        setOption("cseKey", checkFn=checkSse)

def _addOptions(addGroupFn, config):
    #
    #Core options
    #
    addOptionFn = addGroupFn("toil core options", "Options to specify the \
    location of the toil and turn on stats collation about the performance of jobs.")
    #TODO - specify how this works when path is AWS
    addOptionFn('jobStore', type=str,
                      help=("Store in which to place job management files \
                      and the global accessed temporary files"
                      "(If this is a file path this needs to be globally accessible "
                      "by all machines running jobs).\n"
                      "If the store already exists and restart is false an"
                      " ExistingJobStoreException exception will be thrown."))
    addOptionFn("--workDir", dest="workDir", default=None,
                help="Absolute path to directory where temporary files generated during the Toil run should be placed. "
                     "Default is determined by environmental variables (TMPDIR, TEMP, TMP) via mkdtemp")
    addOptionFn("--stats", dest="stats", action="store_true", default=None,
                      help="Records statistics about the toil workflow to be used by 'toil stats'.")
    addOptionFn("--clean", dest="clean", choices=['always', 'onError','never', 'onSuccess'], default=None,
                      help=("Determines the deletion of the jobStore upon completion of the program. "
                            "Choices: 'always', 'onError','never', 'onSuccess'. The --stats option requires "
                            "information from the jobStore upon completion so the jobStore will never be deleted with"
                            "that flag. If you wish to be able to restart the run, choose \'never\' or \'onSuccess\'. "
                            "Default is \'never\' if stats is enabled, and \'onSuccess\' otherwise"))

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
    #TODO - what is this?
    addOptionFn("--scale", dest="scale", default=None,
                help=("A scaling factor to change the value of all submitted tasks's submitted cores. "
                      "Used in singleMachine batch system. default=%s" % config.scale))
    addOptionFn("--masterIP", dest="masterIP", default=None,
                help=("The master node's ip and port number. Used in mesos batch system. default=%s" % config.masterIP))
    addOptionFn("--parasolCommand", dest="parasolCommand", default=None,
                      help="The command to run the parasol program default=%s" % config.parasolCommand)

    #
    #Resource requirements
    #
    addOptionFn = addGroupFn("toil options for cores/memory requirements",
                             "The options to specify default cores/memory requirements (if not specified by the jobs themselves), and to limit the total amount of memory/cores requested from the batch system.")
    addOptionFn("--defaultMemory", dest="defaultMemory", default=None,
                      help=("The default amount of memory to request for a job (in bytes), "
                            "by default is 2^31 = 2 gigabytes, default=%s" % config.defaultMemory))
    addOptionFn("--defaultCores", dest="defaultCores", default=None,
                      help="The number of cpu cores to dedicate a job. default=%s" % config.defaultCores)
    addOptionFn("--defaultDisk", dest="defaultDisk", default=None,
                      help="The amount of disk space to dedicate a job (in bytes). default=%s" % config.defaultDisk)
    addOptionFn("--maxCores", dest="maxCores", default=None,
                      help=("The maximum number of cpu cores to request from the batch system at any "
                            "one time. default=%s" % config.maxCores))
    addOptionFn("--maxMemory", dest="maxMemory", default=None,
                      help=("The maximum amount of memory to request from the batch \
                      system at any one time. default=%s" % config.maxMemory))
    addOptionFn("--maxDisk", dest="maxDisk", default=None,
                      help=("The maximum amount of disk space to request from the batch \
                      system at any one time. default=%s" % config.maxDisk))

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
    addOptionFn("--maxLogFileSize", dest="maxLogFileSize", default=None,
                      help=("The maximum size of a job log file to keep (in bytes), log files larger "
                            "than this will be truncated to the last X bytes. Default is 50 "
                            "kilobytes, default=%s" % config.maxLogFileSize))
    
    addOptionFn("--sseKey", dest="sseKey", default=None,
            help="Path to file containing 32 character key to be used for server-side encryption on awsJobStore. SSE will "
                 "not be used if this flag is not passed.")
    addOptionFn("--cseKey", dest="cseKey", default=None,
                help="Path to file containing 256-bit key to be used for client-side encryption on "
                "azureJobStore. By default, no encryption is used.")

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

def loadBatchSystem(config):
    """
    Load the configured batch system class, instantiate it and return the instance.
    """
    batchSystemClass, kwargs = loadBatchSystemClass(config)
    return createBatchSystem(config, batchSystemClass, kwargs)

def loadBatchSystemClass(config):
    """
    Returns a pair containing the concrete batch system class and a dictionary of keyword arguments to be passed to
    the constructor of that class.

    :param config: the current configuration
    :param key: the name of the configuration attribute that holds the configured batch system name
    """
    batchSystemName = config.batchSystem
    kwargs = dict(config=config,
                  maxCores=config.maxCores,
                  maxMemory=config.maxMemory,
                  maxDisk=config.maxDisk)
    if batchSystemName == 'parasol':
        batchSystemClass = ParasolBatchSystem
        logger.info('Using the parasol batch system')
    elif batchSystemName == 'single_machine' or batchSystemName == 'singleMachine':
        batchSystemClass = SingleMachineBatchSystem
        logger.info('Using the single machine batch system')
    elif batchSystemName == 'gridengine' or batchSystemName == 'gridEngine':
        batchSystemClass = GridengineBatchSystem
        logger.info('Using the grid engine machine batch system')
    elif batchSystemName == 'lsf' or batchSystemName == 'LSF':
        batchSystemClass = LSFBatchSystem
        logger.info('Using the lsf batch system')
    elif batchSystemName == 'mesos' or batchSystemName == 'Mesos':
        from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
        batchSystemClass = MesosBatchSystem
        kwargs["masterIP"] = config.masterIP
        logger.info('Using the mesos batch system')
    else:
        raise RuntimeError('Unrecognised batch system: %s' % batchSystemName)
    return batchSystemClass, kwargs

def createBatchSystem(config, batchSystemClass, kwargs):
    """
    Returns an instance of the given batch system class, or if a big batch system is configured, a batch system
    instance that combines the given class with the configured big batch system.

    :param config: the current configuration
    :param batchSystemClass: the class to be instantiated
    :param kwargs: a list of keyword arguments to be passed to the given class' constructor
    """
    batchSystem = batchSystemClass(**kwargs)
    return batchSystem

def loadJobStore( jobStoreString, config=None ):
    """
    Loads a jobStore.

    :param jobStoreString: see exception message below
    :param config: see AbstractJobStore.__init__
    :return: an instance of a concrete subclass of AbstractJobStore
    :rtype : jobStores.abstractJobStore.AbstractJobStore
    """
    if jobStoreString[ 0 ] in '/.':
        jobStoreString = 'file:' + jobStoreString

    try:
        jobStoreName, jobStoreArgs = jobStoreString.split( ':', 1 )
    except ValueError:
        raise RuntimeError(
            'Job store string must either be a path starting in . or / or a contain at least one '
            'colon separating the name of the job store implementation from an initialization '
            'string specific to that job store. If a path starting in . or / is passed, the file '
            'job store will be used for backwards compatibility.' )

    if jobStoreName == 'file':
        from toil.jobStores.fileJobStore import FileJobStore
        return FileJobStore( jobStoreArgs, config=config )
    elif jobStoreName == 'aws':
        from toil.jobStores.awsJobStore import AWSJobStore
        region, namePrefix = jobStoreArgs.split( ':', 1 )
        return AWSJobStore( region, namePrefix, config=config )
    elif jobStoreName == 'azure':
        from toil.jobStores.azureJobStore import AzureJobStore
        account, namePrefix = jobStoreArgs.split( ':', 1 )
        return AzureJobStore( account, namePrefix, config=config )
    else:
        raise RuntimeError( "Unknown job store implementation '%s'" % jobStoreName )

def serialiseEnvironment(jobStore):
    """
    Puts the environment in a globally accessible pickle file.
    """
    #Dump out the environment of this process in the environment pickle file.
    with jobStore.writeSharedFileStream("environment.pickle") as fileHandle:
        cPickle.dump(os.environ, fileHandle, cPickle.HIGHEST_PROTOCOL)
    logger.info("Written the environment for the jobs to the environment file")

@contextmanager
def setupToil(options, userScript=None):
    """
    Creates the data-structures needed for running a toil.

    :type userScript: toil.resource.ModuleDescriptor
    """
    #Make the default config object
    config = Config()
    #Get options specified by the user
    config.setOptions(options)
    if not options.restart: #Create for the first time
        batchSystemClass, kwargs = loadBatchSystemClass(config)
        #Load the jobStore
        jobStore = loadJobStore(config.jobStore, config=config)
    else:
        #Reload the workflow
        jobStore = loadJobStore(config.jobStore)
        config = jobStore.config
        #Update the earlier config with any options that have been set
        config.setOptions(options)
        #Write these new options back to disk
        jobStore.writeConfigToStore()
        #Get the batch system class
        batchSystemClass, kwargs = loadBatchSystemClass(config)
    if (userScript is not None
        and not userScript.belongsToToil
        and batchSystemClass.supportsHotDeployment()):
        kwargs['userScript'] = userScript.saveAsResourceTo(jobStore)
        # TODO: toil distribution

    batchSystem = createBatchSystem(config, batchSystemClass, kwargs)
    try:
        serialiseEnvironment(jobStore)
        yield (config, batchSystem, jobStore)
    finally:
        logger.debug('Shutting down batch system')
        batchSystem.shutdown()
