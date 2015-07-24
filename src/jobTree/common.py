#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.
from contextlib import contextmanager
import logging
import os
from subprocess import CalledProcessError
import sys
import xml.etree.cElementTree as ET
import cPickle
from argparse import ArgumentParser
from optparse import OptionContainer, OptionGroup

from jobTree.lib.bioio import addLoggingOptions, getLogLevelString, system, absSymPath
from jobTree.batchSystems.parasol import ParasolBatchSystem
from jobTree.batchSystems.gridengine import GridengineBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem
from jobTree.batchSystems.combinedBatchSystem import CombinedBatchSystem
from jobTree.batchSystems.lsf import LSFBatchSystem

logger = logging.getLogger( __name__ )

def gridEngineIsInstalled():
    """
    Returns True if grid-engine is installed, else False.
    """
    try:
        return system("qstat -help") == 0
    except CalledProcessError:
        return False


def parasolIsInstalled():
    """
    Returns True if parasol is installed, else False.
    """
    try:
        return system("parasol status") == 0
    except CalledProcessError:
        return False


####
#Little functions to specify the location of files in the jobTree dir
####

def jobTreePackageDirPath():
    """
    Returns the absolute path of the directory that corresponds to the top-level jobTree package. The return value is
    guaranteed to end in '/jobTree'.
    """
    import jobTree.target

    result = os.path.dirname(absSymPath(jobTree.target.__file__))
    assert result.endswith('/jobTree')
    return result


def _addOptions(addGroupFn, defaultStr):
    addOptionFn = addGroupFn("jobTree core options", "Options to specify the \
    location of the jobTree and turn on stats collation about the performance of jobs.")
    addOptionFn("--jobTree", dest="jobTree", default="./jobTree",
                      help=("Store in which to place job management files \
                      and the global accessed temporary files"
                            "(If this is a file path this needs to be globally accessible by all machines running jobs).\n"
                            "If you pass an existing directory it will check if it's a valid existing "
                            "job tree, then try and restart the jobs in it. The default=%s" % defaultStr))
    addOptionFn("--stats", dest="stats", action="store_true", default=False,
                      help="Records statistics about the job-tree to be used by jobTreeStats. default=%s" % defaultStr)

    addOptionFn = addGroupFn("jobTree options for specifying the batch system",
                             "Allows the specification of the batch system, and arguments to the batch system/big batch system (see below).")
    addOptionFn("--batchSystem", dest="batchSystem", default="singleMachine", #detectQueueSystem(),
                      help=("The type of batch system to run the job(s) with, currently can be "
                            "'singleMachine'/'parasol'/'acidTest'/'gridEngine'/'lsf/mesos/badmesos'. default=%s" % defaultStr))
    addOptionFn("--scale", dest="scale", default=1,
                help=("A scaling factor to change the value of all submitted tasks's submitted cpu. "
                      "Used in singleMachine batch system. default=%s" % defaultStr))
    addOptionFn("--masterIP", dest="masterIP", default='127.0.0.1:5050',
                help=("The master node's ip and port number. Used in mesos batch system. default=%s" % defaultStr))
    addOptionFn("--parasolCommand", dest="parasolCommand", default="parasol",
                      help="The command to run the parasol program default=%s" % defaultStr)

    addOptionFn = addGroupFn("jobTree options for cpu/memory requirements",
                             "The options to specify default cpu/memory requirements (if not specified by the jobs themselves), and to limit the total amount of memory/cpu requested from the batch system.")
    addOptionFn("--defaultMemory", dest="defaultMemory", default=2147483648,
                      help=("The default amount of memory to request for a job (in bytes), "
                            "by default is 2^31 = 2 gigabytes, default=%s" % defaultStr))
    addOptionFn("--defaultCpu", dest="defaultCpu", default=1,
                      help="The number of cpus to dedicate a job. default=%s" % defaultStr)
    addOptionFn("--defaultDisk", dest="defaultDisk", default=2147483648,
                      help="The amount of disk space to dedicate a job (in bytes). default=%s" % defaultStr)
    addOptionFn("--maxCpus", dest="maxCpus", default=sys.maxint,
                      help=("The maximum number of cpus to request from the batch system at any "
                            "one time. default=%s" % defaultStr))
    addOptionFn("--maxMemory", dest="maxMemory", default=sys.maxint,
                      help=("The maximum amount of memory to request from the batch \
                      system at any one time. default=%s" % defaultStr))
    addOptionFn("--maxDisk", dest="maxDisk", default=sys.maxint,
                      help=("The maximum amount of disk space to request from the batch \
                      system at any one time. default=%s" % defaultStr))

    addOptionFn = addGroupFn("jobTree options for rescuing/killing/restarting jobs", \
            "The options for jobs that either run too long/fail or get lost \
            (some batch systems have issues!)")
    addOptionFn("--retryCount", dest="retryCount", default=0,
                      help=("Number of times to retry a failing job before giving up and "
                            "labeling job failed. default=%s" % defaultStr))
    addOptionFn("--maxJobDuration", dest="maxJobDuration", default=str(sys.maxint),
                      help=("Maximum runtime of a job (in seconds) before we kill it "
                            "(this is a lower bound, and the actual time before killing "
                            "the job may be longer). default=%s" % defaultStr))
    addOptionFn("--rescueJobsFrequency", dest="rescueJobsFrequency",
                      help=("Period of time to wait (in seconds) between checking for "
                            "missing/overlong jobs, that is jobs which get lost by the batch system. Expert parameter. (default is set by the batch system)"))
    addOptionFn("--resume", dest="resume", action='store_true', default=False,
                      help=("Pass this flag to resume a previous, incomplete job with an existing jobStore. defualt = %s" % defaultStr))
    addOptionFn("--clean", dest="clean", choices=['always', 'onerror','never'], default='always',
                      help=("Determines the deletion of the jobStore upon completion of the program. If you wish to be able to restart the run, "
                            "choose \'never\'. Default=%s" % defaultStr))

    addOptionFn = addGroupFn("jobTree big batch system options",
                             "jobTree can employ a secondary batch system for running large memory/cpu jobs using the following arguments:")
    addOptionFn("--bigBatchSystem", dest="bigBatchSystem", default=None, #detectQueueSystem(),
                      help=("The batch system to run for jobs with larger memory/cpus requests, currently can be "
                            "'singleMachine'/'parasol'/'acidTest'/'gridEngine'. default=%s" % defaultStr))
    addOptionFn("--bigMemoryThreshold", dest="bigMemoryThreshold", default=sys.maxint, #detectQueueSystem(),
                      help=("The memory threshold above which to submit to the big queue. default=%s" % defaultStr))
    addOptionFn("--bigCpuThreshold", dest="bigCpuThreshold", default=sys.maxint, #detectQueueSystem(),
                      help=("The cpu threshold above which to submit to the big queue. default=%s" % defaultStr))
    addOptionFn("--bigMaxCpus", dest="bigMaxCpus", default=sys.maxint,
                      help=("The maximum number of big batch system cpus to allow at "
                            "one time on the big queue. default=%s" % defaultStr))
    addOptionFn("--bigMaxMemory", dest="bigMaxMemory", default=sys.maxint,
                      help=("The maximum amount of memory to request from the big batch system at any one time. "
                      "default=%s" % defaultStr))

    addOptionFn = addGroupFn("jobTree miscellaneous options", "Miscellaneous options")
    addOptionFn("--jobTime", dest="jobTime", default=30,
                      help=("The approximate time (in seconds) that you'd like a list of child "
                            "jobs to be run serially before being parallelized. "
                            "This parameter allows one to avoid over parallelizing tiny jobs, and "
                            "therefore paying significant scheduling overhead, by running tiny "
                            "jobs in series on a single node/core of the cluster. default=%s" % defaultStr))
    addOptionFn("--maxLogFileSize", dest="maxLogFileSize", default=50120,
                      help=("The maximum size of a job log file to keep (in bytes), log files larger "
                            "than this will be truncated to the last X bytes. Default is 50 "
                            "kilobytes, default=%s" % defaultStr))


def addOptions(parser):
    """
    Adds jobTree options to a parser object, either optparse or argparse.
    """
    # Wrapper function that allows jobTree to be used with both the optparse and 
    # argparse option parsing modules
    addLoggingOptions(parser) # This adds the logging stuff.
    if isinstance(parser, OptionContainer):
        def addGroup(headingString, bodyString):
            group = OptionGroup(parser, headingString, bodyString)
            parser.add_option_group(group)
            return group.add_option

        _addOptions(addGroup, "%default")
        #parser.add_option_group(group)
    elif isinstance(parser, ArgumentParser):
        def addGroup(headingString, bodyString):
            return parser.add_argument_group(headingString, bodyString).add_argument

        _addOptions(addGroup, "%(default)s")
    else:
        raise RuntimeError("Unanticipated class passed to addOptions(), %s. Expecting "
                           "Either optparse.OptionParser or argparse.ArgumentParser" % parser.__class__)


def verifyJobTreeOptions(options):
    """ 
    verifyJobTreeOptions() returns None if all necessary values
    are present in options, otherwise it raises an error.
    It can also serve to validate the values of the options.
    """
    required = ['logLevel', 'batchSystem', 'jobTree']
    for r in required:
        if r not in vars(options):
            raise RuntimeError("Error, there is a missing option (%s), "
                               "did you remember to call Target.addJobTreeOptions()?" % r)
    if options.jobTree is None:
        raise RuntimeError("Specify --jobTree")


def createConfig(options):
    """
    Creates a config object from the options object.
    
    TODO: Make the config object a proper class
    """
    logger.info("Starting to create the job tree setup for the first time")
    config = ET.Element("config")
    config.attrib["log_level"] = getLogLevelString()
    config.attrib["master_ip"] = options.masterIP
    config.attrib["job_store"] = os.path.abspath(options.jobTree) if options.jobTree.startswith('.') else options.jobTree
    config.attrib["parasol_command"] = options.parasolCommand
    config.attrib["try_count"] = str(int(options.retryCount) + 1)
    config.attrib["max_job_duration"] = str(float(options.maxJobDuration))
    config.attrib["batch_system"] = options.batchSystem
    config.attrib["job_time"] = str(float(options.jobTime))
    config.attrib["max_log_file_size"] = str(int(options.maxLogFileSize))
    config.attrib["default_memory"] = str(int(options.defaultMemory))
    config.attrib["default_cpu"] = str(int(options.defaultCpu))
    config.attrib["default_disk"] = str(int(options.defaultDisk))
    config.attrib["max_cpus"] = str(int(options.maxCpus))
    config.attrib["max_memory"] = str(int(options.maxMemory))
    config.attrib["max_disk"] = str(int(options.maxDisk))
    config.attrib["scale"] = str(float(options.scale))
    config.attrib["clean"] = options.clean
    if options.bigBatchSystem is not None:
        config.attrib["big_batch_system"] = options.bigBatchSystem
        config.attrib["big_memory_threshold"] = str(int(options.bigMemoryThreshold))
        config.attrib["big_cpu_threshold"] = str(int(options.bigCpuThreshold))
        config.attrib["big_max_cpus"] = str(int(options.bigMaxCpus))
        config.attrib["big_max_memory"] = str(int(options.bigMaxMemory))
    if options.stats:
        config.attrib["stats"] = ""
    if options.resume:
        config.attrib["resume"] = ""
    return config


def loadBatchSystem(config):
    """
    Load the configured batch system class, instantiate it and return the instance.
    """
    batchSystemClass, kwargs = loadBatchSystemClass(config)
    return createBatchSystem(config, batchSystemClass, kwargs)


def loadBatchSystemClass(config, key="batch_system"):
    """
    Returns a pair containing the concrete batch system class and a dictionary of keyword arguments to be passed to
    the constructor of that class.

    :param config: the current configuration
    :param key: the name of the configuration attribute that holds the configured batch system name
    """
    batchSystemName = config.attrib[key]
    kwargs = dict(config=config,
                  maxCpus=int(config.attrib['max_cpus']),
                  maxMemory=int(config.attrib['max_memory']),
                  maxDisk=int(config.attrib['max_disk']))
    if batchSystemName == 'parasol':
        batchSystemClass = ParasolBatchSystem
        logger.info('Using the parasol batch system')
    elif batchSystemName == 'single_machine' or batchSystemName == 'singleMachine':
        batchSystemClass = SingleMachineBatchSystem
        logger.info('Using the single machine batch system')
    elif batchSystemName == 'gridengine' or batchSystemName == 'gridEngine':
        batchSystemClass = GridengineBatchSystem
        logger.info('Using the grid engine machine batch system')
    elif batchSystemName == 'acid_test' or batchSystemName == 'acidTest':
        # The chance that a job does not complete after 32 goes in one in 4 billion, so you need a lot of jobs
        # before this becomes probable
        config.attrib['try_count'] = str(32)
        batchSystemClass = SingleMachineBatchSystem
        kwargs['badWorker'] = True
    elif batchSystemName == 'lsf' or batchSystemName == 'LSF':
        batchSystemClass = LSFBatchSystem
        logger.info('Using the lsf batch system')
    elif batchSystemName == 'mesos' or batchSystemName == 'Mesos':
        from jobTree.batchSystems.mesos.batchSystem import MesosBatchSystem
        batchSystemClass = MesosBatchSystem
        kwargs["masterIP"] = config.attrib["master_ip"]
        logger.info('Using the mesos batch system')
    elif batchSystemName == 'badmesos' or batchSystemName == 'badMesos':
        from jobTree.batchSystems.mesos.batchSystem import MesosBatchSystem
        batchSystemClass = MesosBatchSystem
        kwargs["masterIP"] = config.attrib["master_ip"]
        kwargs['useBadExecutor'] = True
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
    :param args: a list of arguments to be passed to the given class' constructor
    :param kwargs: a list of keyword arguments to be passed to the given class' constructor
    """
    batchSystem = batchSystemClass(**kwargs)
    if "big_batch_system" in config.attrib:
        bigBatchSystemClass, kwargs = loadBatchSystemClass(config, key="big_batch_system")
        bigMemoryThreshold = int(config.attrib["big_memory_threshold"])
        bigCpuThreshold = int(config.attrib["big_cpu_threshold"])
        kwargs['maxCpus'] = int(config.attrib["big_max_cpus"])
        kwargs['maxMemory'] = int(config.attrib["big_max_memory"])
        bigBatchSystem = bigBatchSystemClass(**kwargs)
        # noinspection PyUnusedLocal
        def batchSystemChoiceFn(command, memory, cpu):
            return memory <= bigMemoryThreshold and cpu <= bigCpuThreshold

        batchSystem = CombinedBatchSystem(config,
                                          batchSystem1=batchSystem,
                                          batchSystem2=bigBatchSystem,
                                          batchSystemChoiceFn=batchSystemChoiceFn)
    return batchSystem


def addBatchSystemConfigOptions(config, batchSystemClass, options):
    """
    Adds configurations options to the config derived from the decision about the batch system.
    """
    #Set the parameters determining the polling frequency of the system.  
    config.attrib["rescue_jobs_frequency"] = str(float(
        batchSystemClass.getRescueJobFrequency()
        if options.rescueJobsFrequency is None
        else options.rescueJobsFrequency))


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
        from jobTree.jobStores.fileJobStore import FileJobStore

        return FileJobStore( jobStoreArgs, config=config )
    elif jobStoreName == 'aws':
        from jobTree.jobStores.awsJobStore import AWSJobStore
        region, namePrefix = jobStoreArgs.split( ':', 1 )
        return AWSJobStore( region, namePrefix, config=config )
    else:
        raise RuntimeError( "Unknown job store implementation '%s'" % jobStoreName )


def serialiseEnvironment(jobStore):
    """
    Puts the environment in a globally accessible pickle file.
    """
    #Dump out the environment of this process in the environment pickle file.
    with jobStore.writeSharedFileStream("environment.pickle") as fileHandle:
        cPickle.dump(os.environ, fileHandle)
    logger.info("Written the environment for the jobs to the environment file")


@contextmanager
def setupJobTree(options, userScript=None):
    """
    Creates the data-structures needed for running a jobTree.

    :type userScript: jobTree.resource.ModuleDescriptor
    """
    verifyJobTreeOptions(options)
    config = createConfig(options)
    batchSystemClass, kwargs = loadBatchSystemClass(config)
    addBatchSystemConfigOptions(config, batchSystemClass, options)
    jobStore = loadJobStore(config.attrib["job_store"], config=config)
    if (userScript is not None
        and not userScript.belongsToJobTree
        and batchSystemClass.supportsHotDeployment()):
        kwargs['userScript'] = userScript.saveAsResourceTo(jobStore)
        # TODO: jobTree distribution
    batchSystem = createBatchSystem(config, batchSystemClass, kwargs)
    try:
        serialiseEnvironment(jobStore)
        yield (config, batchSystem, jobStore)
    finally:
        batchSystem.shutdown()
