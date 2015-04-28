"""Wrapper functions for running the various programs in the jobTree package.
"""

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

import os
import sys 
import xml.etree.cElementTree as ET
import cPickle
from argparse import ArgumentParser
from optparse import OptionParser, OptionContainer, OptionGroup
from sonLib.bioio import logger, addLoggingOptions, getLogLevelString, system, absSymPath
from jobTree.batchSystems.parasol import ParasolBatchSystem
from jobTree.batchSystems.gridengine import GridengineBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem, badWorker
from jobTree.batchSystems.combinedBatchSystem import CombinedBatchSystem
from jobTree.batchSystems.lsf import LSFBatchSystem
from jobTree.jobStores.fileJobStore import FileJobStore

def runJobTreeStats(jobTree, outputFile):
    system("jobTreeStats --jobTree %s --outputFile %s" % (jobTree, outputFile))
    logger.info("Ran the job-tree stats command apparently okay")
    
def runJobTreeStatusAndFailIfNotComplete(jobTreeDir):
    command = "jobTreeStatus --jobTree %s --failIfNotComplete --verbose" % jobTreeDir
    system(command)
    
def gridEngineIsInstalled():
    """Returns True if grid-engine is installed, else False.
    """
    try:
        return system("qstat -help") == 0
    except RuntimeError:
        return False
    
def parasolIsInstalled():
    """Returns True if parasol is installed, else False.
    """
    try:
        return system("parasol status") == 0
    except RuntimeError:
        return False

####
#Little functions to specify the location of files in the jobTree dir
####
    
def workflowRootPath():
    """Function for finding external location.
    """
    import jobTree.src.target
    i = absSymPath(jobTree.src.target.__file__)
    return os.path.split(os.path.split(i)[0])[0]

def _addOptions(addGroupFn, defaultStr):
    addOptionFn = addGroupFn("jobTree core options", "Options to specify the \
    location of the jobTree and turn on stats collation about the performance of jobs.")
    addOptionFn("--jobTree", dest="jobTree", default="./jobTree",
                      help=("Directory in which to place job management files \
                      and the global accessed temporary file directories"
                            "(this needs to be globally accessible by all machines running jobs).\n"
                            "If you pass an existing directory it will check if it's a valid existing "
                            "job tree, then try and restart the jobs in it. The default=%s" % defaultStr))
    addOptionFn("--stats", dest="stats", action="store_true", default=False,
                      help="Records statistics about the job-tree to be used by jobTreeStats. default=%s" % defaultStr)
    
    addOptionFn = addGroupFn("jobTree options for specifying the batch system", "Allows the specification of the batch system, and arguments to the batch system/big batch system (see below).")
    addOptionFn("--batchSystem", dest="batchSystem", default="singleMachine", #detectQueueSystem(),
                      help=("The type of batch system to run the job(s) with, currently can be "
                            "'singleMachine'/'parasol'/'acidTest'/'gridEngine'/'lsf'. default=%s" % defaultStr))
    addOptionFn("--maxThreads", dest="maxThreads", default=4,
                      help=("The maximum number of threads (technically processes at this point) to use when running in single "
                            "machine mode. Increasing this will allow more jobs to run concurrently when running on a single machine. default=%s" % defaultStr))
    addOptionFn("--parasolCommand", dest="parasolCommand", default="parasol",
                      help="The command to run the parasol program default=%s" % defaultStr)
    
    addOptionFn = addGroupFn("jobTree options for cpu/memory requirements", "The options to specify default cpu/memory requirements (if not specified by the jobs themselves), and to limit the total amount of memory/cpu requested from the batch system.")
    addOptionFn("--defaultMemory", dest="defaultMemory", default=2147483648,
                      help=("The default amount of memory to request for a job (in bytes), "
                            "by default is 2^31 = 2 gigabytes, default=%s" % defaultStr))
    addOptionFn("--defaultCpu", dest="defaultCpu", default=1,
                      help="The default the number of cpus to dedicate a job. default=%s" % defaultStr)
    addOptionFn("--maxCpus", dest="maxCpus", default=sys.maxint,
                      help=("The maximum number of cpus to request from the batch system at any "
                            "one time. default=%s" % defaultStr))
    addOptionFn("--maxMemory", dest="maxMemory", default=sys.maxint,
                      help=("The maximum amount of memory to request from the batch \
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
    
    addOptionFn = addGroupFn("jobTree big batch system options", "jobTree can employ a secondary batch system for running large memory/cpu jobs using the following arguments:")
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

def loadTheBatchSystem(config):
    """Load the batch system.
    """
    def batchSystemConstructionFn(batchSystemString, maxCpus, maxMemory):
        batchSystem = None
        if batchSystemString == "parasol":
            batchSystem = ParasolBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory)
            logger.info("Using the parasol batch system")
        elif batchSystemString == "single_machine" or batchSystemString == "singleMachine":
            batchSystem = SingleMachineBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory)
            logger.info("Using the single machine batch system")
        elif batchSystemString == "gridengine" or batchSystemString == "gridEngine":
            batchSystem = GridengineBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory)
            logger.info("Using the grid engine machine batch system")
        elif batchSystemString == "acid_test" or batchSystemString == "acidTest":
            config.attrib["try_count"] = str(32) #The chance that a job does not 
            #complete after 32 goes in one in 4 billion, so you need a lot of 
            #jobs before this becomes probable
            batchSystem = SingleMachineBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory, workerFn=badWorker)
        elif batchSystemString == "lsf" or batchSystemString == "LSF":
            batchSystem = LSFBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory)
            logger.info("Using the lsf batch system")
        else:
            raise RuntimeError("Unrecognised batch system: %s" % batchSystemString)
        return batchSystem
    batchSystem = batchSystemConstructionFn(config.attrib["batch_system"], \
                                            int(config.attrib["max_cpus"]), \
                                            int(config.attrib["max_memory"]))
    if "big_batch_system" in config.attrib:
        bigMemoryThreshold = int(config.attrib["big_memory_threshold"])
        bigCpuThreshold = int(config.attrib["big_cpu_threshold"])
        bigMaxCpus = int(config.attrib["big_max_cpus"])
        bigMaxMemory = int(config.attrib["big_max_memory"])
        bigBatchSystem = batchSystemConstructionFn(config.attrib["big_batch_system"], \
                                                   maxCpus=bigMaxCpus, maxMemory=bigMaxMemory)
        batchSystem = CombinedBatchSystem(config, batchSystem, bigBatchSystem, \
    lambda command, memory, cpu : memory <= bigMemoryThreshold and cpu <= bigCpuThreshold)
    return batchSystem

def serialiseEnvironment(jobStore):
    """Puts the environment in a globally accessible pickle file.
    """
    #Dump out the environment of this process in the environment pickle file.
    fileHandle = jobStore.writeSharedFileStream("environment.pickle")
    cPickle.dump(os.environ, fileHandle)
    fileHandle.close()
    logger.info("Written the environment for the jobs to the environment file")

def createJobTree(options):
    logger.info("Starting to create the job tree setup for the first time")
    options.jobTree = absSymPath(options.jobTree)
    os.mkdir(options.jobTree)
    config = ET.Element("config")
    config.attrib["log_level"] = getLogLevelString()
    config.attrib["job_tree"] = options.jobTree
    config.attrib["parasol_command"] = options.parasolCommand
    config.attrib["try_count"] = str(int(options.retryCount) + 1)
    config.attrib["max_job_duration"] = str(float(options.maxJobDuration))
    config.attrib["batch_system"] = options.batchSystem
    config.attrib["job_time"] = str(float(options.jobTime))
    config.attrib["max_log_file_size"] = str(int(options.maxLogFileSize))
    config.attrib["default_memory"] = str(int(options.defaultMemory))
    config.attrib["default_cpu"] = str(int(options.defaultCpu))
    config.attrib["max_cpus"] = str(int(options.maxCpus))
    config.attrib["max_memory"] = str(int(options.maxMemory))
    config.attrib["max_threads"] = str(int(options.maxThreads))
    if options.bigBatchSystem != None:
        config.attrib["big_batch_system"] = options.bigBatchSystem
        config.attrib["big_memory_threshold"] = str(int(options.bigMemoryThreshold))
        config.attrib["big_cpu_threshold"] = str(int(options.bigCpuThreshold))
        config.attrib["big_max_cpus"] = str(int(options.bigMaxCpus))
        config.attrib["big_max_memory"] = str(int(options.bigMaxMemory))
        
    if options.stats:
        config.attrib["stats"] = ""
    #Load the batch system.
    batchSystem = loadTheBatchSystem(config)
    
    #Set the parameters determining the polling frequency of the system.  
    config.attrib["rescue_jobs_frequency"] = str(float(batchSystem.getRescueJobFrequency()))
    if options.rescueJobsFrequency != None:
        config.attrib["rescue_jobs_frequency"] = str(float(options.rescueJobsFrequency))
    
    jobStore = FileJobStore(jobStoreString=options.jobTree, create=True, config=config)
    
    logger.info("Finished the job tree setup")
    return config, batchSystem, jobStore

def reloadJobTree(jobStoreString):
    """Load the job tree from a dir.
    """
    logger.info("The job tree appears to already exist, so we'll reload it")
    jobStore = FileJobStore(jobStoreString)
    jobStore.config.attrib["log_level"] = getLogLevelString()
    jobStore.updateConfig(jobStore.config) #This updates the global copy of the config file
    batchSystem = loadTheBatchSystem(jobStore.config)
    logger.info("Reloaded the jobtree")
    return jobStore.config, batchSystem, jobStore
