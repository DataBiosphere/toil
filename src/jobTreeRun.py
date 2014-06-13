#!/usr/bin/env python

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

"""A script to setup and run a hierarchical run of cluster jobs.
"""

import os
import sys 
import xml.etree.cElementTree as ET
import cPickle
from argparse import ArgumentParser
from optparse import OptionParser, OptionContainer, OptionGroup

from jobTree.batchSystems.parasol import ParasolBatchSystem
from jobTree.batchSystems.gridengine import GridengineBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem, badWorker
from jobTree.batchSystems.combinedBatchSystem import CombinedBatchSystem
from jobTree.batchSystems.lsf import LSFBatchSystem

from jobTree.src.job import Job

from jobTree.src.master import mainLoop
from jobTree.src.master import getEnvironmentFileName, getStatsFileName, getConfigFileName, getJobFileDirName

from sonLib.bioio import logger, setLoggingFromOptions, addLoggingOptions, getLogLevelString
from sonLib.bioio import TempFileTree
from sonLib.bioio import system, absSymPath

def runJobTree(command, jobTreeDir, logLevel="DEBUG", retryCount=0, batchSystem="single_machine", 
               rescueJobFrequency=None):
    """A convenience function for running job tree from within a python script.
    """
    if rescueJobFrequency != None:
        rescueJobFrequencyString = "--rescueJobsFrequency %s" % float(rescueJobFrequency)
    else:
        rescueJobFrequencyString = ""
    command = "jobTreeRun --command \"%s\" --jobTree %s --logLevel %s \
--retryCount %i --batchSystem %s %s" % \
            (command, jobTreeDir,  logLevel, retryCount, batchSystem, rescueJobFrequencyString)
    logger.info("Running command : %s" % command)
    system(command)
    logger.info("Ran the jobtree apparently okay")
    
def _addOptions(addGroupFn, defaultStr):
    addOptionFn = addGroupFn("jobTree core options", "Options to specify the location of the jobTree and turn on stats collation about the performance of jobs.")
    addOptionFn("--jobTree", dest="jobTree", default="./jobTree",
                      help=("Directory in which to place job management files and the global accessed temporary file directories"
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
                      help=("The maximum amount of memory to request from the batch system at any one time. default=%s" % defaultStr))
    
    addOptionFn = addGroupFn("jobTree options for rescuing/killing/restarting jobs", "The options for jobs that either run too long/fail or get lost (some batch systems have issues!)")
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
    addOptionFn("--command", dest="command", default=None,
                      help="The command to run (which will generate subsequent jobs). This is deprecated")
        
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
            return parser.add_argument_group(parser, headingString, bodyString).add_argument
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
            config.attrib["try_count"] = str(32) #The chance that a job does not complete after 32 goes in one in 4 billion, so you need a lot of jobs before this becomes probable
            batchSystem = SingleMachineBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory, workerFn=badWorker)
        elif batchSystemString == "lsf" or batchSystemString == "LSF":
            batchSystem = LSFBatchSystem(config, maxCpus=maxCpus, maxMemory=maxMemory)
            logger.info("Using the lsf batch system")
        else:
            raise RuntimeError("Unrecognised batch system: %s" % batchSystemString)
        return batchSystem
    batchSystem = batchSystemConstructionFn(config.attrib["batch_system"], int(config.attrib["max_cpus"]), int(config.attrib["max_memory"]))
    if "big_batch_system" in config.attrib:
        bigMemoryThreshold = int(config.attrib["big_memory_threshold"])
        bigCpuThreshold = int(config.attrib["big_cpu_threshold"])
        bigMaxCpus = int(config.attrib["big_max_cpus"])
        bigMaxMemory = int(config.attrib["big_max_memory"])
        bigBatchSystem = batchSystemConstructionFn(config.attrib["big_batch_system"], maxCpus=bigMaxCpus, maxMemory=bigMaxMemory)
        batchSystem = CombinedBatchSystem(config, batchSystem, bigBatchSystem, lambda command, memory, cpu : memory <= bigMemoryThreshold and cpu <= bigCpuThreshold)
    return batchSystem

def loadEnvironment(config):
    """Puts the environment in the pickle file.
    """
    #Dump out the environment of this process in the environment pickle file.
    fileHandle = open(getEnvironmentFileName(config.attrib["job_tree"]), 'w')
    cPickle.dump(os.environ, fileHandle)
    fileHandle.close()
    logger.info("Written the environment for the jobs to the environment file")

def writeConfig(config):
    #Write the config file to disk
    fileHandle = open(getConfigFileName(config.attrib["job_tree"]), 'w')
    tree = ET.ElementTree(config)
    tree.write(fileHandle)
    fileHandle.close()
    logger.info("Written the config file")

def reloadJobTree(jobTree):
    """Load the job tree from a dir.
    """
    logger.info("The job tree appears to already exist, so we'll reload it")
    assert os.path.isfile(getConfigFileName(jobTree)) #A valid job tree must contain the config file
    assert os.path.isfile(getEnvironmentFileName(jobTree)) #A valid job tree must contain a pickle file which encodes the path environment of the job
    assert os.path.isdir(getJobFileDirName(jobTree)) #A job tree must have a directory of jobs.
    
    config = ET.parse(getConfigFileName(jobTree)).getroot()
    config.attrib["log_level"] = getLogLevelString()
    writeConfig(config) #This updates the on disk config file with the new logging setting
    
    batchSystem = loadTheBatchSystem(config)
    logger.info("Reloaded the jobtree")
    return config, batchSystem

def createJobTree(options):
    logger.info("Starting to create the job tree setup for the first time")
    options.jobTree = absSymPath(options.jobTree)
    os.mkdir(options.jobTree)
    os.mkdir(getJobFileDirName(options.jobTree))
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
    
    writeConfig(config)
    
    logger.info("Finished the job tree setup")
    return config, batchSystem

def createFirstJob(command, config, memory=None, cpu=None, time=sys.maxint):
    """Adds the first job to to the jobtree.
    """
    logger.info("Adding the first job")
    if memory == None or memory == sys.maxint:
        memory = float(config.attrib["default_memory"])
    if cpu == None or cpu == sys.maxint:
        cpu = float(config.attrib["default_cpu"])
    job = Job(command=command, memory=memory, cpu=cpu, 
              tryCount=int(config.attrib["try_count"]), jobDir=getJobFileDirName(config.attrib["job_tree"]))
    job.write()
    logger.info("Added the first job")
    
def runJobTreeScript(options):
    """Builds the basic job tree, or takes an existing one
    and runs the job tree master script.
    """
    setLoggingFromOptions(options)
    assert options.jobTree != None #We need a job tree, or a place to create one
    if os.path.isdir(options.jobTree):
        config, batchSystem = reloadJobTree(options.jobTree)
    else:
        assert options.command != None
        config, batchSystem = createJobTree(options)
        #Setup first job.
        createFirstJob(options.command, config)
    loadEnvironment(config)
    return mainLoop(config, batchSystem)
    
def main():
    """This basic pattern can be used in your python script to avoid having to call
    the command line version of jobTree
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = OptionParser()
    addOptions(parser)
    
    options, args = parser.parse_args()
    
    if len(args) != 0:
        parser.error("Unrecognised input arguments: %s" % " ".join(args))
        
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only jobtree may be specified as argument
    if len(args) == 1: #Allow jobTree directory as arg
        options.jobTree = args[0]
        
    ##########################################
    #Now run the job tree construction/master
    ##########################################  
        
    runJobTreeScript(options)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
