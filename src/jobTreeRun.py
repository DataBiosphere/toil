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
import xml.etree.ElementTree as ET
import cPickle
from optparse import OptionParser

from jobTree.batchSystems.parasol import ParasolBatchSystem
from jobTree.batchSystems.gridengine import GridengineBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem, BadWorker

from jobTree.src.master import createJob
from jobTree.src.master import mainLoop
from jobTree.src.master import writeJobs

from jobTree.src.bioio import logger, setLoggingFromOptions, addLoggingOptions
from jobTree.src.bioio import TempFileTree
from jobTree.src.bioio import system


def runJobTree(command, jobTreeDir, logLevel="DEBUG", retryCount=0, batchSystem="single_machine", 
               rescueJobFrequency=None):
    """A convenience function for running job tree from within a python script.
    """
    if rescueJobFrequency != None:
        rescueJobFrequencyString = "--rescueJobsFrequency %s" % float(rescueJobFrequency)
    else:
        rescueJobFrequencyString = ""
    command = "jobTree --command \"%s\" --jobTree %s --logLevel %s \
--retryCount %i --batchSystem %s %s" % \
            (command, jobTreeDir,  logLevel, retryCount, batchSystem, rescueJobFrequencyString)
    logger.info("Running command : %s" % command)
    system(command)
    logger.info("Ran the jobtree apparently okay")
    

def commandAvailable(executable):
    return 0 == os.system("which %s > /dev/null 2> /dev/null" % executable)

def detectQueueSystem():
    if commandAvailable("parasol"):
        return "parasol"
    if commandAvailable("qstat"):
        return "gridEngine"
    return "singleMachine"

def addOptions(parser):
    addLoggingOptions(parser)#This adds the logging stuff..
    
    parser.add_option("--command", dest="command", 
                      help="The command to run (which will generate subsequent jobs)",
                      default=None)
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory in which to place job management files \
(this needs to be globally accessible by all machines running jobs).\n\
If you pass an existing directory it will check if it's a valid existin job tree, then\
try and restart the jobs in it",
                      default=None)
    
    parser.add_option("--batchSystem", dest="batchSystem",
                      help="The type of batch system to run the job(s) with, currently can be 'singleMachine'/'parasol'/'acidTest'/'gridEngine'",
                      default=detectQueueSystem())
    
    parser.add_option("--retryCount", dest="retryCount", 
                      help="Number of times to try a failing job before giving up and labelling job failed",
                      default=0)
    
    parser.add_option("--waitDuration", dest="waitDuration", 
                      help="Period of time to pause after updating the running jobs (default is set by batch system)")
    
    parser.add_option("--rescueJobsFrequency", dest="rescueJobsFrequency", 
                      help="Period of time to wait (in seconds) between checking for missing/overlong jobs (default is set by the batch system)")
    
    parser.add_option("--maxJobDuration", dest="maxJobDuration", 
                      help="Maximum runtime of a job (in seconds) before we kill it (this is an approximate time, and the actual time before killing the job may be longer)",
                      default=str(sys.maxint))
    
    parser.add_option("--jobTime", dest="jobTime", 
                      help="The approximate time (in seconds) that you'd like a list of child jobs to be run serially before being parallised. \
                      This parameter allows one to avoid over parallelising tiny jobs, and therefore paying significant scheduling overhead, by \
                      running tiny jobs in series on a single node/core of the cluster.",
                      default=30)
    
    parser.add_option("--maxLogFileSize", dest="maxLogFileSize", 
                      help="The maximum size of a log file to keep (in bytes), log files larger than this will be truncated to the last X bytes. Default is 50 kilobytes",
                      default=50120)
    
    parser.add_option("--defaultMemory", dest="defaultMemory", 
                      help="The default amount of memory to request for a job (in bytes), by default is 2^31 = 2 gigabytes",
                      default=2147483648)
    
    parser.add_option("--defaultCpu", dest="defaultCpu", 
                      help="The default the number of cpus to dedicate a job, the default is 1",
                      default=1)
    
    parser.add_option("--maxJobs", dest="maxJobs", 
                      help="The maximum number of jobs to issue to the batch system at any one time",
                      default=sys.maxint)
    
    parser.add_option("--maxThreads", dest="maxThreads", 
                      help="The maximum number of threads to use when running in single machine mode",
                      default=4)
    
    parser.add_option("--stats", dest="stats", action="store_true",
                      help="Records statistics about the job-tree to be used by jobTreeStats",
                      default=False)
    
def setupTempFileTrees(config):
    """Load the temp file trees
    """
    config.attrib["job_file_dir"] = TempFileTree(config.attrib["job_file_dir"])
    config.attrib["temp_dir_dir"] = TempFileTree(config.attrib["temp_dir_dir"])
    config.attrib["log_file_dir"] = TempFileTree(config.attrib["log_file_dir"])
    config.attrib["slave_log_file_dir"] = TempFileTree(config.attrib["slave_log_file_dir"])
    logger.info("Setup the temp file trees")
    
def loadTheBatchSystem(config):
    """Load the batch system.
    """
    batchSystemString = config.attrib["batch_system"]
    if batchSystemString == "parasol":
        batchSystem = ParasolBatchSystem(config)
        logger.info("Using the parasol batch system")
    elif batchSystemString == "single_machine" or batchSystemString == "singleMachine":
        batchSystem = SingleMachineBatchSystem(config)
        logger.info("Using the single machine batch system")
    elif batchSystemString == "gridengine" or batchSystemString == "gridEngine":
        batchSystem = GridengineBatchSystem(config)
        logger.info("Using the grid engine machine batch system")
    elif batchSystemString == "acid_test" or batchSystemString == "acidTest":
        batchSystem = SingleMachineBatchSystem(config, workerClass=BadWorker)
        config.attrib["retry_count"] = str(32) #The chance that a job does not complete after 32 goes in one in 4 billion, so you need a lot of jobs before this becomes probable
    else:
        raise RuntimeError("Unrecognised batch system: %s" % batchSystemString)
    return batchSystem

def loadEnvironment(config):
    """Puts the environment in the pickle file.
    """
    #Dump out the environment of this process in the environment pickle file.
    fileHandle = open(config.attrib["environment_file"], 'w')
    cPickle.dump(os.environ, fileHandle)
    fileHandle.close()
    logger.info("Written the environment for the jobs to the environment file")

def reloadJobTree(jobTree):
    """Load the job tree from a dir.
    """
    logger.info("The job tree appears to already exist, so we'll reload it")
    assert os.path.isfile(os.path.join(jobTree, "config.xml")) #A valid job tree must contain the config file
    assert os.path.isfile(os.path.join(jobTree, "environ.pickle")) #A valid job tree must contain a pickle file which encodes the path environment of the job
    assert os.path.isfile(os.path.join(jobTree, "jobNumber.xml")) #A valid job tree must contain a file which is updated with the number of jobs that have been run.
    assert os.path.isdir(os.path.join(jobTree, "jobs")) #A job tree must have a directory of jobs.
    assert os.path.isdir(os.path.join(jobTree, "tempDirDir")) #A job tree must have a directory of temporary directories (for jobs to make temp files in).
    assert os.path.isdir(os.path.join(jobTree, "logFileDir")) #A job tree must have a directory of log files.
    assert os.path.isdir(os.path.join(jobTree, "slaveLogFileDir")) #A job tree must have a directory of slave log files.
    config = ET.parse(os.path.join(jobTree, "config.xml")).getroot()
    setupTempFileTrees(config)
    batchSystem = loadTheBatchSystem(config)
    logger.info("Reloaded the jobtree")
    return config, batchSystem

def createJobTree(options):
    logger.info("Starting to create the job tree setup for the first time")
    options.jobTree = os.path.abspath(options.jobTree)
    os.mkdir(options.jobTree)
    config = ET.Element("config")
    config.attrib["environment_file"] = os.path.join(options.jobTree, "environ.pickle")
    config.attrib["job_number_file"] = os.path.join(options.jobTree, "jobNumber.xml")
    config.attrib["job_file_dir"] = os.path.join(options.jobTree, "jobs")
    config.attrib["temp_dir_dir"] = os.path.join(options.jobTree, "tempDirDir")
    config.attrib["log_file_dir"] = os.path.join(options.jobTree, "logFileDir")
    config.attrib["slave_log_file_dir"] = os.path.join(options.jobTree, "slaveLogFileDir")
    config.attrib["results_file"] = os.path.join(options.jobTree, "results.txt")
    config.attrib["scratch_file"] = os.path.join(options.jobTree, "scratch.txt")
    config.attrib["retry_count"] = str(int(options.retryCount))
    config.attrib["max_job_duration"] = str(float(options.maxJobDuration))
    config.attrib["batch_system"] = options.batchSystem
    config.attrib["job_time"] = str(float(options.jobTime))
    config.attrib["max_log_file_size"] = str(int(options.maxLogFileSize))
    config.attrib["default_memory"] = str(int(options.defaultMemory))
    config.attrib["default_cpu"] = str(int(options.defaultCpu))
    config.attrib["max_jobs"] = str(int(options.maxJobs))
    config.attrib["max_threads"] = str(int(options.maxThreads))
    if options.stats:
        config.attrib["stats"] = os.path.join(options.jobTree, "stats.xml")
        fileHandle = open(config.attrib["stats"], 'w')
        fileHandle.write("<stats>")
        fileHandle.close()
    #Load the batch system.
    batchSystem = loadTheBatchSystem(config)
    
    #Set the two parameters determining the polling frequency of the system.
    config.attrib["wait_duration"] = str(float(batchSystem.getWaitDuration()))
    if options.waitDuration != None:
        config.attrib["wait_duration"] = str(float(options.waitDuration))
        
    config.attrib["rescue_jobs_frequency"] = str(float(batchSystem.getRescueJobFrequency()))
    if options.rescueJobsFrequency != None:
        config.attrib["rescue_jobs_frequency"] = str(float(options.rescueJobsFrequency))
    
    #Write the config file to disk
    fileHandle = open(os.path.join(options.jobTree, "config.xml"), 'w')
    
    tree = ET.ElementTree(config)
    tree.write(fileHandle)
    fileHandle.close()
    logger.info("Written the config file")
    
    #Set up the jobNumber file
    fileHandle = open(config.attrib["job_number_file"], 'w')
    ET.ElementTree(ET.Element("job_number", { "job_number":'0' })).write(fileHandle)
    fileHandle.close()
    
    #Setup the temp file trees.
    setupTempFileTrees(config)
    
    logger.info("Finished the job tree setup")
    return config, batchSystem

def createFirstJob(command, config, memory=None, cpu=None, time=sys.maxint):
    """Adds the first job to to the jobtree.
    """
    logger.info("Adding the first job")
    if memory == None:
        memory = config.attrib["default_memory"]
    if cpu == None:
        cpu = config.attrib["default_cpu"]
    job = createJob({ "command":command, "memory":str(int(memory)), "cpu":str(int(cpu)), "time":str(float(time)) }, None, config)
    writeJobs([job])
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
        raise RuntimeError("Unrecognised input arguments: %s" % " ".join(args))
        
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
