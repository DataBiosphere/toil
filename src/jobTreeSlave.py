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

import os
import sys
import time
import subprocess
import xml.etree.cElementTree as ET
import cPickle
import traceback
import time
import socket

def truncateFile(fileNameString, tooBig=50000):
    """Truncates a file that is bigger than tooBig bytes, leaving only the 
    last tooBig bytes in the file.
    """
    if os.path.getsize(fileNameString) > tooBig:
        fh = open(fileNameString, 'rb+')
        fh.seek(-tooBig, 2) 
        data = fh.read()
        fh.seek(0) # rewind
        fh.write(data)
        fh.truncate()
        fh.close()
        
def getMemoryAndCpuRequirements(config, nextJob):
    """Gets the memory and cpu requirements from the job..
    """
    #Now deal with the CPU and memory..
    memory = config.attrib["default_memory"]
    cpu = config.attrib["default_cpu"]
    if nextJob.attrib.has_key("memory"):
        memory = max(int(nextJob.attrib["memory"]), 0)
    if nextJob.attrib.has_key("cpu"):
        cpu = max(int(nextJob.attrib["cpu"]), 0)
    return memory, cpu
 
def processJob(job, command, memoryAvailable, cpuAvailable, 
               defaultMemory, defaultCpu,
               stats, localTempDir):
    """Runs a job.
    """
    from sonLib.bioio import logger
    from jobTree.src.master import getGlobalTempDirName, getJobFileName
    import jobTree.scriptTree.scriptTree
    
    assert len(job.find("children").findall("child")) == 0
    assert int(job.attrib["child_count"]) == int(job.attrib["black_child_count"])
   
    followOns = job.find("followOns")
    jobFile = getJobFileName(job)
    #Temp file dirs for job.
    depth = len(job.find("followOns").findall("followOn"))
    assert depth >= 1
    globalTempDir = os.path.join(getGlobalTempDirName(job), str(depth))
    if not os.path.isdir(globalTempDir): #Ensures that the global temp dirs of each level are kept separate.
        os.mkdir(globalTempDir)
        os.chmod(globalTempDir, 0777)
    if os.path.isdir(os.path.join(getGlobalTempDirName(job), str(depth+1))):
        system("rm -rf %s" % os.path.join(getGlobalTempDirName(job), str(depth+1)))
    assert not os.path.isdir(os.path.join(getGlobalTempDirName(job), str(depth+2)))

    try: 
        if command != "": #Not a stub
            commandTokens = command.split()  
            for className in commandTokens[1:]:
                #Magic for loading a class
                logger.info("Loading the class name %s" % className)
                l = className.split(".")
                moduleName = ".".join(l[:-1])
                className = l[-1]
                _temp = __import__(moduleName, globals(), locals(), [className], -1)
                exec "%s = 1" % className
                vars()[className] = _temp.__dict__[className]
            loadPickleFile(commandTokens[0]).execute(job, localTempDir, globalTempDir, memoryAvailable, cpuAvailable)
        
            #The follow on command
            if len(job.find("children").findall("child")) != 0 and \
                totalFollowOns == len(followOns): #This is to keep the stack of follow on jobs consistent.
                ET.SubElement(followOns, "followOn", { "command":"", "memory":"1000000", "cpu":"1" })
                logger.info("Making a stub follow on job")
        job.attrib["colour"] = "black" #Update the colour
    except:
        traceback.print_exc(file = fileHandle)
        logger.critical("Caught an exception in the target being run")
        #Reload and colour red
        job = readJob(jobFile) #Reload the job
        logger.critical("Failed the job")
        job.attrib["colour"] = "red" #Update the colour
        
    #Clean up
    system("rm -rf %s/*" % (localTempDir))
    logger.info("Cleaned up by removing the contents of the local temporary file directory for the job")
    return job
    
def main():
    sys.path.append(sys.argv[1])
    sys.argv.remove(sys.argv[1])
    
    #Now we can import all the stuff..
    from sonLib.bioio import getBasicOptionParser
    from sonLib.bioio import parseBasicOptions
    from sonLib.bioio import logger
    from sonLib.bioio import addLoggingFileHandler, redirectLoggerStreamHandlers
    from sonLib.bioio import setLogLevel
    from sonLib.bioio import getTotalCpuTime, getTotalCpuTimeAndMemoryUsage
    from sonLib.bioio import getTempDirectory
    from jobTree.src.master import writeJob
    from jobTree.src.master import readJob
    from jobTree.src.master import getSlaveLogFileName, getLogFileName, getJobStatsFileName, getGlobalTempDirName  
    from jobTree.src.jobTreeRun import getEnvironmentFileName, getConfigFileName
    from sonLib.bioio import system
    
    ##########################################
    #Parse the job.
    ##########################################
    
    jobTreePath = sys.argv[1]
    config = ET.parse(getConfigFileName(jobTreePath)).getroot()
    job = readJob(sys.argv[2])
    
    ##########################################
    #Load the environment for the job
    ##########################################
    
    #First load the environment for the job.
    fileHandle = open(getEnvironmentFileName(jobTreePath), 'r')
    environment = cPickle.load(fileHandle)
    fileHandle.close()
    for i in environment:
        if i not in ("TMPDIR", "TMP", "HOSTNAME", "HOSTTYPE"):
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(':'):
            if e != '':
                sys.path.append(e)
    #os.environ = environment
    #os.putenv(key, value)
        
    ##########################################
    #Setup the temporary directories.
    ##########################################
        
    #Dir to put all the temp files in.
    localSlaveTempDir = getTempDirectory()
    localTempDir = os.path.join(localSlaveTempDir, "localTempDir") 
    os.mkdir(localTempDir)
    os.chmod(localTempDir, 0777)
    
    ##########################################
    #Setup the logging
    ##########################################
    
    #Setup the logging
    tempSlaveLogFile = os.path.join(localSlaveTempDir, "slave_log.txt")
    slaveHandle = open(tempSlaveLogFile, 'w')
    redirectLoggerStreamHandlers(sys.stderr, slaveHandle)
    origStdErr = sys.stderr
    origStdOut = sys.stdout
    sys.stderr = slaveHandle 
    sys.stdout = slaveHandle
    
    setLogLevel(config.attrib["log_level"])
    logger.info("Parsed arguments and set up logging")
    
    try: #Try loop for slave logging
        ##########################################
        #Setup the stats, if requested
        ##########################################
        
        if config.attrib.has_key("stats"):
            startTime = time.time()
            startClock = getTotalCpuTime()
            stats = ET.Element("slave")
        else:
            stats = None
        
        ##########################################
        #Run the script.
        ##########################################
        
        maxTime = float(config.attrib["job_time"])
        assert maxTime > 0.0
        assert maxTime < sys.maxint
        
        jobToRun = job.find("followOns").findall("followOn")[-1]
        memoryAvailable = int(jobToRun.attrib["memory"])
        defaultMemory = int(config.attrib["memory"])
        cpuAvailable = int(jobToRun.attrib["cpu"])
        defaultCpu = int(config.attrib["cpu"])
        
        startTime = time.time()
        while True:
            job = processJob(job, jobToRun, memoryAvailable, cpuAvailable, 
                                     defaultMemory, defaultCpu,
                                     stats, localTempDir, config)
            
            if job.attrib["colour"] != "black": 
                logger.critical("Exiting the slave because of a failed job on host %s", socket.gethostname())
                system("mv %s %s" % (tempLogFile, getLogFileName(job))) #Copy back the job log file, because we saw failure
                break
            
            childrenNode = job.find("children")
            childrenList = childrenNode.findall("child")
            #childRuntime = sum([ float(child.attrib["time"]) for child in childrenList ])
                
            if len(childrenList) >= 2: # or totalRuntime + childRuntime > maxTime: #We are going to have to return to the parent
                logger.info("No more jobs can run in series by this slave, its got %i children" % len(childrenList))
                break
            
            if time.time() - startTime > maxTime:
                logger.info("We are breaking because the maximum time the job should run for has been exceeded")
                break
            
            followOns = job.find("followOns")
            while len(childrenList) > 0:
                child = childrenList.pop()
                childrenNode.remove(child)
                ET.SubElement(followOns, "followOn", child.attrib.copy())
           
            assert len(childrenNode.findall("child")) == 0
            
            if len(followOns.findall("followOn")) == 0:
                logger.info("No more jobs can run by this slave as we have exhausted the follow ons")
                break
            
            #Get the next job and see if we have enough cpu and memory to run it..
            jobToRun = job.find("followOns").findall("followOn")[-1]
            if int(jobToRun.attrib["memory"]) > memoryAvailable:
                logger.info("We need more memory for the next job, so finishing")
                break
            if int(jobToRun.attrib["cpu"]) > cpuAvailable:
                logger.info("We need more cpus for the next job, so finishing")
                break
            
            ##Updated the job so we can start the next loop cycle
            job.attrib["colour"] = "grey"
            writeJob(job)
            logger.info("Updated the status of the job to grey and starting the next job")
        
        #Write back the job file with the updated jobs, using the checkpoint method.
        writeJob(job)
        logger.info("Written out an updated job file")
        
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds" % (time.time() - startTime))
        
        ##########################################
        #Finish up the stats
        ##########################################
        
        if stats != None:
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.attrib["time"] = str(time.time() - startTime)
            stats.attrib["clock"] = str(totalCpuTime - startClock)
            stats.attrib["memory"] = str(totalMemoryUsage)
            fileHandle = open(getJobStatsFileName(job), 'w')
            ET.ElementTree(stats).write(fileHandle)
            fileHandle.close()
        
        ##########################################
        #Cleanup global files at the end of the chain
        ##########################################
       
        if job.attrib["colour"] == "black" and len(job.find("followOns").findall("followOn")) == 0:
            nestedGlobalTempDir = os.path.join(getGlobalTempDirName(job), "1")
            assert os.path.exists(nestedGlobalTempDir)
            system("rm -rf %s" % nestedGlobalTempDir)
            if os.path.exists(getLogFileName(job)):
                os.remove(getLogFileName(job))
            if os.path.exists(getSlaveLogFileName(job)):
                os.remove(getSlaveLogFileName(job))
            if stats != None:
                assert len(os.listdir(getGlobalTempDirName(job))) == 2 #The job file and the stats file
            else:
                assert len(os.listdir(getGlobalTempDirName(job))) == 1 #Just the job file
    
    ##########################################
    #Where slave goes wrong
    ##########################################
    except: #Case that something goes wrong in slave
        traceback.print_exc(file = slaveHandle)
        slaveHandle.flush()
        sys.stderr = origStdErr
        sys.stdout = origStdOut
        redirectLoggerStreamHandlers(slaveHandle, sys.stderr)
        slaveHandle.close()
        system("mv %s %s" % (tempSlaveLogFile, getSlaveLogFileName(job)))
        system("rm -rf %s" % localSlaveTempDir)
        raise RuntimeError()
    
    ##########################################
    #Normal cleanup
    ##########################################
    
    elif config.attrib.has_key("reportAllJobLogFiles"):
                logger.info("Exiting because we've been asked to report all logs, and this involves returning to the master")
                #Copy across the log file
                system("mv %s %s" % (tempLogFile, getLogFileName(job)))
                break
    
    sys.stderr = origStdErr
    sys.stdout = origStdOut
    redirectLoggerStreamHandlers(slaveHandle, sys.stderr)
    slaveHandle.close()
    system("rm -rf %s" % localSlaveTempDir)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()

