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
        
def loadStack(command):
    commandTokens = command.split()
    assert commandTokens[0] == "scriptTree"
    for className in commandTokens[2:]:
        l = className.split(".")
        moduleName = ".".join(l[:-1])
        className = l[-1]
        _temp = __import__(moduleName, globals(), locals(), [className], -1)
        exec "%s = 1" % className
        vars()[className] = _temp.__dict__[className]
    return loadPickleFile(commandTokens[1])
        
def loadPickleFile(pickleFile):
    """Loads the first object from a pickle file.
    """
    fileHandle = open(pickleFile, 'r')
    i = cPickle.load(fileHandle)
    fileHandle.close()
    return i
    
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
    from jobTree.src.job import Job
    from jobTree.src.master import getEnvironmentFileName, getConfigFileName
    from sonLib.bioio import system
    
    ##########################################
    #Input args
    ##########################################
    
    jobTreePath = sys.argv[1]
    jobFile = sys.argv[2]
    
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

    ##########################################
    #Slave log file trapped from here on in
    ##########################################

    slaveFailed = False
    try:
    
        ##########################################
        #Parse input files
        ##########################################
        
        config = ET.parse(getConfigFileName(jobTreePath)).getroot()
        setLogLevel(config.attrib["log_level"])
        job = Job.readJob(jobFile)
        logger.info("Parsed arguments and set up logging")
    
         #Try loop for slave logging
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
        #The max time 
        ##########################################
        
        maxTime = float(config.attrib["job_time"])
        assert maxTime > 0.0
        assert maxTime < sys.maxint
        
        ##########################################
        #The next job
        ##########################################
        
        def globalTempDirName(job, depth):
            return job.getGlobalTempDirName() + str(depth)
        
        command, memoryAvailable, cpuAvailable, depth = job.followOnCommands[-1]
        defaultMemory = int(config.attrib["default_memory"])
        defaultCpu = int(config.attrib["default_cpu"])
        assert len(job.childJobs) == 0
        
        startTime = time.time() 
        while True:
            job.followOnCommands.pop()
            
            ##########################################
            #Global temp dir
            ##########################################
            
            globalTempDir = globalTempDirName(job, depth)
            if not os.path.isdir(globalTempDir): #Ensures that the global temp dirs of each level are kept separate.
                os.mkdir(globalTempDir)
                os.chmod(globalTempDir, 0777)
            i = 1
            while os.path.isdir(globalTempDir(job, depth+i)):
                system("rm -rf %s" % globalTempDir(job, depth+i))
                i += 1
        
            ##########################################
            #Run the job
            ##########################################
        
            if command != "": #Not a stub
                if command[:11] == "scriptTree ":
                    ##########################################
                    #Run the target
                    ##########################################
                    
                    loadStack(command).execute(job=job, stats=stats,
                                    localTempDir=localTempDir, globalTempDir=globalTempDir, 
                                    memoryAvailable=memoryAvailable, cpuAvailable=cpuAvailable, 
                                    defaultMemory=defaultMemory, defaultCpu=defaultCpu)
            
                else: #Is another command
                    system(command) 
            
            ##########################################
            #Cleanup/reset a successful job/checkpoint
            ##########################################
            
            job.remainingRetryCount = int(config.attrib["retry_count"])
            system("rm -rf %s/*" % (localTempDir))
            job.update()
            
            ##########################################
            #Establish if we can run another job
            ##########################################
            
            if time.time() - startTime > maxTime:
                logger.info("We are breaking because the maximum time the job should run for has been exceeded")
                break
            
            #Deal with children
            if len(job.children) >= 1:  #We are going to have to return to the parent
                logger.info("No more jobs can run in series by this slave, its got %i children" % len(job.children))
                break
            
            if len(job.followOnCommands) == 0:
                logger.info("No more jobs can run by this slave as we have exhausted the follow ons")
                break
            
            #Get the next job and see if we have enough cpu and memory to run it..
            command, memory, cpu, depth = job.followOnCommands[-1]
            
            if memory > memoryAvailable:
                logger.info("We need more memory for the next job, so finishing")
                break
            if cpu > cpuAvailable:
                logger.info("We need more cpus for the next job, so finishing")
                break
            
            logger.info("Starting the next job")
        
        ##########################################
        #Finish up the stats
        ##########################################
        
        if stats != None:
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.attrib["time"] = str(time.time() - startTime)
            stats.attrib["clock"] = str(totalCpuTime - startClock)
            stats.attrib["memory"] = str(totalMemoryUsage)
            fileHandle = open(job.getJobStatsFileName(), 'w')
            ET.ElementTree(stats).write(fileHandle)
            fileHandle.close()
        
        ##########################################
        #Cleanup global files at the end of the chain
        ##########################################
       
        if len(job.followOnCommands) == 0 and len(job.children) == 0:
            job.delete()            
        
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds" % (time.time() - startTime))
    
    ##########################################
    #Where slave goes wrong
    ##########################################
    except: #Case that something goes wrong in slave
        traceback.print_exc(file = slaveHandle)
        job = Job.read(job.getJobFileName())
        job.remainingRetryCount -= 1
        job.update()
        slaveFailed = True

    ##########################################
    #Cleanup
    ##########################################
    
    #Close the slave logging
    slaveHandle.flush()
    sys.stderr = origStdErr
    sys.stdout = origStdOut
    redirectLoggerStreamHandlers(slaveHandle, sys.stderr)
    slaveHandle.close()
    
    #Copy back the log file to the global dir, if needed
    if config.attrib.has_key("reportAllJobLogFiles") or slaveFailed:
        truncateFile(tempSlaveLogFile)
        system("mv %s %s" % (tempSlaveLogFile, job.getLogFileName()))
        
    #Remove the temp dir
    system("rm -rf %s" % localSlaveTempDir)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()

