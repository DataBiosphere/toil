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
import xml.etree.ElementTree as ET
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
 
def processJob(job, jobToRun, memoryAvailable, cpuAvailable, stats, environment, 
               localSlaveTempDir, localTempDir, config):
    """Runs a job.
    """
    from sonLib.bioio import logger
    from sonLib.bioio import system
    from sonLib.bioio import getTotalCpuTime, getTotalCpuTimeAndMemoryUsage
    from sonLib.bioio import redirectLoggerStreamHandlers
    from jobTree.src.master import getGlobalTempDirName
    
    assert len(job.find("children").findall("child")) == 0
    assert int(job.attrib["child_count"]) == int(job.attrib["black_child_count"])
    command = jobToRun.attrib["command"]
    #Copy the job file to be edited
    
    tempJob = ET.Element("job")
    ET.SubElement(tempJob, "children")
    
    #Log for job
    tempJob.attrib["log_level"] = config.attrib["log_level"]
    
    #Time length of 'ideal' job before further parallelism is required
    tempJob.attrib["job_time"] = config.attrib["job_time"]

    #Temp file dirs for job.
    tempJob.attrib["local_temp_dir"] = localTempDir
    depth = len(job.find("followOns").findall("followOn"))
    tempJob.attrib["global_temp_dir"] = os.path.join(getGlobalTempDirName(job), str(depth))
    if not os.path.isdir(tempJob.attrib["global_temp_dir"]): #Ensures that the global temp dirs of each level are kept separate.
        os.mkdir(tempJob.attrib["global_temp_dir"])
        os.chmod(tempJob.attrib["global_temp_dir"], 0777)
    if os.path.isdir(os.path.join(getGlobalTempDirName(job), str(depth+1))):
        system("rm -rf %s" % os.path.join(getGlobalTempDirName(job), str(depth+1)))
    assert not os.path.isdir(os.path.join(getGlobalTempDirName(job), str(depth+2)))
    
    #Deal with memory and cpu requirements (this pass tells the running job how much cpu and memory they have,
    #according to the batch system
    tempJob.attrib["available_memory"] = str(memoryAvailable) 
    tempJob.attrib["available_cpu"] = str(cpuAvailable)
    
    #Run the actual command
    tempLogFile = os.path.join(localSlaveTempDir, "temp.log")
    fileHandle = open(tempLogFile, 'w')
    
    if stats != None:
        tempJob.attrib["stats"] = ""
        startTime = time.time()
        startClock = getTotalCpuTime()
    
    #If you're a script tree python process, we don't need to python
    if command[:10] == "scriptTree":
        import jobTree.scriptTree.scriptTree
        savedStdErr = sys.stderr
        savedStdOut = sys.stdout
        exitValue = 0
        try: 
            sys.stderr = fileHandle 
            sys.stdout = fileHandle
            redirectLoggerStreamHandlers(savedStdErr, fileHandle)
            l = command.split()
            jobTree.scriptTree.scriptTree.run(tempJob, l[1], l[2:])
        except:
            traceback.print_exc(file = fileHandle)
            exitValue = 1
        sys.stderr = savedStdErr
        sys.stdout = savedStdOut
        redirectLoggerStreamHandlers(fileHandle, sys.stderr)
        if exitValue == 1:
            logger.critical("Caught an exception in the target being run")
    else:
        if "JOB_FILE" not in command:
            logger.critical("There is no 'JOB_FILE' string in the command to be run to take the job-file argument: %s" % command)
            job.attrib["colour"] = "red" #Update the colour
        
        #Now write the temp job file
        tempFile = os.path.join(localSlaveTempDir, "tempJob.xml")
        fileHandle2 = open(tempFile, 'w')  
        tree = ET.ElementTree(tempJob)
        tree.write(fileHandle2)
        fileHandle2.close()
        logger.info("Copied the jobs files ready for the job")
        
        process = subprocess.Popen(command.replace("JOB_FILE", tempFile), shell=True, stdout=fileHandle, stderr=subprocess.STDOUT, env=environment)
        sts = os.waitpid(process.pid, 0)
        exitValue = sts[1]
        if exitValue == 0:
            tempJob = ET.parse(tempFile).getroot()
        
    fileHandle.close()
    truncateFile(tempLogFile, int(config.attrib["max_log_file_size"]))
    
    logger.info("Ran the job command=%s with exit status %i" % (command, exitValue))
    
    if exitValue == 0:
        logger.info("Passed the job, okay")
        
        if stats != None:
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            jobTag = ET.SubElement(stats, "job", { "time":str(time.time() - startTime), 
                                                  "clock":str(totalCpuTime - startClock),
                                                  "memory":str(totalMemoryUsage) })
            if tempJob.find("stack") != None:
                jobTag.append(tempJob.find("stack"))
        
        job.attrib["colour"] = "black" #Update the colour
        
        #Deal with any logging messages directed at the master
        if tempJob.find("messages") != None:
            messages = job.find("messages")
            if messages == None:
                messages = ET.SubElement(job, "messages")
            for messageTag in tempJob.find("messages").findall("message"):
                messages.append(messageTag)
        
        #The children
        children = job.find("children")
        assert len(children.findall("child")) == 0 #The children
        assert tempJob.find("children") != None
        for child in tempJob.find("children").findall("child"):
            memory, cpu = getMemoryAndCpuRequirements(config, child)
            ET.SubElement(children, "child", { "command":child.attrib["command"], 
                    "memory":str(memory), "cpu":str(cpu) })
            logger.info("Making a child with command: %s" % (child.attrib["command"]))
        
        #The follow on command
        followOns = job.find("followOns")
        followOns.remove(followOns.findall("followOn")[-1]) #Remove the old job
        if tempJob.attrib.has_key("command"):
            memory, cpu = getMemoryAndCpuRequirements(config, tempJob)
            ET.SubElement(followOns, "followOn", { "command":tempJob.attrib["command"], 
                    "memory":str(memory), "cpu":str(cpu) })
            logger.info("Making a follow on job with command: %s" % tempJob.attrib["command"])
            
        elif len(tempJob.find("children").findall("child")) != 0: #This is to keep the stack of follow on jobs consistent.
            ET.SubElement(followOns, "followOn", { "command":"echo JOB_FILE", "memory":"1000000", "cpu":"1" })
            logger.info("Making a stub follow on job")
    else:
        logger.critical("Failed the job")
        job.attrib["colour"] = "red" #Update the colour
    
    #Clean up
    system("rm -rf %s/*" % (localTempDir))
    logger.info("Cleaned up by removing the contents of the local temporary file directory for the job")
    
    return tempLogFile
    
def main():
    sys.path.append(sys.argv[1])
    sys.argv.remove(sys.argv[1])
    
    #Now we can import all the stuff..
    from sonLib.bioio import getBasicOptionParser
    from sonLib.bioio import parseBasicOptions
    from sonLib.bioio import logger
    from sonLib.bioio import addLoggingFileHandler
    from sonLib.bioio import setLogLevel
    from sonLib.bioio import getTotalCpuTime, getTotalCpuTimeAndMemoryUsage
    from sonLib.bioio import getTempDirectory
    from jobTree.src.master import writeJobs
    from jobTree.src.master import readJob
    from jobTree.src.master import getSlaveLogFileName, getLogFileName, getStatsFileName    
    from sonLib.bioio import system
    
    ##########################################
    #Construct the arguments.
    ##########################################
    
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
  
    parser.add_option("--job", dest="jobFile", 
                      help="Job file containing command to run",
                      default="None")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Location of the job tree",
                      default="None")
    
    options, args = parseBasicOptions(parser)
    assert len(args) == 0

    ##########################################
    #Parse the job.
    ##########################################
    
    job = readJob(options.jobFile)
    config = ET.parse(os.path.join(options.jobTree, "config.xml")).getroot()
    
    ##########################################
    #Setup the logging
    ##########################################
    
    #Setup the logging
    setLogLevel(config.attrib["log_level"])
    addLoggingFileHandler(getSlaveLogFileName(job), rotatingLogging=False)
    logger.info("Parsed arguments and set up logging")
    
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
    #Load the environment for the job
    ##########################################
    
    #First load the environment for the job.
    fileHandle = open(config.attrib["environment_file"], 'r')
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
    logger.info("Loaded the environment for the process")
        
    ##########################################
    #Setup the temporary directories.
    ##########################################
        
    #Dir to put all the temp files in.
    localSlaveTempDir = getTempDirectory()
    localTempDir = os.path.join(localSlaveTempDir, "localTempDir") 
    os.mkdir(localTempDir)
    os.chmod(localTempDir, 0777)
    
    ##########################################
    #Run the script.
    ##########################################
    
    maxTime = float(config.attrib["job_time"])
    assert maxTime > 0.0
    assert maxTime < sys.maxint
    jobToRun = job.find("followOns").findall("followOn")[-1]
    memoryAvailable = int(jobToRun.attrib["memory"])
    cpuAvailable = int(jobToRun.attrib["cpu"])
    startTime = time.time()
    while True:
        tempLogFile = processJob(job, jobToRun, memoryAvailable, cpuAvailable, stats, environment, localSlaveTempDir, localTempDir, config)
        
        if job.attrib["colour"] != "black": 
            logger.critical("Exiting the slave because of a failed job on host %s", socket.gethostname())
            system("mv %s %s" % (tempLogFile, getLogFileName(job))) #Copy back the job log file, because we saw failure
            break
        elif config.attrib.has_key("reportAllJobLogFiles"):
            logger.info("Exiting because we've been asked to report all logs, and this involves returning to the master")
            #Copy across the log file
            system("mv %s %s" % (tempLogFile, getLogFileName(job)))
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
        writeJobs([ job ])
        logger.info("Updated the status of the job to grey and starting the next job")
    
    #Write back the job file with the updated jobs, using the checkpoint method.
    writeJobs([ job ])
    logger.info("Written out an updated job file")
    
    logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds" % (time.time() - startTime))
    
    ##########################################
    #Cleanup the temporary directory
    ##########################################
    
    system("rm -rf %s" % localSlaveTempDir)
    
    ##########################################
    #Finish up the stats
    ##########################################
    
    if stats != None:
        totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
        stats.attrib["time"] = str(time.time() - startTime)
        stats.attrib["clock"] = str(totalCpuTime - startClock)
        stats.attrib["memory"] = str(totalMemoryUsage)
        fileHandle = open(getStatsFileName(job), 'w')
        ET.ElementTree(stats).write(fileHandle)
        fileHandle.close()
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()

