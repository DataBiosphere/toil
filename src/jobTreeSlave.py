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

def truncateFile(fileNameString, toBig=50000):
    """Truncates a file that is bigger than toBig bytes, leaving only the 
    last toBig bytes in the file.
    """
    if os.path.getsize(fileNameString) > toBig:
        fh = open(fileNameString, 'rb+')
        fh.seek(-toBig, 2) 
        data = fh.read()
        fh.seek(0) # rewind
        fh.write(data)
        fh.truncate()
        fh.close()
        
def getMemoryCpuAndTimeRequirements(job, nextJob):
    """Gets the memory and cpu requirements from the job..
    """
    #Now deal with the CPU and memory..
    memory = job.attrib["default_memory"]
    cpu = job.attrib["default_cpu"]
    compTime = sys.maxint
    if nextJob.attrib.has_key("memory"):
        memory = max(int(nextJob.attrib["memory"]), 0)
    if nextJob.attrib.has_key("cpu"):
        cpu = max(int(nextJob.attrib["cpu"]), 0)
    if nextJob.attrib.has_key("time"):
        compTime = max(float(nextJob.attrib["time"]), 0.0)
    return memory, cpu, compTime
 
def processJob(job, jobToRun, memoryAvailable, cpuAvailable, stats):
    """Runs a job.
    """
    from sonLib.bioio import getTempFile
    from sonLib.bioio import getTempDirectory
    from sonLib.bioio import logger
    from sonLib.bioio import system
    from sonLib.bioio import getTotalCpuTime
    
    assert len(job.find("children").findall("child")) == 0
    assert int(job.attrib["child_count"]) == int(job.attrib["black_child_count"])
    command = jobToRun.attrib["command"]
    #Copy the job file to be edited
    
    tempJob = ET.Element("job")
    ET.SubElement(tempJob, "children")
    
    #Log for job
    tempJob.attrib["log_level"] = job.attrib["log_level"]
    
    #Time length of 'ideal' job before further parallelism is required
    tempJob.attrib["job_time"] = job.attrib["job_time"]
    
    #Dir to put all the temp files in.
    localSlaveTempDir = getTempDirectory()

    #Temp file dirs for job.
    localTempDir = getTempDirectory(rootDir=localSlaveTempDir)
    tempJob.attrib["local_temp_dir"] = localTempDir
    depth = len(job.find("followOns").findall("followOn"))
    tempJob.attrib["global_temp_dir"] = os.path.join(job.attrib["global_temp_dir"], str(depth))
    if not os.path.isdir(tempJob.attrib["global_temp_dir"]): #Ensures that the global temp dirs of each level are kept separate.
        os.mkdir(tempJob.attrib["global_temp_dir"])
        os.chmod(tempJob.attrib["global_temp_dir"], 0777)
    if os.path.isdir(os.path.join(job.attrib["global_temp_dir"], str(depth+1))):
        system("rm -rf %s" % os.path.join(job.attrib["global_temp_dir"], str(depth+1)))
    assert not os.path.isdir(os.path.join(job.attrib["global_temp_dir"], str(depth+2)))
    
    #Deal with memory and cpu requirements (this pass tells the running job how much cpu and memory they have,
    #according to the batch system
    tempJob.attrib["available_memory"] = str(memoryAvailable)
    tempJob.attrib["available_cpu"] = str(cpuAvailable)
    if stats != None:
        tempJob.attrib["stats"] = getTempFile(rootDir=localSlaveTempDir)
        os.remove(tempJob.attrib["stats"])
    
    #Now write the temp job file
    tempFile = getTempFile(rootDir=localSlaveTempDir)
    fileHandle = open(tempFile, 'w') 
    tree = ET.ElementTree(tempJob)
    tree.write(fileHandle)
    fileHandle.close()
    logger.info("Copied the jobs files ready for the job")
    
    if "JOB_FILE" not in command:
        logger.critical("There is no 'JOB_FILE' string in the command to be run to take the job-file argument: %s" % command)
        job.attrib["colour"] = "red" #Update the colour
    else:
        #First load the environment for the job.
        fileHandle = open(job.attrib["environment_file"], 'r')
        environment = cPickle.load(fileHandle)
        fileHandle.close()
        logger.info("Loaded the environment for the process")
        
        #Run the actual command
        tempLogFile = getTempFile(suffix=".log", rootDir=localSlaveTempDir)
        fileHandle = open(tempLogFile, 'w')
        finalCommand = command.replace("JOB_FILE", tempFile)
        if stats != None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        process = subprocess.Popen(finalCommand, shell=True, stdout=fileHandle, stderr=subprocess.STDOUT, env=environment)
            
        sts = os.waitpid(process.pid, 0)
        fileHandle.close()
        truncateFile(tempLogFile, int(job.attrib["max_log_file_size"]))
        
        #Copy across the log file
        system("mv %s %s" % (tempLogFile, job.attrib["log_file"]))
        i = sts[1]
        
        logger.info("Ran the job command=%s with exit status %i" % (finalCommand, i))
        
        if i == 0:
            logger.info("Passed the job, okay")
            
            if stats != None:
                jobTag = ET.SubElement(stats, "job", { "time":str(time.time() - startTime), "clock":str(getTotalCpuTime() - startClock) })
                if os.path.exists(tempJob.attrib["stats"]):
                    jobTag.append(ET.parse(tempJob.attrib["stats"]).getroot())
            
            tempJob = ET.parse(tempFile).getroot()
            job.attrib["colour"] = "black" #Update the colour
            
            #Deal with any logging messages directed at the master
            if tempJob.find("messages") != None:
                messages = job.find("messages")
                if messages == None:
                    messages = ET.SubElement(job, "messages")
                for messageTag in tempJob.find("messages").findall("message"):
                    messages.append(messageTag)
            
            #Update the runtime of the stack..
            totalRuntime = float(job.attrib["total_time"])  #This is the estimate runtime of the jobs on the followon stack
            runtime = float(jobToRun.attrib["time"])
            totalRuntime -= runtime
            if totalRuntime < 0.0:
                totalRuntime = 0.0
            
            #The children
            children = job.find("children")
            assert len(children.findall("child")) == 0 #The children
            assert tempJob.find("children") != None
            for child in tempJob.find("children").findall("child"):
                memory, cpu, compTime = getMemoryCpuAndTimeRequirements(job, child)
                ET.SubElement(children, "child", { "command":child.attrib["command"], 
                        "time":str(compTime), "memory":str(memory), "cpu":str(cpu) })
                logger.info("Making a child with command: %s" % (child.attrib["command"]))
            
            #The follow on command
            followOns = job.find("followOns")
            followOns.remove(followOns.findall("followOn")[-1]) #Remove the old job
            if tempJob.attrib.has_key("command"):
                memory, cpu, compTime = getMemoryCpuAndTimeRequirements(job, tempJob)
                ET.SubElement(followOns, "followOn", { "command":tempJob.attrib["command"], 
                        "time":str(compTime), "memory":str(memory), "cpu":str(cpu) })
                ##Add the runtime to the total runtime..
                totalRuntime += compTime
                logger.info("Making a follow on job with command: %s" % tempJob.attrib["command"])
                
            elif len(tempJob.find("children").findall("child")) != 0: #This is to keep the stack of follow on jobs consistent.
                ET.SubElement(followOns, "followOn", { "command":"echo JOB_FILE", "time":"0", "memory":"1000000", "cpu":"1" })
                logger.info("Making a stub follow on job")
            #Write back the runtime, after addin the follow on time and subtracting the time of the run job.
            job.attrib["total_time"] = str(totalRuntime)
        else:
            logger.critical("Failed the job")
            job.attrib["colour"] = "red" #Update the colour
    
    #Clean up
    system("rm -rf %s" % (localSlaveTempDir))
    logger.info("Cleaned up by removing temp jobfile (the copy), and the temporary file directory for the job")
    
def main():
    sys.path +=  [ sys.argv[1] ]
    sys.argv.remove(sys.argv[1])
    
    #Now we can import all the stuff..
    from sonLib.bioio import getBasicOptionParser
    from sonLib.bioio import parseBasicOptions
    from sonLib.bioio import logger
    from sonLib.bioio import addLoggingFileHandler
    from sonLib.bioio import setLogLevel
    from sonLib.bioio import getTotalCpuTime
    
    from jobTree.src.master import writeJobs
    
    ##########################################
    #Construct the arguments.
    ##########################################
    
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
    parser.add_option("--job", dest="jobFile", 
                      help="Job file containing command to run",
                      default="None")
    
    options, args = parseBasicOptions(parser)
    assert len(args) == 0

    ##########################################
    #Parse the job.
    ##########################################
    
    job = ET.parse(options.jobFile).getroot()
    
    ##########################################
    #Setup the logging
    ##########################################
    
    #Setup the logging
    setLogLevel(job.attrib["log_level"])
    addLoggingFileHandler(job.attrib["slave_log_file"], rotatingLogging=False)
    logger.info("Parsed arguments and set up logging")
    
    ##########################################
    #Setup the stats, if requested
    ##########################################
    
    if job.attrib.has_key("stats"):
        startTime = time.time()
        startClock = time.clock()
        stats = ET.Element("slave")
    else:
        stats = None
    
    ##########################################
    #Run the script.
    ##########################################
    
    maxTime = float(job.attrib["job_time"])
    assert maxTime > 0.0
    assert maxTime < sys.maxint
    jobToRun = job.find("followOns").findall("followOn")[-1]
    memoryAvailable = int(jobToRun.attrib["memory"])
    cpuAvailable = int(jobToRun.attrib["cpu"])
    while True:
        processJob(job, jobToRun, memoryAvailable, cpuAvailable, stats)
        
        if job.attrib["colour"] != "black":
            logger.critical("Exiting the slave because of a failed job")
            break
   
        totalRuntime = float(job.attrib["total_time"])  #This is the estimate runtime of the jobs on the followon stack
        
        childrenNode = job.find("children")
        childrenList = childrenNode.findall("child")
        #childRuntime = sum([ float(child.attrib["time"]) for child in childrenList ])
            
        if len(childrenList) >= 2: # or totalRuntime + childRuntime > maxTime: #We are going to have to return to the parent
            logger.info("No more jobs can run in series by this slave, its got %i children" % len(childrenList))
            break
        
        followOns = job.find("followOns")
        while len(childrenList) > 0:
            child = childrenList.pop()
            childrenNode.remove(child)
            totalRuntime += float(child.attrib["time"])
            ET.SubElement(followOns, "followOn", child.attrib.copy())
        #assert totalRuntime <= maxTime + 1 #The plus one second to avoid unimportant rounding errors
        job.attrib["total_time"] = str(totalRuntime)
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
    
    logger.info("Finished running the chain of jobs on this node")
    
    ##########################################
    #Finish up the stats
    ##########################################
    
    if stats != None:
        stats.attrib["time"] = str(time.time() - startTime)
        stats.attrib["clock"] = str(getTotalCpuTime() - startClock)
        fileHandle = open(job.attrib["stats"], 'w')
        ET.ElementTree(stats).write(fileHandle)
        fileHandle.close()
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()

