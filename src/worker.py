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
import importlib

import os
import sys
import xml.etree.cElementTree as ET
import cPickle
import traceback
import time
import socket
import logging

logger = logging.getLogger( __name__ )


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


def loadStack(command,jobStore):
    commandTokens = command.split()
    assert commandTokens[0] == "scriptTree"
    moduleDirPath = commandTokens[2]
    if moduleDirPath not in sys.path:
        sys.path.append(moduleDirPath)
    for targetClassName in commandTokens[3:]:
        l = targetClassName.split(".")
        moduleName = ".".join(l[:-1])
        targetClassName = l[-1]
        targetModule = importlib.import_module(moduleName)
        thisModule = sys.modules[__name__]
        thisModule.__dict__[targetClassName] = targetModule.__dict__[targetClassName]
    return loadPickleFile(commandTokens[1], jobStore)
        
def loadPickleFile(pickleFile,jobStore):
    """Loads the first object from a pickle file.
    """
    with jobStore.readFileStream(pickleFile) as fileHandle:
        return cPickle.load( fileHandle )
    
def nextOpenDescriptor():
    """Gets the number of the next available file descriptor.
    """
    descriptor = os.open("/dev/null", os.O_RDONLY)
    os.close(descriptor)
    return descriptor
    
def main():
    sys.path.append(sys.argv[1])
    sys.argv.remove(sys.argv[1])
    
    #Now we can import all the stuff..
    from jobTree.lib.bioio import setLogLevel
    from jobTree.lib.bioio import getTotalCpuTime
    from jobTree.lib.bioio import getTotalCpuTimeAndMemoryUsage
    from jobTree.lib.bioio import getTempDirectory
    from jobTree.lib.bioio import makeSubDir
    from jobTree.lib.bioio import system

    from jobTree.src.common import loadJobStore
    
    ########################################## 
    #Input args
    ##########################################
    
    jobStoreString = sys.argv[1]
    jobStoreID = sys.argv[2]
    
    ##########################################
    #Load the jobStore/config file
    ##########################################
    
    jobStore = loadJobStore(jobStoreString)
    config = jobStore.config 
    setLogLevel(config.attrib["log_level"])
    
    ##########################################
    #Load the environment for the job
    ##########################################
    
    #First load the environment for the job.
    with jobStore.readSharedFileStream("environment.pickle") as fileHandle:
        environment = cPickle.load(fileHandle)
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
    localWorkerTempDir = getTempDirectory()
    localTempDir = makeSubDir(os.path.join(localWorkerTempDir, "localTempDir"))
    
    ##########################################
    #Setup the logging
    ##########################################
    
    #Setup the logging. This is mildly tricky because we don't just want to
    #redirect stdout and stderr for this Python process; we want to redirect it
    #for this process and all children. Consequently, we can't just replace
    #sys.stdout and sys.stderr; we need to mess with the underlying OS-level
    #file descriptors. See <http://stackoverflow.com/a/11632982/402891>
    
    #When we start, standard input is file descriptor 0, standard output is
    #file descriptor 1, and standard error is file descriptor 2.

    #What file do we want to point FDs 1 and 2 to?    
    tempWorkerLogPath = os.path.join(localWorkerTempDir, "worker_log.txt")
    
    #Save the original stdout and stderr (by opening new file descriptors to the
    #same files)
    origStdOut = os.dup(1)
    origStdErr = os.dup(2)
    
    #Open the file to send stdout/stderr to.
    logFh = os.open(tempWorkerLogPath, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

    #Replace standard output with a descriptor for the log file
    os.dup2(logFh, 1)
    
    #Replace standard error with a descriptor for the log file
    os.dup2(logFh, 2)
    
    #Since we only opened the file once, all the descriptors duped from the
    #original will share offset information, and won't clobber each others'
    #writes. See <http://stackoverflow.com/a/5284108/402891>. This shouldn't
    #matter, since O_APPEND seeks to the end of the file before every write, but
    #maybe there's something odd going on...
    
    #Close the descriptor we used to open the file
    os.close(logFh)
    
    for handler in list(logger.handlers): #Remove old handlers
        logger.removeHandler(handler)
    
    #Add the new handler. The sys.stderr stream has been redirected by swapping
    #the file descriptor out from under it.
    logger.addHandler(logging.StreamHandler(sys.stderr))

    #Put a message at the top of the log, just to make sure it's working.
    print "---JOBTREE WORKER OUTPUT LOG---"
    sys.stdout.flush()
    
    #Log the number of open file descriptors so we can tell if we're leaking
    #them.
    logger.debug("Next available file descriptor: {}".format(
        nextOpenDescriptor()))
    
    ##########################################
    #Get job info
    ##########################################
    
    job = jobStore.load(jobStoreID)
    job.messages = [] #This is the only way to stop messages logging twice, 
    #as are read only in the master
    job.children = [] #Similarly, this is where old children are flushed out.
    if job.logJobStoreFileID != None:
        job.clearLogFile(jobStore) #This cleans the old log file
    jobStore.store(job) #Update status, to avoid reissuing children after
    #running a follow on below.
    logger.info("Parsed arguments and set up logging")

     #Try loop for worker logging
    ##########################################
    #Setup the stats, if requested
    ##########################################
    
    if config.attrib.has_key("stats"):
        startTime = time.time()
        startClock = getTotalCpuTime()
        stats = ET.Element("worker")
    else:
        stats = None
    
    ##########################################
    #The max time 
    ##########################################
    
    maxTime = float(config.attrib["job_time"])
    assert maxTime > 0.0
    assert maxTime < sys.maxint

    ##########################################
    #Worker log file trapped from here on in
    ##########################################

    workerFailed = False
    try:
        
        ##########################################
        #The next job
        ##########################################
        
        command, memoryAvailable, cpuAvailable, depth = job.followOnCommands[-1]
        defaultMemory = int(config.attrib["default_memory"])
        defaultCpu = int(config.attrib["default_cpu"])
        assert len(job.children) == 0
        
        startTime = time.time() 
        while True:
            job.followOnCommands.pop()
                
            ##########################################
            #Old children, not yet deleted
            #
            #These may exist because of the lazy cleanup
            #we do
            ##########################################
        
            #for childDir in listChildDirs(job.jobStoreID):
            #    logger.debug("Cleaning up old child %s" % childDir)
            #    system("rm -rf %s" % childDir)
            #    jobStore.delete()
        
            ##########################################
            #Run the job
            ##########################################
        
            if command != "": #Not a stub
                if command[:11] == "scriptTree ":
                    ##########################################
                    #Run the target
                    ##########################################

                    loadStack(command,jobStore).execute(job=job, stats=stats,
                                    localTempDir=localTempDir, jobStore=jobStore, 
                                    memoryAvailable=memoryAvailable, 
                                    cpuAvailable=cpuAvailable, 
                                    defaultMemory=defaultMemory, 
                                    defaultCpu=defaultCpu, depth=depth)
            
                else: #Is another command
                    system(command)
            
            ##########################################
            #Cleanup/reset a successful job/checkpoint
            ##########################################
            
            job.remainingRetryCount = int(config.attrib["try_count"])
            system("rm -rf %s/*" % (localTempDir))
            
            if len(job.children) == 1: #If job has a single child, 
                #just make it a follow on
                job.followOnCommands.append(job.children.pop() + (depth + 1,))
            
            childCommands = job.children #This is a hack until we stop 
            #overloading the use of this array
            job.children = []
            jobStore.addChildren(job=job, childCommands=childCommands)
            
            ##########################################
            #Establish if we can run another job
            ##########################################
            
            if time.time() - startTime > maxTime:
                # FIXME: Shouldn't we raise an exception here so we can see the stack trace in the
                # FIXME: ... master? Without an exception, the log is swallowed and no one sees it.
                logger.info("We are breaking because the maximum time the job should run for has been exceeded")
                break
            
            #Deal with children
            if len(job.children) >= 1:  #We are going to have to return to the parent
                logger.info("No more jobs can run in series by this worker, its got %i children" % len(job.children))
                break
            
            if len(job.followOnCommands) == 0:
                logger.info("No more jobs can run by this worker as we have exhausted the follow ons")
                break
            
            #Get the next job and see if we have enough cpu and memory to run it..
            command, memory, cpu, depth = job.followOnCommands[-1]
            
            if memory > memoryAvailable:
                # FIXME: Shouldn't we raise an exception here so we can see the stack trace in the
                # FIXME: ... master? Without an exception, the log is swallowed and no one sees it.
                logger.info("We need more memory for the next job, so finishing")
                break
            if cpu > cpuAvailable:
                # FIXME: Shouldn't we raise an exception here so we can see the stack trace in the
                # FIXME: ... master? Without an exception, the log is swallowed and no one sees it.
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
            jobStore.writeStats(ET.tostring(stats))
        
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds" % (time.time() - startTime))
    
    ##########################################
    #Where worker goes wrong
    ##########################################
    except: #Case that something goes wrong in worker
        traceback.print_exc()
        logger.critical("Exiting the worker because of a failed job on host %s", socket.gethostname())
        job = jobStore.load(jobStoreID)
        job.setupJobAfterFailure(config)
        workerFailed = True

    ##########################################
    #Cleanup
    ##########################################
    
    #Close the worker logging
    #Flush at the Python level
    sys.stdout.flush()
    sys.stderr.flush()
    #Flush at the OS level
    os.fsync(1)
    os.fsync(2)
    
    #Close redirected stdout and replace with the original standard output.
    os.dup2(origStdOut, 1)
    
    #Close redirected stderr and replace with the original standard error.
    os.dup2(origStdOut, 2)
    
    #sys.stdout and sys.stderr don't need to be modified at all. We don't need
    #to call redirectLoggerStreamHandlers since they still log to sys.stderr
    
    #Close our extra handles to the original standard output and standard error
    #streams, so we don't leak file handles.
    os.close(origStdOut)
    os.close(origStdErr)
    
    #Now our file handles are in exactly the state they were in before.
    
    #Copy back the log file to the global dir, if needed
    if workerFailed:
        truncateFile(tempWorkerLogPath)
        job.setLogFile(tempWorkerLogPath, jobStore)
        os.remove(tempWorkerLogPath)
        jobStore.store(job)

    #Remove the temp dir
    system("rm -rf %s" % localWorkerTempDir)
    
    #This must happen after the log file is done with, else there is no place to put the log
    if (not workerFailed) and len(job.followOnCommands) == 0 and len(job.children) == 0:
        #Cleanup global files at the end of the chain
        
        if len(job.messages) == 0:
            #We can also safely get rid of the job
            jobStore.delete(job)
            
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
