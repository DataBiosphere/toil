#!/usr/bin/env python

import os 
import re
import subprocess
import time
import sys


from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.abstractBatchSystem import AbstractBatchSystem


#2,097,152 K
def getjobexitcode(tmpFileForStdOut, jobid):
    process = subprocess.Popen(["qacct", "-j", str(jobid)], stdout = subprocess.PIPE,stderr = subprocess.STDOUT)
    for line in process.communicate()[0].split("\n"):
        if line.startswith("exit_status"):
            return int(line.split()[1])
    return None
def getjobinfo(jobid):
    process = subprocess.Popen(["qstat", "-j",str(jobid)], stdout = subprocess.PIPE)
    jobinfo = list()
    headers = list()
    results = process.communicate()[0].split("\n")
    for currline in results:
        if currline.startswith("submission_time"):
            jobtime =  " ".join(currline.split()[1:])
            return time.mktime(time.strptime(jobtime,"%a %b %d %H:%M:%S %Y"))
def killjob(jobid, tmpFileForStdOut):
    fileHandle = open(tmpFileForStdOut, 'w')
    process = subprocess.Popen(["qdel",str(jobid)], stdout=fileHandle)

def addjob(jobcommand, tmpFileForStdOut, cores = None, mem = None, out = "/dev/null"):
    qsubline = list(["qsub","-b","y","-terse","-j" ,"y", "-v", "LD_LIBRARY_PATH=%s" % os.environ["LD_LIBRARY_PATH"], "-o", out ])
    
    reqline = list()
    ## setting -l cpu=1 means that only a few jobs run at a time
    cores = None
    if cores is not None:
        reqline.append("p="+str(cores))
    if mem is not None:
        reqline.append("vf="+str(mem/ 1024)+"K")
    if len(reqline) > 0:
        qsubline.extend(["-hard","-l", ",".join(reqline)])
        
    qsubline.append(jobcommand)
    logger.debug("**"+" ".join(qsubline))
    process = subprocess.Popen(qsubline, stdout=subprocess.PIPE)
    result = None
        
    line = process.communicate()[0].split("\n")[0]
    #print "**ERROR"+str(process.returncode)
    #print "**"+line
    result = int(line)
    return result
    #print "**"+" ".join(qsubline)
    #while True:
        #time.sleep(100)
        



class GridengineBatchSystem(AbstractBatchSystem):
    """The interface for gridengine.
    """
    
    def __init__(self, config):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        self.gridengineResultsFile = config.attrib["results_file"]
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.gridengineResultsFileHandle = open(self.gridengineResultsFile, 'w')
        self.gridengineResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence
        self.scratchFile = self.config.attrib["scratch_file"]
        self.currentjobs = set()
        
    def __des__(self):
        #Closes the file handle associated with the results file.
        self.gridengineResultsFileHandle.close() #Close the results file, cos were done.
        
    def issueJobs(self, jobCommands):
        """Issues grid engine with job commands.
        """
        issuedJobs = {}
        for command, memory, cpu , outfile in jobCommands:
            #print command
  
            #time.sleep(0.1) #Sleep to let parasol catch up #Apparently unnecessarys
            jobID = addjob(command, self.scratchFile,cores = cpu, mem = memory, out = outfile)
            

            logger.debug("Got the job id: %s" % (jobID))
            assert jobID not in issuedJobs.keys()
            issuedJobs[jobID] = command
            logger.debug("Issued the job command: %s with job id: %i " % (command, jobID))
            self.currentjobs.add(jobID)
        return issuedJobs
    
    def killJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        for jobID in jobIDs:
            self.currentjobs.remove(jobID)
            killjob(jobID)
    
    def getIssuedJobIDs(self):
        """Gets the list of jobs issued to parasol.
        """
        #Example issued job, first field is jobID, last is the results file
        #31816891 localhost  benedictpaten 2009/07/23 10:54:09 python ~/Desktop/out.txt           
        return self.currentjobs
    
    def getRunningJobIDs(self):
        times = {}
        for currjob in self.getIssuedJobIDs():
            time = time.time() - getjobinfo(currjob["job-ID"])
            times[currjob] = time
        return times
    
    def getUpdatedJobs(self):
        retcodes = {}
        for currjob in self.currentjobs:
            
            exit = getjobexitcode(self.scratchFile, currjob)
            if exit is not None:
                
                retcodes[currjob] = exit 
        self.currentjobs -= set(retcodes.keys())
        return retcodes
    
    def getWaitDuration(self):
        """We give parasol a second to catch its breath (in seconds)
        """
        return 0.0
    
    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour
        
def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
