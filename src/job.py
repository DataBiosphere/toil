
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import os
import re
from jobTree.src.bioio import system

def getJobFileName(jobDir):
    return os.path.join(jobDir, "job")

def getJobLogFileName(jobDir):
    return os.path.join(jobDir, "log.txt")

class Job:
    def __init__(self, command, memory, cpu, tryCount, jobDir):
        self.remainingRetryCount = tryCount
        self.jobDir = jobDir
        self.children = []
        self.followOnCommands = []
        self.followOnCommands.append((command, memory, cpu, 0))
        self.messages = []
    
    def getJobFileName(self):
        return getJobFileName(self.jobDir)
      
    def getLogFileName(self):
        return getJobLogFileName(self.jobDir)
        
    def getGlobalTempDirName(self):
        return os.path.join(self.jobDir, "gTD")
    
    @staticmethod
    def read(jobFile):
        """Loads a job from disk.
        """
        fileHandle = open(jobFile, 'r')
        job = _convertJsonJobToJob(pickler.load(fileHandle))
        fileHandle.close()
        return job

    def write(self):
        """Updates a job's status on disk atomically
        """
        updatingFile = os.path.join(self.jobDir, "updating")
        open(updatingFile, 'w').close()
        self._write(".new")
        os.remove(updatingFile)
        os.rename(self.getJobFileName() + ".new", self.getJobFileName())
        return self.getJobFileName()
    
    def delete(self):
        """Removes from disk atomically, can not then subsequently call read(), write() or addChildren()
        """
        os.remove(self.getJobFileName()) #This is the atomic operation, if this file is not present the job is deleted.
        dirToRemove = self.jobDir
        while 1:
            head, tail = os.path.split(dirToRemove)
            if re.match("t[0-9]+$", tail):
                command = "rm -rf %s" % dirToRemove
            else:
                command = "rm -rf %s/*" % dirToRemove #We're at the root
            try:
                system(command)
            except RuntimeError:
                pass #This is not a big deal, as we expect collisions
            dirToRemove = head
            try:
                if len(os.listdir(dirToRemove)) != 0:
                    break
            except os.error: #In case stuff went wrong, but as this is not critical we let it slide
                break
    
    def update(self, depth, tryCount):
        """Creates a set of child jobs for the given job and updates state of job atomically on disk with new children.
        """
        updatingFile = self.getJobFileName() + ".updating"
        open(updatingFile, 'w').close()
        if len(self.children) == 1: #Just make it a follow on
            self.followOnCommands.append(self.children.pop() + (depth + 1,))
        elif len(self.children) > 1:
            self.children = [ (Job(command, memory, cpu, tryCount, tempDir).write(), memory, cpu) for ((command, memory, cpu), tempDir) in zip(self.children, _createTempDirectories(self.jobDir, len(self.children))) ]
        self._write(".new")
        os.remove(updatingFile)
        os.rename(self.getJobFileName() + ".new", self.getJobFileName())
        
    def _write(self, suffix=""):
        fileHandle = open(self.getJobFileName() + suffix, 'w')
        pickler.dump(_convertJobToJson(self), fileHandle)
        fileHandle.close()

"""Private functions
"""

def _convertJobToJson(job):
    jsonJob = [ job.remainingRetryCount,
                job.jobDir,
                job.children,
                job.followOnCommands,
                job.messages ]
    return jsonJob

def _convertJsonJobToJob(jsonJob):
    job = Job("", 0, 0, 0, None)
    job.remainingRetryCount = jsonJob[0] 
    job.jobDir = jsonJob[1]
    job.children = jsonJob[2] 
    job.followOnCommands = jsonJob[3] 
    job.messages = jsonJob[4] 
    return job
        
def _createTempDirectories(rootDir, number, filesPerDir=4):
    def fn(i):
        dirName = os.path.join(rootDir, "t%i" % i)
        os.mkdir(dirName)
        return dirName
    if number > filesPerDir:
        if number % filesPerDir != 0:
            return reduce(lambda x,y:x+y, [ _createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir) for i in range(filesPerDir-1) ], _createTempDirectories(fn(0), (number % filesPerDir) + number/filesPerDir, filesPerDir)) 
        else:
            return reduce(lambda x,y:x+y, [ _createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir) for i in range(filesPerDir) ], []) 
    else:
        return [ fn(i) for i in xrange(number) ]
