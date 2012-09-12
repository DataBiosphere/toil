
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import os

def readJob(jobFile):
    fileHandle = open(jobFile, 'r')
    job = convertJsonJobToJob(pickler.load(fileHandle))
    fileHandle.close()
    return job

def getJobFileName(globalTempDir):
    return os.path.join(globalTempDir, "job.xml")

def getJobStatsFileName(globalTempDir):
    return os.path.join(globalTempDir, "stats.xml")

def convertToJobToJson(job):
    jsonJob = [ job.globalTempDir,
                job.remainingRetryCount,
                job.colour,
                job.parentJobFile,
                job.issuedChildCount,
                job.completedChildCount,
                job.childCommandsToIssue,
                job.followOnCommandsToIssue,
                job.messages ]
    return jsonJob

def convertJsonJobToJob(jsonJob):
    job = Job("", 0, 0, None, "", 0)
    job.globalTempDir = jsonJob[0] 
    job.remainingRetryCount = jsonJob[1] 
    job.colour = jsonJob[2] 
    job.parentJobFile = jsonJob[3] 
    job.issuedChildCount = jsonJob[4] 
    job.completedChildCount = jsonJob[5] 
    job.childCommandsToIssue = jsonJob[6] 
    job.followOnCommandsToIssue = jsonJob[7] 
    job.messages = jsonJob[8] 
    return job

class Job:
    #Colours for job
    blue = 0
    black = 1
    grey = 2
    red = 3
    dead = 4
    
    @staticmethod
    def translateColourToString(colour):
        colours = { Job.blue:"blue", 
                   Job.black:"black", Job.grey:"grey", 
                   Job.red:"red", Job.dead:"dead" }
        return colours[colour]
    
    def __init__(self, command, memory, cpu, parentJobFile, globalTempDir, retryCount):
        self.globalTempDir = globalTempDir
        self.remainingRetryCount = retryCount
        self.colour = Job.grey
        self.parentJobFile = parentJobFile
        self.issuedChildCount = 0
        self.completedChildCount = 0
        self.childCommandsToIssue = []
        self.followOnCommandsToIssue = []
        self.messages = []
        self.addFollowOnCommand((command, memory, cpu))
    
    def write(self, jobFile):
        fileHandle = open(jobFile, 'w')
        pickler.dump(convertToJobToJson(self), fileHandle)
        fileHandle.close() 
    
    def getJobFileName(self):
        return getJobFileName(self.globalTempDir)
      
    def getLogFileName(self):
        return os.path.join(self.globalTempDir, "log.txt")
        
    def getGlobalTempDirName(self):
        return self.globalTempDir
        
    def getJobStatsFileName(self):
        return getJobStatsFileName(self.globalTempDir)
    
    def getRemainingRetryCount(self):
        return self.remainingRetryCount
    
    def setRemainingRetryCount(self, remainingRetryCount):
        self.remainingRetryCount = remainingRetryCount
    
    def getParentJobFile(self):
        return self.parentJobFile
    
    def getIssuedChildCount(self):
        return self.issuedChildCount
    
    def setIssuedChildCount(self, issuedChildCount):
        self.issuedChildCount = issuedChildCount
    
    def getCompletedChildCount(self):
        return self.completedChildCount
    
    def setCompletedChildCount(self, completedChildCount):
        self.completedChildCount = completedChildCount
        
    def getColour(self):
        return self.colour
    
    def setColour(self, colour):
        self.colour = colour 
        
    def getNumberOfFollowOnCommandsToIssue(self):
        return len(self.followOnCommandsToIssue)
        
    def getNextFollowOnCommandToIssue(self):
        return self.followOnCommandsToIssue[-1]
    
    def popNextFollowOnCommandToIssue(self):
        return self.followOnCommandsToIssue.pop()
    
    def addFollowOnCommand(self, commandTuple):
        self.followOnCommandsToIssue.append(commandTuple)
        
    def getNumberOfChildCommandsToIssue(self):
        return len(self.childCommandsToIssue)
    
    def addChildCommand(self, commandTuple):
        self.childCommandsToIssue.append(commandTuple)
        
    def removeChildrenToIssue(self):
        children = self.childCommandsToIssue
        self.childCommandsToIssue = []
        return children
        
    def getNumberOfMessages(self):
        return len(self.messages)
    
    def removeMessages(self):
        messages = self.messages
        self.messages = []
        return messages
    
    def addMessage(self, message):
        self.messages.append(message)