import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
from sonLib.bioio import logger

class Job:
    def __init__(self, command, memory, cpu, 
                 tryCount, jobStoreID, logJobStoreFileID):
        self.remainingRetryCount = tryCount
        self.jobStoreID = jobStoreID
        self.children = []
        self.followOnCommands = []
        self.followOnCommands.append((command, memory, cpu, 0))
        self.messages = []
        self.logJobStoreFileID = logJobStoreFileID

    def setupJobAfterFailure(self, config):
        if len(self.followOnCommands) > 0:
            self.remainingRetryCount = max(0, self.remainingRetryCount-1)
            logger.critical("Due to failure we are reducing the remaining retry \
            count of job %s to %s" % (self.jobStoreID, self.remainingRetryCount))
            #Set the default memory to be at least as large as the default, in 
            #case this was a malloc failure (we do this because of the combined
            #batch system)
            self.followOnCommands[-1] = (self.followOnCommands[-1][0], \
            max(self.followOnCommands[-1][1], float(config.attrib["default_memory"]))) + \
            self.followOnCommands[-1][2:]
            logger.critical("We have set the default memory of the failed job to %s bytes" \
                            % self.followOnCommands[-1][1])
        else:
            logger.critical("The job %s has no follow on jobs to reset" % self.jobStoreID)
    
    def clearLogFile(self, jobStore):
        """Clears the log file, if it is set.
        """
        if self.logJobStoreFileID != None:
            jobStore.deleteFile(self.logJobStoreFileID)
            self.logJobStoreFileID = None
    
    def setLogFile(self, logFile, jobStore):
        """Sets the log file in the file store. 
        """
        if self.logJobStoreFileID != None: #File already exists
            jobStore.updateFile(self.logJobStoreFileID, logFile)
        else:
            self.logJobStoreFileID = jobStore.writeFile(self.jobStoreID, logFile)
            assert self.logJobStoreFileID != None
    
    def getLogFileHandle(self, jobStore):
        """Returns a file handle to the log file, or None if not set.
        """
        return None if self.logJobStoreFileID == None else \
            jobStore.readFileStream(self.logJobStoreFileID)

    # FIXME: Remove json from method name. If it's not a string it can't be referred to as JSON
    # simply because JSON is a serialization format.

    # FIXME: consider just returning Job.__dict__ as that would be less brittle

    def convertJobToJson(job):
        jsonJob = [ job.remainingRetryCount,
                    job.jobStoreID,
                    job.children,
                    job.followOnCommands,
                    job.messages,
                    job.logJobStoreFileID ]
        return jsonJob

    # FIXME: see above

    # FIXME: consider using Job(**kwargs) as that would be less brittle
    
    @staticmethod
    def convertJsonJobToJob(jsonJob):
        job = Job("", 0, 0, 0, None, None)
        job.remainingRetryCount = jsonJob[0] 
        job.jobStoreID = jsonJob[1]
        job.children = jsonJob[2] 
        job.followOnCommands = jsonJob[3] 
        job.messages = jsonJob[4] 
        job.logJobStoreFileID = jsonJob[5]
        return job
