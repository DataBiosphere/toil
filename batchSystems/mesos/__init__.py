import os
import time
from jobTree.batchSystems.mesos.mesosScheduler import MesosScheduler
from jobTree.batchSystems.mesos.JobTreeJob import JobTreeJob
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import mesosExecutor
from Queue import Queue
from sonLib.bioio import logger


class MesosBatchSystem(AbstractBatchSystem):
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        self.killedQueue = Queue() # implement in scheduler
        self.currentJobs = []
        self.nextJobID = 0
        self.mesosThread = MesosScheduler(masterIP="127.0.0.1:5050")
        self.mesosThread.setDaemon(True)
        self.mesosThread.start()

    def issueJob(self, command, memory, cpu):
        # puts job into job_type_queue to be run by mesos, AND puts jobID in current_job[]
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentJobs.append(jobID)

        # TODO: this is convoluted, construct ResourceSummary here and pass to JobTreeJob constructor
        job = JobTreeJob(jobID=jobID, cpu=cpu, memory=memory, command=command, cwd=os.getcwd())
        job_type = job.resources

        # if job type already described, add to queue. If not, create dictionary entry & add.
        if job_type in self.mesosThread.jobQueueDict:
            self.mesosThread.jobQueueDict[job_type].put(job)
        else:
            self.mesosThread.jobQueueDict[job_type] = Queue()
            self.mesosThread.jobQueueDict[job_type].put(job)

        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def killJobs(self, jobIDs):
        # kills jobs described by jobIDs
        for jobID in jobIDs:
            self.mesosThread.killQueue.put(jobID)

    def getIssuedJobIDs(self):
        # returning from our local current jobs, not those passed to mesos
        return list(self.currentJobs)

    def getRunningJobIDs(self):
        currentTime= dict()
        for x in self.mesosThread.jobTimeMap:
            currentTime[x.key]= time.time()-x.value

    def getUpdatedJob(self, maxWait):
        i = self.getFromQueueSafely(self.mesosThread.updatedJobsQueue, maxWait)
        if i == None:
            return None
        jobID, retcode = i
        self.mesosThread.updatedJobsQueue.task_done()
        return i

    def getWaitDuration(self):
        return 0

    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def getJobs(self):
        return self.mesosThread.jobQueueDict