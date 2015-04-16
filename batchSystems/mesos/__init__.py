__author__ = 'CJ'
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from Queue import Queue
from sonLib.bioio import logger
from jobTree.batchSystems.mesos import JobTreeJob


class MesosBatchSystem(AbstractBatchSystem):
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        # job type queue is a dictionary of queues. Jobs are grouped in queues with resource requirements as keys.
        self.queue_dictionary = {}
        self.kill_queue = Queue()
        self.current_jobs = []
        self.nextJobID = 0

    def issueJob(self, command, memory, cpu):
        # puts job into job_type_queue to be run by mesos
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.current_jobs.append(jobID)

        job = JobTreeJob(jobID=jobID, cpu=cpu, memory=memory, command=command)
        job_type = job.resources

        # if job type already described, add to queue. If not, create dictionary entry & add.
        if job_type in self.queue_dictionary:
            self.queue_dictionary[job_type].put(job)
        else:
            self.queue_dictionary[job_type] = Queue()
            self.queue_dictionary[job_type].put(job)

        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def killJobs(self, jobIDs):
        # kills jobs described by jobIDs
        for jobID in jobIDs:
            self.kill_queue.put(jobID)

    def getIssuedJobIDs(self):
        jobIDs = []
        for job_type in self.queue_dictionary:
            for job in job_type:
                jobIDs.append(job.jobID)
        return jobIDs

    def getJobs(self):
        return self.queue_dictionary
