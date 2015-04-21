__author__ = 'CJ'
import os
import sys
from jobTree.batchSystems.mesos.mesosScheduler import MesosSchedulerDriver, MesosScheduler
from jobTree.batchSystems.mesos.JobTreeJob import JobTreeJob
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from threading import Thread
from Queue import Queue
from sonLib.bioio import logger
from mesos.interface import mesos_pb2


class MesosBatchSystem(AbstractBatchSystem):
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        # job type queue is a dictionary of queues. Jobs are grouped in queues with resource requirements as keys.
        self.queue_dictionary = {}
        self.kill_queue = Queue()
        self.killed_queue = Queue()
        self.running_dictionary = {}
        self.updatedJobsQueue = Queue()
        self.current_jobs = []
        self.nextJobID = 0
        self.mesos_thread = MesosFrameWorkThread(queue_dictionary=self.queue_dictionary, master_ip="127.0.0.1:5050",
                                                 kill_queue=self.kill_queue, running_dictionary=self.running_dictionary,
                                                 updated_job_queue=self.updatedJobsQueue)
        self.mesos_thread.start()

    def issueJob(self, command, memory, cpu):
        # puts job into job_type_queue to be run by mesos, AND puts jobID in current_job[]
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.current_jobs.append(jobID)

        job = JobTreeJob(jobID=jobID, cpu=cpu, memory=memory, command=command, cwd=os.getcwd())
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
        # returning from our local current jobs, not those passed to mesos
        return list(self.current_jobs)

    def getRunningJobIDs(self):
        return self.running_dictionary

    def getUpdatedJob(self, maxWait):
        i = self.getFromQueueSafely(self.updatedJobsQueue, maxWait)
        if i == None:
            return None
        jobID, retcode = i
        self.updatedJobsQueue.task_done()
        print str(self.current_jobs[0])
        return i

    def getWaitDuration(self):
        return 0

    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def getJobs(self):
        return self.queue_dictionary


class MesosFrameWorkThread(Thread):
    def __init__(self, queue_dictionary, master_ip, kill_queue, running_dictionary, updated_job_queue):
        Thread.__init__(self)
        self.kill_queue = kill_queue
        self.running_dictionary = running_dictionary
        self.updated_job_queue = updated_job_queue
        self.queue_dictionary = queue_dictionary
        self.master_ip = master_ip

    def start_framework(self):

        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "default"
        executor.command.value = os.path.abspath("/Users/CJ/git/jobTree/jobTree/batchSystems/mesos/mesosExecutor.py")
        executor.name = "Test Executor (Python)"
        executor.source = "python_test"

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "JobTree Framework (Python)"

        # TODO(vinod): Make checkpointing the default when it is default
        # on the slave.
        if os.getenv("MESOS_CHECKPOINT"):
            print "Enabling checkpoint for the framework"
            framework.checkpoint = True

        implicitAcknowledgements = 1
        if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
            print "Enabling explicit status update acknowledgements"
            implicitAcknowledgements = 0

        if os.getenv("MESOS_AUTHENTICATE"):
            print "Enabling authentication for the framework"

            if not os.getenv("DEFAULT_PRINCIPAL"):
                print "Expecting authentication principal in the environment"
                sys.exit(1)

            if not os.getenv("DEFAULT_SECRET"):
                print "Expecting authentication secret in the environment"
                sys.exit(1)

            credential = mesos_pb2.Credential()
            credential.principal = os.getenv("DEFAULT_PRINCIPAL")
            credential.secret = os.getenv("DEFAULT_SECRET")

            framework.principal = os.getenv("DEFAULT_PRINCIPAL")

            driver = MesosSchedulerDriver(
                MesosScheduler(implicitAcknowledgements=implicitAcknowledgements, executor=executor, job_queues=self.queue_dictionary,
                               kill_queue=self.kill_queue,
                               running_dictionary=self.running_dictionary, updated_job_queue=self.updated_job_queue),
                framework,
                self.master_ip,
                implicitAcknowledgements,
                credential)
        else:
            framework.principal = "test-framework-python"

            driver = MesosSchedulerDriver(
                MesosScheduler(implicitAcknowledgements=implicitAcknowledgements, executor=executor, job_queues=self.queue_dictionary,
                               kill_queue=self.kill_queue,
                               running_dictionary=self.running_dictionary, updated_job_queue=self.updated_job_queue),
                framework,
                self.master_ip,
                implicitAcknowledgements)

        driver_result = driver.run()
        status = 0 if driver_result == mesos_pb2.DRIVER_STOPPED else 1

        # Ensure that the driver process terminates.
        driver.stop()
        sys.exit(status)

    def run(self):
        self.start_framework()