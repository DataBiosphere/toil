import os
import sys
from jobTree.batchSystems.mesos.mesosScheduler import MesosSchedulerDriver, MesosScheduler
from jobTree.batchSystems.mesos.JobTreeJob import JobTreeJob
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import mesosExecutor
from threading import Thread
from Queue import Queue
from sonLib.bioio import logger
from mesos.interface import mesos_pb2


class MesosBatchSystem(AbstractBatchSystem):
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        # job type queue is a dictionary of queues. Jobs are grouped in queues with resource requirements as keys.
        self.queueDictionary = {}
        # TODO: I never see any evidence of this queue being read from
        self.killQueue = Queue()
        # TODO: I never see any evidence of this field ever being used
        self.killedQueue = Queue()
        # TODO: Can we combine this with some
        self.runningDictionary = {}
        self.updatedJobsQueue = Queue()
        self.currentJobs = []
        self.nextJobID = 0
        # TODO: stick with one naming convention as much as possible
        # TODO: I don't like the way all this state is threaded through from here down into the Scheduler instance ...
        # ... Why not just pass a reference to the MesosBatchSystem around? Ask me to explain.
        self.mesosThread = MesosFrameWorkThread(queue_dictionary=self.queueDictionary, master_ip="127.0.0.1:5050",
                                                 kill_queue=self.killQueue, running_dictionary=self.runningDictionary,
                                                 updated_job_queue=self.updatedJobsQueue)
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
        if job_type in self.queueDictionary:
            self.queueDictionary[job_type].put(job)
        else:
            self.queueDictionary[job_type] = Queue()
            self.queueDictionary[job_type].put(job)

        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def killJobs(self, jobIDs):
        # kills jobs described by jobIDs
        for jobID in jobIDs:
            self.killQueue.put(jobID)

    def getIssuedJobIDs(self):
        # returning from our local current jobs, not those passed to mesos
        return list(self.currentJobs)

    def getRunningJobIDs(self):
        return self.runningDictionary

    def getUpdatedJob(self, maxWait):
        i = self.getFromQueueSafely(self.updatedJobsQueue, maxWait)
        if i == None:
            return None
        jobID, retcode = i
        self.updatedJobsQueue.task_done()
        return i

    def getWaitDuration(self):
        return 0

    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def getJobs(self):
        return self.queueDictionary


class MesosFrameWorkThread(Thread):
    def __init__(self, queue_dictionary, master_ip, kill_queue, running_dictionary, updated_job_queue):
        Thread.__init__(self)
        self.killQueue = kill_queue
        self.runningDictionary = running_dictionary
        self.updatedJobQueue = updated_job_queue
        self.queueDictionary = queue_dictionary
        self.masterIP = master_ip

    @staticmethod
    def executorScriptPath():
        path = mesosExecutor.__file__
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    def start_framework(self):

        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "MesosExecutor"
        executor.command.value = self.executorScriptPath()
        executor.name = "Test Executor (Python)"
        executor.source = "python_test"

        cpus = executor.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 0.1

        mem = executor.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = 32

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

            # TODO: let's delete this branch and replace it with raising a NotImplementedError

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
                MesosScheduler(implicitAcknowledgements=implicitAcknowledgements, executor=executor, job_queues=self.queueDictionary,
                               kill_queue=self.killQueue,
                               running_dictionary=self.runningDictionary, updated_job_queue=self.updatedJobQueue),
                framework,
                self.masterIP,
                implicitAcknowledgements,
                credential)
        else:
            framework.principal = "test-framework-python"

            driver = MesosSchedulerDriver(
                MesosScheduler(implicitAcknowledgements=implicitAcknowledgements, executor=executor, job_queues=self.queueDictionary,
                               kill_queue=self.killQueue,
                               running_dictionary=self.runningDictionary, updated_job_queue=self.updatedJobQueue),
                framework,
                self.masterIP,
                implicitAcknowledgements)

        driver_result = driver.run()
        status = 0 if driver_result == mesos_pb2.DRIVER_STOPPED else 1

        # Ensure that the driver process terminates.
        driver.stop()
        sys.exit(status)

    def run(self):
        self.start_framework()