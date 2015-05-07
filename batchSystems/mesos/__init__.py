from collections import defaultdict
import os
import sys
import time
import pickle
from mesos.interface.mesos_pb2 import TaskID
from jobTree.batchSystems.mesos import ResourceRequirement
from jobTree.batchSystems.mesos.JobTreeJob import JobTreeJob
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import mesosExecutor, badExecutor
from Queue import Queue
from threading import Thread
from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver
import mesos
import logging

log = logging.getLogger( __name__ )

class MesosBatchSystem(AbstractBatchSystem, mesos.interface.Scheduler, Thread):
    """
    Class describes the mesos scheduler framework which acts as the mesos batch system for jobtree
    First methods are jobtree callbacks, then framework methods in rough chronological order of call.
    """
    def __init__(self, config, maxCpus, maxMemory, badExecutor=False):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        Thread.__init__(self)

        # written to when mesos kills tasks, as directed by jobtree
        self.killedQueue = Queue()

        # dictionary of queues, which jobTree assigns jobs to. Each queue represents a job type,
        # defined by resource usage
        # FIXME: Are dictionaries thread safe?
        self.jobQueueDict = defaultdict(Queue)

        # ip of mesos master. specified in MesosBatchSystem, currently loopback
        self.masterIP="127.0.0.1:5050"

        # queue of jobs to kill, by jobID.
        self.killSet = set()

        # Dict of launched jobIDs to time they were started. Req'd by jobTree
        self.runningJobMap = {}

        # Queue of jobs whose status has been updated, according to mesos. Req'd by jobTree
        self.updatedJobsQueue = Queue()

        # Dict of taskID to executorID. Used by mesos to send framework messages
        self.taskExecutorMap = {}

        # checks environment variables to determine wether to use implicit/explicit Acknowledgments
        self.implicitAcknowledgements = self.getImplicit()

        # reference to our schedulerDriver, to be instantiated in run()
        self.driver=None

        # returns mesos executor object, which is merged into mesos tasks as they are built
        if badExecutor:
            self.executor = self.buildExecutor(bad=True)
        else:
            self.executor = self.buildExecutor(bad = False)

        self.nextJobID = 0
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0
        self.lastReconciliation = time.time()
        self.reconciliationPeriod = 120

        self.setDaemon(True)
        self.start()

    def issueJob(self, command, memory, cpu):
        """Issues the following command returning a unique jobID. Command
        is the string to run, memory is an int giving
        the number of bytes the job needs to run in and cpu is the number of cpus needed for
        the job and error-file is the path of the file to place any std-err/std-out in.
        """
        # puts job into job_type_queue to be run by mesos, AND puts jobID in current_job[]
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1

        # TODO: this is convoluted, construct ResourceSummary here and pass to JobTreeJob constructor
        job = JobTreeJob(jobID=jobID, cpu=cpu, memory=memory, command=command, cwd=os.getcwd())
        job_type = job.resources

        log.debug("Queueing the job command: %s with job id: %s ..." % (command, str(jobID)))
        self.jobQueueDict[job_type].put(job)
        log.debug("... done.")

        return jobID

    def killJobs(self, jobIDs):
        """Kills the given job IDs. But when is it called?
        """
        localSet = set()
        if self.driver is None:
            raise RuntimeError("There is no scheduler driver")
        for jobID in jobIDs:
            log.debug("passing tasks to kill to mesos driver")
            self.killSet.add(jobID)
            localSet.add(jobID)
            taskId = TaskID()
            taskId.value = str(jobID)
            self.driver.killTask(taskId)

        while localSet:
            i = self.getFromQueueSafely(self.killedQueue, 3)
            if i is not None:
                self.killSet.remove(i)
                localSet.remove(i)

    def getIssuedJobIDs(self):
        """A list of jobs (as jobIDs) currently issued (may be running, or maybe
        just waiting).
        """
        # TODO: Ensure jobList holds jobs that have been "launched" from mesos
        jobList = []
        for queue in self.jobQueueDict:
            jobList.append(list(queue))

        for k,v in self.runningJobMap.iteritems():
            jobList.append(k)

        return jobList

    def getRunningJobIDs(self):
        """Gets a map of jobs (as jobIDs) currently running (not just waiting)
        and a how long they have been running for (in seconds).
        """
        currentTime= dict()
        for k,v in self.runningJobMap.iteritems():
            currentTime[k]= time.time()-v
        return currentTime

    def getUpdatedJob(self, maxWait):
        """Gets a job that has updated its status,
        according to the job manager. Max wait gives the number of seconds to pause
        waiting for a result. If a result is available returns (jobID, exitValue)
        else it returns None.
        """
        i = self.getFromQueueSafely(self.updatedJobsQueue, maxWait)
        if i == None:
            return None
        jobID, retcode = i
        self.updatedJobsQueue.task_done()
        log.debug("Job updated with code {}".format(retcode))
        return i

    def getWaitDuration(self):
        """Gets the period of time to wait (floating point, in seconds) between checking for
        missing/overlong jobs.
        """
        return self.reconciliationPeriod

    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def buildExecutor(self, bad):
        """
        build executor here to avoid cluttering constructor
        :return:
        """
        executor = mesos_pb2.ExecutorInfo()
        if bad:
            executor.command.value = self.executorScriptPath(executorFile=badExecutor)
            executor.executor_id.value = "badExecutor"
        else:
            executor.command.value = self.executorScriptPath(executorFile=mesosExecutor)
            executor.executor_id.value = "jobTreeExecutor"
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
        return executor

    @staticmethod
    def executorScriptPath(executorFile):
        """
        gets path to executor that will run on slaves. Originally was hardcoded, this
        method is more flexible. Return path to .py files only
        :return:
        """
        path = executorFile.__file__
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    def getImplicit(self):
        """
        determine whether to run with implicit or explicit acknowledgements.
        :return:
        """
        implicitAcknowledgements = 1
        if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
            log.debug("Enabling explicit status update acknowledgements")
            implicitAcknowledgements = 0

        return implicitAcknowledgements

    def run(self):
        """
        Starts mesos framework driver, which handles scheduler-mesos communications.
        :return:
        """
        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "JobTree Framework (Python)"

        # TODO(vinod): Make checkpointing the default when it is default
        # on the slave.
        if os.getenv("MESOS_CHECKPOINT"):
            log.debug( "Enabling checkpoint for the framework")
            framework.checkpoint = True

        if os.getenv("MESOS_AUTHENTICATE"):

            # TODO: let's delete this branch and replace it with raising a NotImplementedError

            log.debug( "Enabling authentication for the framework")

            if not os.getenv("DEFAULT_PRINCIPAL"):
                log.error( "Expecting authentication principal in the environment")
                sys.exit(1)

            if not os.getenv("DEFAULT_SECRET"):
                log.error( "Expecting authentication secret in the environment")
                sys.exit(1)

            credential = mesos_pb2.Credential()
            credential.principal = os.getenv("DEFAULT_PRINCIPAL")
            credential.secret = os.getenv("DEFAULT_SECRET")

            framework.principal = os.getenv("DEFAULT_PRINCIPAL")

            self.driver = MesosSchedulerDriver(
                self,
                framework,
                self.masterIP,
                self.implicitAcknowledgements,
                credential)
        else:
            framework.principal = "test-framework-python"

            self.driver = MesosSchedulerDriver(
                self,
                framework,
                self.masterIP,
                self.implicitAcknowledgements)

        driver_result = self.driver.run()
        status = 0 if driver_result == mesos_pb2.DRIVER_STOPPED else 1

        # Ensure that the driver process terminates.
        self.driver.stop()
        sys.exit(status)

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos master.

        """
        log.debug("Registered with framework ID %s" % frameworkId.value)

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework.
        """
        job_types = list(self.jobQueueDict.keys())
        # sorts from largest to smallest cpu usage
        # TODO: add a size() method to ResourceSummary and use it as the key. Ask me why.
        job_types.sort(key=lambda resourceRequirement: ResourceRequirement.ResourceRequirement.cpu)
        job_types.reverse()

        if len(job_types)==0 or (len(self.getIssuedJobIDs()) - len(self.getRunningJobIDs()) == 0):
            for offer in offers:
                log.warning( "No jobs to assign. Rejecting offer".format(offer.id.value))
                driver.declineOffer(offer.id)
            return

        # right now, gives priority to largest jobs
        for offer in offers:
            tasks = []

            # TODO: In an offer, can there ever be more than one resource with the same name?
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            log.debug( "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem))

            remainingCpus = offerCpus
            remainingMem = offerMem

            for job_type in job_types:
                while (not self.jobQueueDict[job_type].empty()) and \
                                remainingCpus >= job_type.cpu and \
                                remainingMem >= self.__bytesToMB(job_type.memory): # job tree specifies mem in bytes.

                    jt_job = self.jobQueueDict[job_type].get()
                    self.runningJobMap[jt_job.jobID] = time.time()
                    task = self.createTask(jt_job, offer)

                    log.debug( "Launching mesos task %s using offer %s" \
                          % (task.task_id.value, offer.id.value))

                    tasks.append(task)
                    # TODO: You might want to simply place the entire task object into that dictionary
                    # TODO: When are entries removed from that dictionary?
                    self.taskExecutorMap[task.task_id.value] = (
                        offer.slave_id, task.executor.executor_id)

                    remainingCpus -= job_type.cpu
                    remainingMem -= self.__bytesToMB(job_type.memory)

            # If we put the launch call inside the while, multiple accepts are used on the same offer. We dont want that.
            # this explains why it works in the simple hello_world case: there is only one jobType, so offer is accepted once.
            driver.launchTasks(offer.id, tasks)
            if len(tasks) == 0:
                log.critical("Offer not large enough to run any tasks")

    def createTask(self, jt_job, offer):
        """
        build the mesos task object from the jobTree job here to avoid
        further cluttering resourceOffers
        """
        self.tasksLaunched += 1
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(jt_job.jobID)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % jt_job.jobID

        # assigns jobTree command to task
        task.data = pickle.dumps(jt_job)

        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = jt_job.resources.cpu

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = jt_job.resources.memory/1000000
        return task

    def __updateState(self, intID, exitStatus):
        self.updatedJobsQueue.put((intID, exitStatus))
        del self.runningJobMap[intID]

    def statusUpdate(self, driver, update):
        """
        Invoked when the status of a task has changed (e.g., a slave is lost and so the task is lost,
        a task finishes and an executor sends a status update saying so, etc). Note that returning from this
        callback _acknowledges_ receipt of this status update! If for whatever reason the scheduler aborts during this
        callback (or the process exits) another status update will be delivered (note, however, that this is currently
        not true if the slave sending the status update is lost/fails during that time).
        """
        log.debug( "Task %s is in a state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state)))

        intID=int(update.task_id.value) # jobTree keeps jobIds as ints
        stringID=update.task_id.value # mesos keeps jobIds as strings

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            self.__updateState(intID, 0)
            slave_id, executor_id = self.taskExecutorMap[stringID]
            del self.taskExecutorMap[stringID]

            # im not sure what they are using this for. It seems like this is to know when to shutDown.
            # we dont want mesos to shut down, and this dictionary does not shrink.
            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_FAILED:
            log.warning( "Task %s is in unexpected state %s with message '%s'" \
                % (stringID, mesos_pb2.TaskState.Name(update.state), update.message))
            self.__updateState(intID, 1)

        if update.state == mesos_pb2.TASK_KILLED:
            # check if the killJob call will auto update jobTree state- probably will
            self.__updateState(intID, 1)
            if intID in self.killSet:
                self.killedQueue.put(intID)

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when an executor sends a message.
        """
        self.messagesReceived += 1

        if self.messagesReceived == len(self.runningJobMap):
            if self.messagesReceived != self.messagesSent:
                log.error( "ERROR: sent {} but recieved {}".format(self.messagesSent,self.messagesReceived))
                #sys.exit(1)
            log.debug( "All tasks done, and all messages received, waiting for more tasks")

    def __reconcile(self, driver):
        """
        queries the master about a list of running tasks. IF the master has no knowledge of them
        their state is updated to LOST.
        :param driver:
        :return:
        """
        # FIXME: we need additional reconciliation. What about the tasks the master knows about but haven't updated?
        now = time.time()
        if now > self.lastReconciliation+self.reconciliationPeriod:
            self.lastReconciliation=now
            driver.reconcileTasks(list(self.runningJobMap.keys()))

    def reregistered(self, driver, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos master.
        """
        log.debug("Registered with new master")

    def executorLost(self, driver, executorId, slaveId, status):
        """
        Invoked when an executor has exited/terminated.
        """
        log.warning("executor %s lost.".format(executorId))

    @staticmethod
    def __bytesToMB(mem):
        """
        used when converting job tree reqs to mesos reqs
        """
        return mem/1024/1024