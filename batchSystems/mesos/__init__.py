from collections import defaultdict, namedtuple
import os
import sys
import time
import pickle
from mesos.interface.mesos_pb2 import TaskID
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import mesosExecutor, badExecutor
from Queue import Queue
from threading import Thread
from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver
import mesos
import logging


log = logging.getLogger( __name__ )

class TaskData(namedtuple("TaskData", ["startTime", "slaveID", "executorID"])):
    pass

class ResourceRequirement(namedtuple("ResourceRequirement", ["memory", "cpu"])):
    pass

class JobTreeJob:
    # describes basic job tree job, with various resource requirements.
    def __init__(self, jobID, cpu, memory, command, cwd):
        self.resources = ResourceRequirement(memory=memory, cpu=cpu)
        self.jobID = jobID
        self.command = command
        self.cwd = cwd


class MesosBatchSystem(AbstractBatchSystem, mesos.interface.Scheduler, Thread):
    """
    Class describes the mesos scheduler framework which acts as the mesos batch system for jobtree
    First methods are jobtree callbacks, then framework methods in rough chronological order of call.
    """
    def __init__(self, config, maxCpus, maxMemory, badExecutor=False):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)
        Thread.__init__(self)

        # written to when mesos kills tasks, as directed by jobtree
        self.killedSet = set()

        # dictionary of queues, which jobTree assigns jobs to. Each queue represents a job type,
        # defined by resource usage
        # FIXME: Are dictionaries thread safe?
        self.jobQueueList = defaultdict(list)

        # ip of mesos master. specified in MesosBatchSystem, currently loopback
        self.masterIP = "127.0.0.1:5050"

        # queue of jobs to kill, by jobID.
        self.killSet = set()

        # Dict of launched jobIDs to TaskData named tuple. Contains start time, executorID, and slaveID.
        self.runningJobMap = {}

        # Queue of jobs whose status has been updated, according to mesos. Req'd by jobTree
        self.updatedJobsQueue = Queue()

        # checks environment variables to determine wether to use implicit/explicit Acknowledgments
        self.implicitAcknowledgements = self.getImplicit()

        # reference to our schedulerDriver, to be instantiated in run()
        self.driver = None

        # returns mesos executor object, which is merged into mesos tasks as they are built
        if badExecutor:
            self.executor = self.buildExecutor(bad=True)
        else:
            self.executor = self.buildExecutor(bad=False)

        self.nextJobID = 0
        self.lastReconciliation = time.time()
        self.reconciliationPeriod = 120

        # These refer to the driver thread. We set it to deamon so it doesn't block
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
        self.jobQueueList[job_type].append(job)
        log.debug("... queued")

        return jobID

    def killJobs(self, jobIDs):
        """Kills the given job IDs.
        """
        localSet = set()
        if self.driver is None:
            raise RuntimeError("There is no scheduler driver")
        for jobID in jobIDs:
            log.debug("passing tasks to kill to mesos driver")
            self.killSet.add(jobID)
            localSet.add(jobID)

            if jobID not in self.getIssuedJobIDs():
                self.killSet.remove(jobID)
                localSet.remove(jobID)
                log.debug("Job %s already finished", jobID)
            else:
                taskId = TaskID()
                taskId.value = str(jobID)
                self.driver.killTask(taskId)

        while localSet:
            log.debug("in while loop")
            intersection=localSet.intersection(self.killedSet)
            localSet-=intersection
            self.killedSet-=intersection
            if not intersection:
                log.debug("sleeping in the while")
                time.sleep(1)

    def getIssuedJobIDs(self):
        """A list of jobs (as jobIDs) currently issued (may be running, or maybe
        just waiting).
        """
        # TODO: Ensure jobList holds jobs that have been "launched" from mesos
        jobList = []
        for k, queue in self.jobQueueList.iteritems():
            for item in queue:
                jobList.append(item.jobID)
        for k,v in self.runningJobMap.iteritems():
            jobList.append(k)

        return jobList

    def getRunningJobIDs(self):
        """Gets a map of jobs (as jobIDs) currently running (not just waiting)
        and a how long they have been running for (in seconds).
        """
        currentTime= dict()
        for jobID,data in self.runningJobMap.iteritems():
            currentTime[jobID] = time.time()-data.startTime
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

    def _sortJobsByResourceReq(self):
        job_types = list(self.jobQueueList.keys())
        # sorts from largest to smallest cpu usage
        # TODO: add a size() method to ResourceSummary and use it as the key. Ask me why.
        job_types.sort(key=lambda resourceRequirement: ResourceRequirement.cpu)
        job_types.reverse()
        return job_types

    def _declineAllOffers(self, driver, offers):
        for offer in offers:
            log.warning("No jobs to assign. Rejecting offer".format(offer.id.value))
            driver.declineOffer(offer.id)

    def _determineOfferResources(self, offer):
        offerCpus = 0
        offerMem = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                offerCpus += resource.scalar.value
            elif resource.name == "mem":
                offerMem += resource.scalar.value
        return offerCpus, offerMem

    def _prepareToRun(self, job_type, offer, index):
        jt_job = self.jobQueueList[job_type][index]  # get the first element to insure FIFO
        task = self._createTask(jt_job, offer)
        return task

    def _deleteByJobID(self, jobID,):
        for key, jobType in self.jobQueueList.iteritems():
            for job in jobType:
                if jobID == job.jobID:
                    jobType.remove(job)

    def _updateStateToRunning(self, offer, task):
        self.runningJobMap[int(task.task_id.value)] = TaskData(startTime=time.time(), slaveID=offer.slave_id,
                                                               executorID=task.executor.executor_id)
        # remove the element from the list FIXME: not efficient, I'm sure.
        self._deleteByJobID(int(task.task_id.value))

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework.
        """
        job_types = self._sortJobsByResourceReq()

        if len(job_types)==0 or (len(self.getIssuedJobIDs()) - len(self.getRunningJobIDs()) == 0):
            log.debug("Declining offers")
            # If there are no jobs, we can get stuck with no jobs and no new offers until we decline it.
            self._declineAllOffers(driver, offers)
            return

        # right now, gives priority to largest jobs
        for offer in offers:
            tasks = []
            # TODO: In an offer, can there ever be more than one resource with the same name?
            offerCpus, offerMem = self._determineOfferResources(offer)
            log.debug( "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem))
            remainingCpus = offerCpus
            remainingMem = offerMem

            for job_type in job_types:
                nextToLaunchIndex=0
                # Because we are not removing from the list until outside of the while loop, we must decrement the
                # number of jobs left to run ourselves to avoid infinite loop.
                while (len(self.jobQueueList[job_type])-nextToLaunchIndex > 0) and \
                                remainingCpus >= job_type.cpu and \
                                remainingMem >= self.__bytesToMB(job_type.memory):  # job tree specifies mem in bytes.

                    task = self._prepareToRun(job_type, offer, nextToLaunchIndex)
                    if int(task.task_id.value) not in self.runningJobMap:
                        # check to make sure task isn't already running (possibly in very unlikely edge case)
                        tasks.append(task)
                        log.debug( "Preparing to launch mesos task %s using offer %s..." % (task.task_id.value, offer.id.value))
                        remainingCpus -= job_type.cpu
                        remainingMem -= self.__bytesToMB(job_type.memory)
                    nextToLaunchIndex+=1

            # If we put the launch call inside the while loop, multiple accepts are used on the same offer.
            driver.launchTasks(offer.id, tasks)

            for task in tasks:
                self._updateStateToRunning(offer, task)
                log.debug( "...launching mesos task %s" % task.task_id.value)

            if len(tasks) == 0:
                log.critical("Offer not large enough to run any tasks")

    def _createTask(self, jt_job, offer):
        """
        build the mesos task object from the jobTree job here to avoid
        further cluttering resourceOffers
        """
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

        try:
            self.killSet.remove(intID)
        except KeyError:
            pass
        else:
            self.killedSet.add(intID)

        if update.state == mesos_pb2.TASK_FINISHED:
            self.__updateState(intID, 0)

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_FAILED or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_ERROR:
            log.warning( "Task %s is in unexpected state %s with message '%s'" \
                % (stringID, mesos_pb2.TaskState.Name(update.state), update.message))
            self.__updateState(intID, 1)

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when an executor sends a message.
        """
        log.info("Executor {} on slave {} sent a message: {}".format(executorId, slaveId, message))

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