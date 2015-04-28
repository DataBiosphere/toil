from collections import namedtuple
import pickle
import mesos.interface
import time
import sys
import os
import Queue
import mesos.native
from mesos.native import MesosSchedulerDriver
from threading import Thread
from mesos.interface import mesos_pb2
from jobTree.batchSystems.mesos import JobTreeJob, ResourceRequirement
import jobTree.batchSystems.abstractBatchSystem
from jobTree.batchSystems.mesos import mesosExecutor
import logging

# TODO: document class with a short docstring

# TODO: replace all print statements with logger calls (see also mesosExecutor.py)

# TODO: document each method, especially callbacks

# TODO: rename internal method to start with double underscore (ask me for explanation)
log = logging.getLogger( __name__ )
class MesosScheduler(mesos.interface.Scheduler, Thread):
    """
    Part of mesos framework. Communicates via the MesosSchedulerDriver, which is launched
    in the run() method. Instantiated in MesosBatchSystem.
    """
    def __init__(self, masterIP):
        Thread.__init__(self)

        self.jobQueueDict = {}
        self.masterIP=masterIP
        self.killQueue = Queue.Queue()
        self.jobTimeMap = {}
        self.updatedJobsQueue = Queue.Queue()
        self.taskExecutorMap = {}

        self.implicitAcknowledgements = self.getImplicit()
        self.executor = self.buildExecutor()

        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos master.

        """
        log.debug("Registered with framework ID %s" % frameworkId.value)

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
        return mem/1024/1024

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework.
        """
        job_types = list(self.jobQueueDict.keys())
        # sorts from largest to smallest cpu usage
        # TODO: add a size() method to ResourceSummary and use it as the key. Ask me why.
        job_types.sort(key=lambda resourceRequirement: ResourceRequirement.ResourceRequirement.cpu)
        job_types.reverse()

        # right now, gives priority to largest jobs
        for offer in offers:
            # if len(job_types)==0:
            #     print "reject offer {}".format(offer.id.value)
            #     driver.declineOffer(offer.id)
            #     return
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
                                remainingMem >= self.__bytesToMB(job_type.memory): #job tree specifies its resources in bytes.
                    jt_job = self.jobQueueDict[job_type].get()

                    self.jobTimeMap[jt_job.jobID] = time.time()

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
            driver.launchTasks(offer.id, tasks)
            # If we put the launch call inside the while, multiple accepts are used on the same offer. We dont want that.
            # this explains why it works in the simple hello_world case: there is only one jobType, so offer is accepted once.

    def createTask(self, jt_job, offer):
        """
        build the mesos task object from the jobTree job
        """
        tid = self.tasksLaunched
        self.tasksLaunched += 1
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % tid

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


    def statusUpdate(self, driver, update):
        log.debug( "Task %s is in a state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state)))

        # TODO: I think this is left over from the example and should be removed
        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            self.updatedJobsQueue.put((int(update.task_id.value), 1))
            # message not going through. What exactly does this mean for the slave?

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            self.updatedJobsQueue.put((int(update.task_id.value), 0))
            # problem: queues are approximate. Just make this queue.empty()?
            if self.tasksFinished == len(self.jobTimeMap):
                log.debug( "All tasks done, waiting for final framework message")

            slave_id, executor_id = self.taskExecutorMap[update.task_id.value]

            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            log.warning( "Task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message))
            # driver.abort()
            self.updatedJobsQueue.put((int(update.task_id.value), 1))

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when an executor sends a message.
        """
        self.messagesReceived += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            print "seems like slave {} not communicating".format(slaveId)
        print "Received message:", repr(str(message))

        # probably doesnt work. running dictionary can shrink.
        if self.messagesReceived == len(self.jobTimeMap):
            if self.messagesReceived != self.messagesSent:
                log.error( "ERROR: sent {} but recieved {}".format(self.messagesSent,self.messagesReceived))
                #sys.exit(1)
            log.debug( "All tasks done, and all messages received, waiting for more tasks")

    @staticmethod
    def executorScriptPath():
        path = mesosExecutor.__file__
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    def buildExecutor(self):
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
        return executor

    def getImplicit(self):
        implicitAcknowledgements = 1
        if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
            log.debug("Enabling explicit status update acknowledgements")
            implicitAcknowledgements = 0

        return implicitAcknowledgements

    def start_framework(self):

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

            driver = MesosSchedulerDriver(
                self,
                framework,
                self.masterIP,
                self.implicitAcknowledgements,
                credential)
        else:
            framework.principal = "test-framework-python"

            driver = MesosSchedulerDriver(
                self,
                framework,
                self.masterIP,
                self.implicitAcknowledgements)

        driver_result = driver.run()
        status = 0 if driver_result == mesos_pb2.DRIVER_STOPPED else 1

        # Ensure that the driver process terminates.
        driver.stop()
        sys.exit(status)

    def run(self):
        self.start_framework()