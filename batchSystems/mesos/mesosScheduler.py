from collections import namedtuple
import pickle

__author__ = 'CJ'
import mesos.interface
import sys
import os
import Queue
import mesos.native
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2
from jobTree.batchSystems.mesos import JobTreeJob, ResourceSummary


class MesosScheduler(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements, executor, job_queues, kill_queue, running_dictionary, updated_job_queue):
            self.jobQueues = job_queues
            self.killQueue = kill_queue
            self.runningDictionary = running_dictionary
            self.updatedJobQueue = updated_job_queue
            self.taskData = {}

            self.implicitAcknowledgements = implicitAcknowledgements
            self.executor = executor

            self.tasksLaunched = 0
            self.tasksFinished = 0
            self.messagesSent = 0
            self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def reregistered(self, driver, masterInfo):
        print "I am still registered with the master."

    def executorLost(self, driver, executorId, slaveId, status):
        print "executor %s lost".format(executorId)

    def reconcile

    def resourceOffers(self, driver, offers):
        # given resources, assign jobs to utilize them.
        # right now, gives priority to largest jobs
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            job_types = list(self.jobQueues.keys())
            # sorts from largest to smallest cpu usage
            job_types.sort(key=lambda ResourceSummary: ResourceSummary.cpu)
            job_types.reverse()

            for job_type in job_types:
                #not part of task object
                task_cpu = job_type.cpu
                task_memory = job_type.memory/1000000

                # loop through the resource requirements for queues.
                # if the requirement matches the offer, loop through the queue and
                # assign jobTree jobs as tasks until the offer is used up or the queue empties.

                while (not self.jobQueues[job_type].empty()) and \
                                remainingCpus >= task_cpu and \
                                remainingMem >= task_memory:

                    jt_job = self.jobQueues[job_type].get()

                    self.runningDictionary[jt_job.jobID] = 1

                    tid = self.tasksLaunched
                    self.tasksLaunched += 1

                    print "Launching task %d using offer %s" \
                          % (tid, offer.id.value)

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
                    cpus.scalar.value = task_cpu

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = task_memory

                    tasks.append(task)
                    self.taskData[task.task_id.value] = (
                        offer.slave_id, task.executor.executor_id)

                    remainingCpus -= task_cpu
                    remainingMem -= task_memory

                driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        print "Task %s is in a state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            self.updatedJobQueue.put((int(update.task_id.value), 1))
            # message not going through. What exactly does this mean for the slave?

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            self.updatedJobQueue.put((int(update.task_id.value), 0))
            # problem: queues are approximate. Just make this queue.empty()?
            if self.tasksFinished == len(self.runningDictionary):
                print "All tasks done, waiting for final framework message"

            slave_id, executor_id = self.taskData[update.task_id.value]

            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            print "not Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            # driver.abort()
            self.updatedJobQueue.put((int(update.task_id.value), 1))

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            print "seems like slave {} not communicating".format(slaveId)
        print "Received message:", repr(str(message))

        # probably doesnt work. running dictionary can shrink.
        if self.messagesReceived == len(self.runningDictionary):
            if self.messagesReceived != self.messagesSent:
                print "Sent", self.messagesSent,
                print "but received", self.messagesReceived
                sys.exit(1)
            print "All tasks done, and all messages received, waiting for more tasks"
            # driver.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    queue = Queue.Queue()

    job1 = JobTreeJob(jobID=1, memory=1, cpu=1, command="echo 'job1'>>job1.txt")
    job2 = JobTreeJob(jobID=2, memory=1, cpu=1, command="echo 'job2'>>job2.txt")

    queue.put(job1)
    queue.put(job2)

    key = ResourceSummary.ResourceSummary(memory=1, cpu=1)

    dictionary = {key:queue}

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = os.path.abspath("/Users/CJ/git/mesos/src/examples/python/test-executor")
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
            MesosScheduler(implicitAcknowledgements, executor, dictionary),
            framework,
            sys.argv[1],
            implicitAcknowledgements,
            credential)
    else:
        framework.principal = "test-framework-python"

        driver = MesosSchedulerDriver(
            MesosScheduler(implicitAcknowledgements, executor, dictionary),
            framework,
            sys.argv[1],
            implicitAcknowledgements)

    driver_result = driver.run()

    status = 0 if driver_result == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    sys.exit(status)
