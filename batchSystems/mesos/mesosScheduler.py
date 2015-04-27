from collections import namedtuple
import pickle
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

    @staticmethod
    def bytesToMB(mem):
        return mem/1000000

    def resourceOffers(self, driver, offers):

        job_types = list(self.jobQueues.keys())
        # sorts from largest to smallest cpu usage
        job_types.sort(key=lambda ResourceSummary: ResourceSummary.cpu)
        job_types.reverse()

        # right now, gives priority to largest jobs
        for offer in offers:
            #prevents race condition bug
            if len(job_types)==0:
                driver.declineOffer(offer.id)
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

            for job_type in job_types:
                print "Memory Req: "+str(job_type.memory)
                print "CPU Req "+str(job_type.cpu)
                print "Unique Job Types: "+str(len(job_types))
                #not part of task object
                while (not self.jobQueues[job_type].empty()) and \
                                remainingCpus >= job_type.cpu and \
                                remainingMem >= self.bytesToMB(job_type.memory): #job tree specifies its resources in bytes.
                    jt_job = self.jobQueues[job_type].get()

                    # maps the id to the time running. Right now all running time is 1
                    self.runningDictionary[jt_job.jobID] = 1

                    task = self.createTask(jt_job, offer)
                    print "Launching mesos task %s using offer %s" \
                          % (task.task_id.value, offer.id.value)

                    tasks.append(task)
                    self.taskData[task.task_id.value] = (
                        offer.slave_id, task.executor.executor_id)

                    remainingCpus -= job_type.cpu
                    remainingMem -= self.bytesToMB(job_type.memory)
            #if we launch offers in for loop, they are invalid...
            driver.launchTasks(offer.id, tasks)

    def createTask(self, jt_job, offer):
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
            print "Task %s is in unexpected state %s with message '%s'" \
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
                print "ERROR: sent", self.messagesSent,
                print "but received", self.messagesReceived
                #sys.exit(1)
            print "All tasks done, and all messages received, waiting for more tasks"