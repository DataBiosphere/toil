# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import logging
import time

from threading import Thread, Event

# Python 3 compatibility imports
from six.moves.queue import Empty, Queue

logger = logging.getLogger( __name__ )

class ServiceManager( object ):
    """
    Manages the scheduling of services.
    """
    def __init__(self, jobStore, toilState):
        logger.debug("Initializing service manager")
        self.jobStore = jobStore
        
        self.toilState = toilState

        self.jobGraphsWithServicesBeingStarted = set()

        self._terminate = Event() # This is used to terminate the thread associated
        # with the service manager

        self._jobGraphsWithServicesToStart = Queue() # This is the input queue of
        # jobGraphs that have services that need to be started

        self._jobGraphsWithServicesThatHaveStarted = Queue() # This is the output queue
        # of jobGraphs that have services that are already started

        self._serviceJobGraphsToStart = Queue() # This is the queue of services for the
        # batch system to start

        self.jobsIssuedToServiceManager = 0 # The number of jobs the service manager
        # is scheduling

        # Start a thread that starts the services of jobGraphs in the
        # jobsWithServicesToStart input queue and puts the jobGraphs whose services
        # are running on the jobGraphssWithServicesThatHaveStarted output queue
        self._serviceStarter = Thread(target=self._startServices,
                                     args=(self._jobGraphsWithServicesToStart,
                                           self._jobGraphsWithServicesThatHaveStarted,
                                           self._serviceJobGraphsToStart, self._terminate,
                                           self.jobStore))
        
    def start(self): 
        """
        Start the service scheduling thread.
        """
        self._serviceStarter.start()

    def scheduleServices(self, jobGraph):
        """
        Schedule the services of a job asynchronously.
        When the job's services are running the jobGraph for the job will
        be returned by toil.leader.ServiceManager.getJobGraphsWhoseServicesAreRunning.

        :param toil.jobGraph.JobGraph jobGraph: wrapper of job with services to schedule.
        """
        # Add jobGraph to set being processed by the service manager
        self.jobGraphsWithServicesBeingStarted.add(jobGraph)

        # Add number of jobs managed by ServiceManager
        self.jobsIssuedToServiceManager += sum(map(len, jobGraph.services)) + 1 # The plus one accounts for the root job

        # Asynchronously schedule the services
        self._jobGraphsWithServicesToStart.put(jobGraph)

    def getJobGraphWhoseServicesAreRunning(self, maxWait):
        """
        :param float maxWait: Time in seconds to wait to get a jobGraph before returning
        :return: a jobGraph added to scheduleServices whose services are running, or None if
        no such job is available.
        :rtype: JobGraph
        """
        try:
            jobGraph = self._jobGraphsWithServicesThatHaveStarted.get(timeout=maxWait)
            self.jobGraphsWithServicesBeingStarted.remove(jobGraph)
            assert self.jobsIssuedToServiceManager >= 0
            self.jobsIssuedToServiceManager -= 1
            return jobGraph
        except Empty:
            return None

    def getServiceJobsToStart(self, maxWait):
        """
        :param float maxWait: Time in seconds to wait to get a job before returning.
        :return: a tuple of (serviceJobStoreID, memory, cores, disk, ..) representing
        a service job to start.
        :rtype: toil.job.ServiceJobNode
        """
        try:
            serviceJob = self._serviceJobGraphsToStart.get(timeout=maxWait)
            assert self.jobsIssuedToServiceManager >= 0
            self.jobsIssuedToServiceManager -= 1
            return serviceJob
        except Empty:
            return None

    def killServices(self, services, error=False):
        """
        :param dict services: Maps service jobStoreIDs to the communication flags for the service
        """
        for serviceJobStoreID in services:
            serviceJob = services[serviceJobStoreID]
            if error:
                self.jobStore.deleteFile(serviceJob.errorJobStoreID)
            self.jobStore.deleteFile(serviceJob.terminateJobStoreID)
            
    def isActive(self, serviceJobNode):
        """
        Returns true if the service job has not been told to terminate.
        :rtype: boolean
        """
        return self.jobStore.fileExists(serviceJobNode.terminateJobStoreID)

    def isRunning(self, serviceJobNode):
        """
        Returns true if the service job has started and is active
        :rtype: boolean
        """
        return (not self.jobStore.fileExists(serviceJobNode.startJobStoreID)) and self.isActive(serviceJobNode)

    def check(self):
        """
        Check on the service manager thread.
        :raise RuntimeError: If the underlying thread has quit.
        """
        if not self._serviceStarter.is_alive():
            raise RuntimeError("Service manager has quit")

    def shutdown(self):
        """
        Cleanly terminate worker threads starting and killing services. Will block
        until all services are started and blocked.
        """
        logger.info('Waiting for service manager thread to finish ...')
        startTime = time.time()
        self._terminate.set()
        self._serviceStarter.join()
        # Kill any services still running to avoid deadlock
        for services in self.toilState.servicesIssued.values():
            self.killServices(services, error=True)
        logger.info('... finished shutting down the service manager. Took %s seconds', time.time() - startTime)

    @staticmethod
    def _startServices(jobGraphsWithServicesToStart,
                       jobGraphsWithServicesThatHaveStarted,
                       serviceJobsToStart,
                       terminate, jobStore):
        """
        Thread used to schedule services.
        """
        while True:
            try:
                # Get a jobGraph with services to start, waiting a short period
                jobGraph = jobGraphsWithServicesToStart.get(timeout=1.0)
            except:
                # Check if the thread should quit
                if terminate.is_set():
                    logger.debug('Received signal to quit starting services.')
                    break
                continue

            if jobGraph is None: # Nothing was ready, loop again
                continue

            # Start the service jobs in batches, waiting for each batch
            # to become established before starting the next batch
            for serviceJobList in jobGraph.services:
                for serviceJob in serviceJobList:
                    logger.debug("Service manager is starting service job: %s, start ID: %s", serviceJob, serviceJob.startJobStoreID)
                    assert jobStore.fileExists(serviceJob.startJobStoreID)
                    # At this point the terminateJobStoreID and errorJobStoreID could have been deleted!
                    serviceJobsToStart.put(serviceJob)

                # Wait until all the services of the batch are running
                for serviceJob in serviceJobList:
                    while jobStore.fileExists(serviceJob.startJobStoreID):
                        # Sleep to avoid thrashing
                        time.sleep(1.0)

                        # Check if the thread should quit
                        if terminate.is_set():
                            logger.debug('Received signal to quit starting services.')
                            break

            # Add the jobGraph to the output queue of jobs whose services have been started
            jobGraphsWithServicesThatHaveStarted.put(jobGraph)
