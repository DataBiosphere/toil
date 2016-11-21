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
from Queue import Queue, Empty
from threading import Thread, Event

logger = logging.getLogger( __name__ )

class ServiceManager( object ):
    """
    Manages the scheduling of services.
    """
    def __init__(self, jobStore):
        """
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore 
        """
        self.jobStore = jobStore

        self.jobWrappersWithServicesBeingStarted = set()

        self._terminate = Event() # This is used to terminate the thread associated
        # with the service manager

        self._jobWrappersWithServicesToStart = Queue() # This is the input queue of
        # jobWrappers that have services that need to be started

        self._jobWrappersWithServicesThatHaveStarted = Queue() # This is the output queue
        # of jobWrappers that have services that are already started

        self._serviceJobWrappersToStart = Queue() # This is the queue of services for the
        # batch system to start

        self.serviceJobsIssuedToServiceManager = 0 # The number of jobs the service manager
        # is scheduling

        # Start a thread that starts the services of jobWrappers in the
        # jobsWithServicesToStart input queue and puts the jobWrappers whose services
        # are running on the jobWrappersWithServicesThatHaveStarted output queue
        self._serviceStarter = Thread(target=self._startServices,
                                     args=(self._jobWrappersWithServicesToStart,
                                           self._jobWrappersWithServicesThatHaveStarted,
                                           self._serviceJobWrappersToStart, self._terminate,
                                           self.jobStore))
        
    def start(self):
        """
        Start the service scheduling thread.
        """
        self._serviceStarter.start()

    def scheduleServices(self, jobWrapper):
        """
        Schedule the services of a job asynchronously.
        When the job's services are running the jobWrapper for the job will
        be returned by toil.leader.ServiceManager.getJobWrappersWhoseServicesAreRunning.

        :param toil.jobWrapper.JobWrapper jobWrapper: wrapper of job with services to schedule.
        """
        # Add jobWrapper to set being processed by the service manager
        self.jobWrappersWithServicesBeingStarted.add(jobWrapper)

        # Add number of jobs managed by ServiceManager
        self.serviceJobsIssuedToServiceManager += sum(map(len, jobWrapper.services)) + 1 # The plus one accounts for the root job

        # Asynchronously schedule the services
        self._jobWrappersWithServicesToStart.put(jobWrapper)

    def getJobWrapperWhoseServicesAreRunning(self, maxWait):
        """
        :param float maxWait: Time in seconds to wait to get a jobWrapper before returning
        :return: a jobWrapper added to scheduleServices whose services are running, or None if
        no such job is available.
        :rtype: JobWrapper
        """
        try:
            jobWrapper = self._jobWrappersWithServicesThatHaveStarted.get(timeout=maxWait)
            self.jobWrappersWithServicesBeingStarted.remove(jobWrapper)
            assert self.serviceJobsIssuedToServiceManager >= 0
            self.serviceJobsIssuedToServiceManager -= 1
            return jobWrapper
        except Empty:
            return None

    def getServiceJobsToStart(self, maxWait):
        """
        :param float maxWait: Time in seconds to wait to get a job before returning.
        :return: a tuple of (serviceJobStoreID, memory, cores, disk, ..) representing
        a service job to start.
        :rtype: (str, float, float, float)
        """
        try:
            jobTuple = self._serviceJobWrappersToStart.get(timeout=maxWait)
            assert self.serviceJobsIssuedToServiceManager >= 0
            self.serviceJobsIssuedToServiceManager -= 1
            return jobTuple
        except Empty:
            return None

    def killServices(self, services, error=False):
        """
        :param dict services: Maps service jobStoreIDs to the communication flags for the service
        """
        for serviceJobStoreID in services:
            startJobStoreID, terminateJobStoreID, errorJobStoreID = services[serviceJobStoreID]
            if error:
                self.jobStore.deleteFile(errorJobStoreID)
            self.jobStore.deleteFile(terminateJobStoreID)

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
        logger.info('... finished shutting down the service manager. Took %s seconds', time.time() - startTime)

    @staticmethod
    def _startServices(jobWrappersWithServicesToStart,
                       jobWrappersWithServicesThatHaveStarted,
                       serviceJobsToStart,
                       terminate, jobStore):
        """
        Thread used to schedule services.
        """
        while True:
            try:
                # Get a jobWrapper with services to start, waiting a short period
                jobWrapper = jobWrappersWithServicesToStart.get(timeout=1.0)
            except:
                # Check if the thread should quit
                if terminate.is_set():
                    logger.debug('Received signal to quit starting services.')
                    break
                continue

            if jobWrapper is None: # Nothing was ready, loop again
                continue

            # Start the service jobs in batches, waiting for each batch
            # to become established before starting the next batch
            for serviceJobList in jobWrapper.services:
                for serviceJobStoreID, memory, cores, disk, startJobStoreID, terminateJobStoreID, errorJobStoreID in serviceJobList:
                    logger.debug("Service manager is starting service job: %s, start ID: %s", serviceJobStoreID, startJobStoreID)
                    assert jobStore.fileExists(startJobStoreID)
                    # At this point the terminateJobStoreID and errorJobStoreID could have been deleted!
                    serviceJobsToStart.put((serviceJobStoreID, memory, cores, disk))

                # Wait until all the services of the batch are running
                for serviceTuple in serviceJobList:
                    while jobStore.fileExists(serviceTuple[4]):
                        # Sleep to avoid thrashing
                        time.sleep(1.0)

                        # Check if the thread should quit
                        if terminate.is_set():
                            logger.debug('Received signal to quit starting services.')
                            break

            # Add the jobWrapper to the output queue of jobs whose services have been started
            jobWrappersWithServicesThatHaveStarted.put(jobWrapper)