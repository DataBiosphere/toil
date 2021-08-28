# Copyright (C) 2015-2021 Regents of the University of California
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


import logging
import time
from queue import Empty, Queue
from threading import Event, Thread
from typing import Dict, Set, Optional

from toil.job import ServiceJobDescription
from toil.lib.throttle import LocalThrottle, throttle
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.toilState import ToilState

logger = logging.getLogger( __name__ )

class ServiceManager( object ):
    """
    Manages the scheduling of services.
    """
    def __init__(self, jobStore: AbstractJobStore, toilState: ToilState):
        logger.debug("Initializing service manager")
        self.jobStore = jobStore

        self.toilState = toilState

        self.jobDescriptionsWithServicesBeingStarted: Set[ServiceJobDescription] = set()

        self._terminate = Event() # This is used to terminate the thread associated
        # with the service manager

        self._jobDescriptionsWithServicesToStart: Queue[ServiceJobDescription] = Queue() # This is the input queue of
        # JobDescriptions that have services that need to be started

        self._jobDescriptionsWithServicesThatHaveStarted: Queue[ServiceJobDescription] = Queue() # This is the output queue
        # of JobDescriptions that have services that are already started

        self._jobDescriptionsWithServicesThatHaveFailedToStart: Queue[ServiceJobDescription] = Queue() # This is the output queue
        # of JobDescriptions that have services that are unable to start

        self.serviceJobDescriptionsToStart: Queue[ServiceJobDescription] = Queue() # This is the queue of services for the
        # batch system to start

        self.jobsIssuedToServiceManager = 0 # The number of jobs the service manager
        # is scheduling

        # Start a thread that starts the services of JobDescriptions in the
        # _jobDescriptionsWithServicesToStart input queue and puts the
        # JobDescriptions whose services are running on the
        # jobDescriptionsWithServicesThatHaveStarted output queue, and whose
        # services can't start on the
        # _jobDescriptionsWithServicesThatHaveFailedToStart output queue
        self._serviceStarter = Thread(target=self._startServices,
                                      args=(self._jobDescriptionsWithServicesToStart,
                                            self._jobDescriptionsWithServicesThatHaveStarted,
                                            self._jobDescriptionsWithServicesThatHaveFailedToStart,
                                            self.serviceJobDescriptionsToStart, self._terminate,
                                            self.jobStore),
                                      daemon=True)



    def start(self) -> None:
        """
        Start the service scheduling thread.
        """
        self._serviceStarter.start()

    def scheduleServices(self, jobDesc: ServiceJobDescription) -> None:
        """
        Schedule the services of a job asynchronously.
        When the job's services are running the JobDescription for the job will
        be returned by toil.leader.ServiceManager.getJobDescriptionWhoseServicesAreRunning.

        :param toil.job.JobDescription jobDesc: description job with services to schedule.
        """
        # Add job to set being processed by the service manager
        self.jobDescriptionsWithServicesBeingStarted.add(jobDesc)

        # Add number of jobs managed by ServiceManager
        self.jobsIssuedToServiceManager += len(jobDesc.services) + 1 # The plus one accounts for the root job

        # Asynchronously schedule the services
        self._jobDescriptionsWithServicesToStart.put(jobDesc)

    def getJobDescriptionWhoseServicesAreRunning(self, maxWait: float) -> Optional[ServiceJobDescription]:
        """
        :param float maxWait: Time in seconds to wait to get a JobDescription before returning
        :return: a JobDescription added to scheduleServices whose services are running, or None if
        no such job is available.
        :rtype: toil.job.JobDescription
        """
        try:
            jobDesc = self._jobDescriptionsWithServicesThatHaveStarted.get(timeout=maxWait)
            self.jobDescriptionsWithServicesBeingStarted.remove(jobDesc)
            assert self.jobsIssuedToServiceManager >= 0
            self.jobsIssuedToServiceManager -= 1
            return jobDesc
        except Empty:
            return None

    def getJobDescriptionWhoseServicesFailedToStart(self, maxWait: float) -> Optional[ServiceJobDescription]:
        """
        :param float maxWait: Time in seconds to wait to get a JobDescription before returning
        :return: a JobDescription added to scheduleServices whose services failed to start, or None if
        no such job is available.
        :rtype: toil.job.JobDescription
        """
        try:
            jobDesc = self._jobDescriptionsWithServicesThatHaveFailedToStart.get(timeout=maxWait)
            self.jobDescriptionsWithServicesBeingStarted.remove(jobDesc)
            assert self.jobsIssuedToServiceManager >= 0
            self.jobsIssuedToServiceManager -= 1
            return jobDesc
        except Empty:
            return None

    def getServiceJobsToStart(self, maxWait: float) -> ServiceJobDescription:
        """
        :param float maxWait: Time in seconds to wait to get a job before returning.
        :return: a tuple of (serviceJobStoreID, memory, cores, disk, ..) representing
        a service job to start.
        :rtype: toil.job.ServiceJobDescription
        """
        try:
            serviceJob = self.serviceJobDescriptionsToStart.get(timeout=maxWait)
            assert isinstance(serviceJob, ServiceJobDescription)
            assert self.jobsIssuedToServiceManager >= 0
            self.jobsIssuedToServiceManager -= 1
            return serviceJob
        except Empty:
            return None

    def killServices(self, services: Dict[str, ServiceJobDescription], error: bool =False) -> None:
        """
        :param dict services: Maps service jobStoreIDs to the communication flags for the service
        """
        for serviceJobStoreID in services:
            serviceJob = services[serviceJobStoreID]
            if error:
                self.jobStore.deleteFile(serviceJob.errorJobStoreID)
            self.jobStore.deleteFile(serviceJob.terminateJobStoreID)

    def isActive(self, service: ServiceJobDescription) -> bool:
        """
        Returns true if the service job has not been told to terminate.

        :param toil.job.JobDescription service: Service to check on
        :rtype: boolean
        """
        return self.jobStore.fileExists(service.terminateJobStoreID)

    def isRunning(self, service: ServiceJobDescription) -> bool:
        """
        Returns true if the service job has started and is active

        :param toil.job.JobDescription service: Service to check on
        :rtype: boolean
        """
        return (not self.jobStore.fileExists(service.startJobStoreID)) and self.isActive(service)

    def check(self) -> None:
        """
        Check on the service manager thread.
        :raise RuntimeError: If the underlying thread has quit.
        """
        if not self._serviceStarter.is_alive():
            raise RuntimeError("Service manager has quit")

    def shutdown(self) -> None:
        """
        Cleanly terminate worker threads starting and killing services. Will block
        until all services are started and blocked.
        """
        logger.debug('Waiting for service manager thread to finish ...')
        startTime = time.time()
        self._terminate.set()
        self._serviceStarter.join()
        # Kill any services still running to avoid deadlock
        for services in list(self.toilState.servicesIssued.values()):
            self.killServices(services, error=True)
        logger.debug('... finished shutting down the service manager. Took %s seconds', time.time() - startTime)

    @staticmethod
    def _startServices(jobDescriptionsWithServicesToStart: 'Queue[ServiceJobDescription]',
                       jobDescriptionsWithServicesThatHaveStarted: 'Queue[ServiceJobDescription]',
                       jobDescriptionsWithServicesThatHaveFailedToStart: 'Queue[ServiceJobDescription]',
                       serviceJobsToStart: 'Queue[ServiceJobDescription]',
                       terminate: Event,
                       jobStore: AbstractJobStore) -> None:
        """
        Thread used to schedule services.
        """

        # Keep the user informed, but not too informed, as services start up
        logLimiter = LocalThrottle(60)

        # These are all keyed by service JobDescription object, not ID
        # TODO: refactor!
        servicesThatAreStarting = set()
        servicesRemainingToStartForJob = {}
        serviceToParentJobDescription = {}
        jobDescriptionsWithFailedServices = set()
        while True:
            with throttle(1.0):
                if terminate.is_set():
                    logger.debug('Received signal to quit starting services.')
                    break
                try:
                    jobDesc = jobDescriptionsWithServicesToStart.get_nowait()
                    if len(list(jobDesc.serviceHostIDsInBatches())) > 1:
                        # Have to fall back to the old blocking behavior to
                        # ensure entire service "groups" are issued as a whole.
                        blockUntilServiceGroupIsStarted(jobDesc,
                                                        jobDescriptionsWithServicesThatHaveStarted,
                                                        serviceJobsToStart, terminate, jobStore)
                        continue
                    # Found a new job that needs to schedule its services.
                    for onlyBatch in jobDesc.serviceHostIDsInBatches():
                        # There should be just one batch so we can do it here.
                        servicesRemainingToStartForJob[jobDesc] = len(onlyBatch)
                        for serviceJobID in onlyBatch:
                            # Load up the service object.
                            # TODO: cache?
                            serviceJobDesc = jobStore.load(serviceJobID)
                            # Remember the parent job
                            serviceToParentJobDescription[serviceJobDesc] = jobDesc
                            # We should now start to monitor this service to see if
                            # it has started yet.
                            servicesThatAreStarting.add(serviceJobDesc)
                            # Send the service JobDescription off to be started
                            logger.debug('Service manager is starting service job: %s, start ID: %s', serviceJobDesc, serviceJobDesc.startJobStoreID)
                            serviceJobsToStart.put(serviceJobDesc)
                except Empty:
                    # No new jobs that need services scheduled.
                    pass

                pendingServiceCount = len(servicesThatAreStarting)
                if pendingServiceCount > 0 and logLimiter.throttle(False):
                    logger.debug('%d services are starting...', pendingServiceCount)

                for serviceJobDesc in list(servicesThatAreStarting):
                    if not jobStore.fileExists(serviceJobDesc.startJobStoreID):
                        # Service has started (or failed)
                        logger.debug('Service %s has removed %s and is therefore started', serviceJobDesc, serviceJobDesc.startJobStoreID)
                        servicesThatAreStarting.remove(serviceJobDesc)
                        parentJob = serviceToParentJobDescription[serviceJobDesc]
                        servicesRemainingToStartForJob[parentJob] -= 1
                        assert servicesRemainingToStartForJob[parentJob] >= 0
                        del serviceToParentJobDescription[serviceJobDesc]
                        if not jobStore.fileExists(serviceJobDesc.errorJobStoreID):
                            logger.error('Service %s has immediately failed before it could be used', serviceJobDesc)
                            # It probably hasn't fileld in the promise that the job that uses the service needs.
                            jobDescriptionsWithFailedServices.add(parentJob)

                # Find if any JobDescriptions have had *all* their services started.
                jobDescriptionsToRemove = set()
                for jobDesc, remainingServices in servicesRemainingToStartForJob.items():
                    if remainingServices == 0:
                        if jobDesc in jobDescriptionsWithFailedServices:
                            logger.error('Job %s has had all its services try to start, but at least one failed', jobDesc)
                            jobDescriptionsWithServicesThatHaveFailedToStart.put(jobDesc)
                        else:
                            logger.debug('Job %s has all its services started', jobDesc)
                            jobDescriptionsWithServicesThatHaveStarted.put(jobDesc)
                        jobDescriptionsToRemove.add(jobDesc)
                for jobDesc in jobDescriptionsToRemove:
                    del servicesRemainingToStartForJob[jobDesc]

def blockUntilServiceGroupIsStarted(jobDesc: ServiceJobDescription,
                                    jobDescriptionsWithServicesThatHaveStarted: 'Queue[ServiceJobDescription]',
                                    serviceJobsToStart: 'Queue[ServiceJobDescription]',
                                    terminate: Event,
                                    jobStore: AbstractJobStore) -> None:

    # Keep the user informed, but not too informed, as services start up
    logLimiter = LocalThrottle(60)

    # Start the service jobs in batches, waiting for each batch
    # to become established before starting the next batch
    for serviceJobList in jobDesc.serviceHostIDsInBatches():
        # When we load the job descriptions we store them here to go over them again.
        waitOn = []
        for serviceJobID in serviceJobList:
            # Load up the service object.
            # TODO: cache?
            serviceJobDesc = jobStore.load(serviceJobID)
            logger.debug("Service manager is starting service job: %s, start ID: %s", serviceJobDesc, serviceJobDesc.startJobStoreID)
            assert jobStore.fileExists(serviceJobDesc.startJobStoreID)
            # At this point the terminateJobStoreID and errorJobStoreID could have been deleted!
            serviceJobsToStart.put(serviceJobDesc)
            # Save for the waiting loop
            waitOn.append(serviceJobDesc)

        # Wait until all the services of the batch are running
        for serviceJobDesc in waitOn:
            while jobStore.fileExists(serviceJobDesc.startJobStoreID):
                # Sleep to avoid thrashing
                time.sleep(1.0)

                if logLimiter.throttle(False):
                    logger.info('Service %s is starting...', serviceJobDesc)

                # Check if the thread should quit
                if terminate.is_set():
                    return

            # We don't bail out early here.

            # We need to try and fail to start *all* the services, so they
            # *all* come back to the leaser as expected, or the leader will get
            # stuck waiting to hear about a later dependent service failing. So
            # we have to *try* to start all the services, even if the services
            # they depend on failed. They should already have been killed,
            # though, so they should stop immediately when we run them. TODO:
            # this is a bad design!


    # Add the JobDescription to the output queue of jobs whose services have been started
    jobDescriptionsWithServicesThatHaveStarted.put(jobDesc)
