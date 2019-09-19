# Copyright (C) 2019 Regents of the University of California
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

"""
Batch system for running Toil workflows on Kubernetes.

Ony useful with network-based job stores, like AWSJobStore.

Within non-priveleged Kubernetes containers, additional Docker containers
cannot yet be launched. That functionality will need to wait for user-mode
Docker
"""

from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str

import base64
import datetime
import getpass
import kubernetes
import logging
import os
import pickle
import subprocess
import sys
import uuid
import time

from kubernetes.client.rest import ApiException
from six.moves.queue import Empty, Queue

from toil import applianceSelf, customDockerInitCmd
from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   BatchSystemLocalSupport)

logger = logging.getLogger(__name__)


class KubernetesBatchSystem(BatchSystemLocalSupport):

    @classmethod
    def supportsAutoDeployment(cls):
        return True

    @classmethod
    def supportsWorkerCleanup(cls):
        return False
   
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(KubernetesBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        # Load ~/.kube/config
        kubernetes.config.load_kube_config()

        # Find all contexts and the active context.
        # The active context gets us our namespace.
        contexts, active_context = kubernetes.config.list_kube_config_contexts()
        if not contexts:
            raise RuntimeError("No Kubernetes contexts available in ~/.kube/config")
            
        # Identify the namespace to work in
        self.namespace = active_context.get('context', {}).get('namespace', 'default')

        # Create a prefix for jobs, starting with our username
        self.jobPrefix = '{}-toil-{}-'.format(getpass.getuser(), uuid.uuid4())
        
        # Instead of letting Kubernetes assign unique job names, we assign our
        # own based on a numerical job ID. This functionality is managed by the
        # BatchSystemLocalSupport.

        # Here is where we will store the user script resource object if we get one.
        self.userScript = None

        # Ge tthe image to deploy from Toil's configuration
        self.dockerImage = applianceSelf()
           
        # Required APIs needed from kubernetes
        self.batchApi = kubernetes.client.BatchV1Api()
        self.coreApi = kubernetes.client.CoreV1Api()
    
        self.jobIds = set()

    def setUserScript(self, userScript):
        self.userScript = userScript
        
    # setEnv is provided by BatchSystemSupport, updates self.environment
    
    def issueBatchJob(self, jobNode):
        # TODO: get a sensible self.maxCores, etc. so we can checkResourceRequest.
        # How do we know if the cluster will autoscale?
        
        # Try the job as local
        localID = self.handleLocalJob(jobNode)
        if localID:
            # It is a local job
            return localID
        else:
            # We actually want to send to the cluster
            
            # Check resource requirements (managed by BatchSystemSupport)
            self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
            
            # Make a batch system scope job ID
            jobID = self.getNextJobID()
            # Make a unique name
            jobName = self.jobPrefix + str(jobID)

            # Make a job dict to send to the executor.
            # First just wrap the command and the environment to run it in
            job = {'command': jobNode.command,
                   'environment': self.environment.copy()}
            # TODO: query customDockerInitCmd to respect TOIL_CUSTOM_DOCKER_INIT_COMMAND

            if self.userScript is not None:
                # If there's a user script resource be sure to send it along
                job['userScript'] = self.userScript

            # Encode it in a form we can send in a command-line argument.
            # But make sure it is text so we can ship it to Kubernetes via JSON.
            encodedJob = base64.b64encode(pickle.dumps(job)).decode('utf-8')

            # The Kubernetes API makes sense only in terms of the YAML format. Objects
            # represent sections of the YAML files. Except from our point of view, all
            # the internal nodes in the YAML structure are named and typed.

            # For docs, start at the root of the job hierarchy:
            # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md

            # Make a definition for the container's resource requirements
            requirements_dict = {'cpu': jobNode.cores,
                                 'memory': jobNode.memory,
                                 'ephemeral-storage': jobNode.disk}
            resources = kubernetes.client.V1ResourceRequirements(limits=requirements_dict,
                                                                 requests=requirements_dict)
            # Make a volume to provision disk
            volume_name = 'tmp'
            volume_source = kubernetes.client.V1EmptyDirVolumeSource()
            volume = kubernetes.client.V1Volume(name=volume_name, empty_dir=volume_source)
            # Make a mount for the volume
            volume_mount = kubernetes.client.V1VolumeMount(mount_path='/tmp', name=volume_name)
            # Make a container definition
            container = kubernetes.client.V1Container(command=['_toil_kubernetes_executor', encodedJob],
                                                      image=self.dockerImage,
                                                      name="runner-container",
                                                      resources=resources,
                                                      volume_mounts=[volume_mount])
            # Wrap the container in a spec
            pod_spec = kubernetes.client.V1PodSpec(containers=[container],
                                                   volumes=[volume],
                                                   restart_policy="Never")
            # Wrap the spec in a template
            template = kubernetes.client.V1PodTemplateSpec(spec=pod_spec)
            # Make another spec for the job, asking to run the template with backoff
            job_spec = kubernetes.client.V1JobSpec(template=template, backoff_limit=1)
            # Make metadata to tag the job with info.
            # We use generate_name to ensure a unique name
            metadata = kubernetes.client.V1ObjectMeta(name=jobName)
            # And make the actual job
            job = kubernetes.client.V1Job(spec=job_spec,
                                          metadata=metadata,
                                          api_version="batch/v1",
                                          kind="Job")
            
            # Make the job
            launched = self.batchApi.create_namespaced_job(self.namespace, job)

            logger.debug('Launched job: %s', str(launched))
            
            return jobID
            
            
    def _isJobOurs(self, jobObject):
        """
        Determine if a Kubernetes job belongs to us.
        
        :param kubernetes.client.V1Job jobObject: a Kubernetes job being considered.

        :return: True if the job is our responsibility, and false otherwise.
        :rtype: bool
        """
        
        return jobObject.metadata.name.startswith(self.jobPrefix)
        
        
    
    def _ourJobObjects(self, onlySucceeded=False, limit=None):
        """
        Yield all Kubernetes V1Job objects that we are responsible for that the
        cluster knows about.

        Doesn't support a free-form selector, because there's only about 3
        things jobs can be selected on: https://stackoverflow.com/a/55808444
        
        :param bool onlySucceeded: restrict results to succeeded jobs.
        :param int limit: max results to yield.
        """
        
        # We need to page through the list from the cluster with a continuation
        # token. These expire after about 5 minutes. If we use an expired one,
        # we get a 410 error and a new token, and we can use the new token to
        # get the rest of the list, but the list will be updated.
        #
        # TODO: How to get the new token isn't clear. See
        # https://github.com/kubernetes-client/python/issues/953. For now we
        # will just throw an error if we don't get to the end of the list in
        # time.
        token = None
        
        # Do our own limiting since we need to apply a filter that the server
        # can't.
        seen = 0
        
        # TODO: We ought to label our jobs by owning Toil workflow so we can
        # look them up instead of filtering down later.
        
        while True:
            # We can't just pass e.g. a None continue token when there isn't
            # one, because the Kubernetes module reads its kwargs dict and
            # cares about presence/absence. So we build a dict to send.
            kwargs = {}
            if onlySucceeded:
                # Check only successful jobs.
                # Note that for selectors it is "successful" while for the
                # actual object field it is "succeeded".
                kwargs['field_selector'] = 'status.successful==1'
            if token is not None:
                kwargs['_continue'] = token
            results = self.batchApi.list_namespaced_job(self.namespace, **kwargs)
            
            for job in results.items:
                if self._isJobOurs(job):
                    # This job belongs to us
                    yield job
                    
                    # Don't go over the limit
                    seen += 1
                    if limit is None or seen == limit:
                        return
                    
            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)

            if token is None:
                # There isn't one. We got everything.
                break
                
    def _getPodForJob(self, jobObject):
        """
        Get the pod that belongs to the given job. The pod knows about things
        like the job's exit code.
        
        :param kubernetes.client.V1Job jobObject: a Kubernetes job to look up
                                       pods for.

        :return: The pod for the job.
        :rtype: kubernetes.client.V1Pod
        """
        
        token = None
        
        # Work out what the return code was (which we need to get from the
        # pods) We get the associated pods by querying on the label selector
        # `job-name=JOBNAME`
        query = 'job-name={}'.format(jobObject.metadata.name)
        
        while True:
            # We can't just pass e.g. a None continue token when there isn't
            # one, because the Kubernetes module reads its kwargs dict and
            # cares about presence/absence. So we build a dict to send.
            kwargs = {'label_selector': query}
            if token is not None:
                kwargs['_continue'] = token
            results = self.coreApi.list_namespaced_pod(self.namespace, **kwargs)
            
            for pod in results.items:
                # Return the first pod we find
                return pod
                    
            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)
            
            if token is None:
                # There isn't one. We got everything.
                break
                
        # If we get here, no pages had any pods.
        raise RuntimeError('Could not find any pods for job {}'.format(jobObject.metadata.name))
   
    def _getIDForOurJob(self, jobObject):
        """
        Get the JobID number that belongs to the given job that we own.

        :param kubernetes.client.V1Job jobObject: a Kubernetes job object that is a job we issued.

        :return: The JobID for the job.
        :rtype: int
        """

        return int(jobObject.metadata.name[len(self.jobPrefix):])
            
            
    def getUpdatedBatchJob(self, maxWait):
        
        # See if a local batch job has updated and is available immediately
        local_tuple = self.getUpdatedLocalJob(0)
        if local_tuple:
            # If so, use it
            return local_tuple
        else:
            # Otherwise, go looking for other jobs
            
            # Everybody else does this with a queue and some other thread that
            # is responsible for populating it.
            # But we can just ask kubernetes now.
            
            # Find a job that is done, failed, or stuck
            jobObject = None
            # Put 'done', 'failed', or 'stuck' here
            chosenFor = ''
            for j in self._ourJobObjects(onlySucceeded=True, limit=1):
                # Look for succeeded jobs because that's the only filter Kubernetes has
                jobObject = j
                chosenFor = 'done'

            if jobObject is None:
                for j in self._ourJobObjects():
                    # If there aren't any succeeded jobs, scan all jobs
                    # See how many times each failed
                    failCount = getattr(j.status, 'failed', 0)
                    if failCount is None:
                        # Make sure it is an int
                        failCount = 0
                    if failCount > 0:
                        # Take the first failed one you find
                        jobObject = j
                        chosenFor = 'failed'
                        break

            if jobObject is None:
                # If no jobs are failed, look for jobs with pods with
                # containers stuck in Waiting with reason ImagePullBackOff
                for j in self._ourJobObjects():
                    pod = self._getPodForJob(j)

                    waitingInfo = getattr(pod.status.container_statuses[0].state, 'waiting')
                    if waitingInfo is not None and waitingInfo.reason == 'ImagePullBackOff':
                        # Assume it will never finish, even if the registry comes back or whatever.
                        # We can get into this state when we send in a non-existent image.
                        # See https://github.com/kubernetes/kubernetes/issues/58384
                        jobObject = j
                        chosenFor = 'stuck'
                        logger.warning('Failing stuck job; did you try to run a non-existent Docker image?'
                                       ' Check TOIL_APPLIANCE_SELF.')
                        break
                    
            if jobObject is None:
                # TODO: block and wait for the jobs to update, until maxWait is hit
                
                # For now just say we couldn't get anything
                return None
            else:
                # Work out what the job's ID was (whatever came after our name prefix)
                jobID = int(jobObject.metadata.name[len(self.jobPrefix):])
                
                # Grab the pod
                pod = self._getPodForJob(jobObject)
                
                if chosenFor == 'done' or chosenFor == 'failed':
                    # The job actually finished or failed

                    # Get the exit code form the pod
                    exitCode = pod.status.container_statuses[0].state.terminated.exit_code
                
                    # Compute how long the job ran for (subtract datetimes)
                    # We need to look at the pod's start time because the job's
                    # start time is just when the job is created.
                    runtime = (jobObject.status.completion_time - 
                               pod.status.start_time).total_seconds()
                else:
                    # The job has gotten stuck

                    assert chosenFor == 'stuck'
                    
                    # Synthesize an exit code and runtime (since the job never
                    # really could start running)
                    exitCode = -1
                    runtime = 0
                
                
                # Delete the job and all dependents (pods)
                self.batchApi.delete_namespaced_job(jobObject.metadata.name,
                                                    self.namespace,
                                                    propagation_policy='Foreground')
                                                    
               
                
                # Return the one finished job we found
                return jobID, exitCode, runtime
            
    def shutdown(self):
        
        # Shutdown local processes first
        self.shutdownLocal()
        
        # Clears jobs belonging to this run
        for job in self._ourJobObjects():
            loggger.debug(job)
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            # Kill jobs whether they succeeded or failed
            try:
                # Delete with background poilicy so we can quickly issue lots of commands
                response = self.batchApi.delete_namespaced_job(jobname, 
                                                               self.namespace, 
                                                               propagation_policy='Background')
                logger.debug(response)
            except ApiException as e:
                print("Exception when calling BatchV1Api->delte_namespaced_job: %s\n" % e)


    def _getIssuedNonLocalBatchJobIDs(self):
        """
        Get the issued batch job IDs that are not for local jobs.
        """
        jobIDs = []
        got_list = self._ourJobObjects()
        for job in got_list:
            # Get the ID for each job
            jobIDs.append(self._getIDForOurJob(job))
        return jobIDs

    def getIssuedBatchJobIDs(self):
        # Make sure to send the local jobs also
        return self._getIssuedNonLocalBatchJobIDs() + list(self.getIssuedLocalJobIDs())

    def getRunningBatchJobIDs(self):
        # We need a dict from jobID (string?) to seconds it has been running
        secondsPerJob = dict()
        for job in self._ourJobObjects():
            # Grab the pod for each job
            pod = self._getPodForJob(job)

            if pod.status.phase == 'Running':
                # The job's pod is running

                # The only time we have handy is when the pod got assigned to a
                # kubelet, which is technically before it started running.
                runtime = (datetime.utcnow() - pod.status.start_time).totalseconds()

                # Save it under the stringified job ID
                secondsPerJob[str(self._getIDForOurJob(job))] = runtime

        # Mix in the local jobs
        secondsPerJob.update(self.getRunningLocalJobIDs())
        return secondsPerJob
            
    def killBatchJobs(self, jobIDs):
        
        # Kill all the ones that are local
        self.killLocalJobs(jobIDs)
        
        # Clears workflow's jobs listed in jobIDs.

        # First get the jobs we even issued non-locally
        issuedOnKubernetes = set(self._getIssuedNonLocalBatchJobIDs())

        for jobID in jobIDs:
            # For each job we are supposed to kill
            if jobID not in issuedOnKubernetes:
                # It never went to Kubernetes (or wasn't there when we just
                # looked), so we can't kill it on Kubernetes.
                continue
            # Work out what the job would be named
            jobName = self.jobPrefix + str(jobID)

            # Delete the requested job in the foreground.
            # TODO: are we supposed to support multiple deletes?
            response = self.batchApi.delete_namespaced_job(jobName, 
                                                           self.namespace, 
                                                           propagation_policy='Foreground')
            logger.debug(response)

def executor():
    """
    Main function of the _toil_kubernetes_executor entrypoint.

    Runs inside the Toil container.

    Responsible for setting up the user script and running the command for the
    job (which may in turn invoke the Toil worker entrypoint).

    """

    logging.basicConfig(level=logging.DEBUG)
    logger.debug("Starting executor")

    if len(sys.argv) != 2:
        log.error('Executor requires exactly one base64-encoded argument')
        sys.exit(1)

    # Take in a base64-encoded pickled dict as our first argument and decode it
    try:
        # Make sure to encode the text arguments to bytes before base 64 decoding
        job = pickle.loads(base64.b64decode(sys.argv[1].encode('utf-8')))
    except:
        exc_info = sys.exc_info()
        logger.error('Exception while unpickling task: ', exc_info=exc_info)
        sys.exit(1)

    if 'userScript' in job:
        job['userScript'].register()
    logger.debug("Invoking command: '%s'", job['command'])
    # Construct the job's environment
    jobEnv = dict(os.environ, **job['environment'])
    logger.debug('Using environment variables: %s', jobEnv.keys())
    
    # Start the child process
    child = subprocess.Popen(job.command,
                             preexec_fn=lambda: os.setpgrp(),
                             shell=True,
                             env=jobEnv)

    # Reporduce child's exit code
    sys.exit(child.wait())


