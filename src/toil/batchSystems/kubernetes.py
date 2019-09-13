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

from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   BatchSystemLocalSupport)

log = logging.getLogger(__name__)


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

        # TODO: set this to TOIL_APPLIANCE_SELF, somehow, even though we aren't technically autoscaling.
        self.dockerImage = 'quay.io/uscs_cgl/toil:latest'
           
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

            if self.userScript is not None:
                # If there's a user script resource be sure to send it along
                job['userScript'] = self.userScript

            # Encode it in a form we can send in a command-line argument
            encodedJob = base64.b64encode(pickle.dumps(job))

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

            log.debug('Launched job: %s', str(launched))
            
            return jobID
            
            
    def _isJobOurs(self, jobObject):
        """
        Determine if a Kubernetes job belongs to us.
        
        :param kubernetes.client.V1Job jobObject: a Kubernetes job being considered.

        :return: True if the job is our responsibility, and false otherwise.
        :rtype: bool
        """
        
        return jobObject.metadata.name.startswith(self.jobPrefix)
        
        
    
    def _ourJobObjects(self, selector=None, limit=None):
        """
        Yield all Kubernetes V1Job objects that we are responsible for that the
        cluster knows about.
        
        :param str selector: a Kubernetes field selector, like
                   "status.failed!=0,status.active=0", to restrict the search.
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
            results = self.batchApi.list_namespaced_job(self.namespace,
                                                        field_selector=selector,
                                                        _continue = token)
            
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
            results = self.coreApi.list_namespaced_pod(self.namespace,
                                                       label_selector=query,
                                                       _continue = token)
            
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
            
            # There's no way to filter for failed OR succeeded jobs, but we can
            # look for one and then the other.
            jobObject = None
            for j in self._ourJobObjects("status.failed=1", limit=1):
                jobObject = j
            if jobObject is None:
                for j in self._ourJobObjects("status.succeeded=1", limit=1):
                    jobObject = j
                    
            if jobObject is None:
                # TODO: block and wait for the jobs to update, until maxWait is hit
                
                # For now just say we couldn't get anything
                return None
            else:
                # Work out what the job's ID was (whatever came after our name prefix)
                jobID = int(jobObject.metadata.name[len(self.jobPrefix):])
                
                # Grab the pod
                pod = self._getPodForJob(jobObject)
                
                # Get the exit code form the pod
                exitCode = pod.status.container_statuses[0].state.terminated.exit_code
                
                # Compute how long the job ran for (subtract datetimes)
                runtime = (jobObject.status.completion_time - 
                           jobObject.status.start_time).total_seconds()
                
                
                # Delete the job and all dependents (pods)
                self.batchApi.delete_namespaced_job(jobObject.metadata.name,
                                                    self.namespace,
                                                    propagation_policy='Foreground')
                                                    
               
                
                # Return the one finished job we found
                return jobID, exitCode, runtime
            
    def shutdown(self):
        
        # Shutdown local processes first
        self.shutdownLocal()
        
        # Clears batch's job belonging to the owner in the namespace
        try:
            jobs = self.batchApi.list_namespaced_job(self.namespace,pretty=True)
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
        for job in jobs.items:
            if self._jobIsOurs(job): 
                logging.debug(job)
                jobname = job.metadata.name
                jobstatus = job.status.conditions
                if job.status.succeeded ==1:
                    try:
                        response = self.batchApi.delete_namespaced_job(jobname, 
                                                            self.namespace, 
                                                            propagation_policy='Foreground')
                        logging.debug(response)
                    except ApiException as e:
                        print("Exception when calling BatchV1Api->delte_namespaced_job: %s\n" % e)
            else:
                logger.debug("{job.metadata.name} is }ot our job")
                continue


    def getIssuedBatchJobIDs(self):
        try:
            got_list = self.batchApi.list_job_for_all_namespaces(pretty=True).items
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_job_for_all_namespaces %s\n" % e)
            
        for job in got_list:
            if not self._isOurJob(job):
                logger.debug("{job.metadata.name} is not our job")
                continue
            else:
                jobname = job.status.name
                jobstatus = job.status.conditions
                logging.debug("{jobname} Status: {jobstatus}")
                self.jobIds.update(jobname)
        return jobIds
            
    def killBatchjobs(self, jobIDs):
        
        self.killLocalJobs(jobIDs)
        
        # Clears owners job in namespace
        
        try:
            jobs = self.batchApi.list_namespaced_job(self.namespace,pretty=True)
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
        for job in jobs.items:
            if self._isJobOurs(job):
                logging.debug(job)
                jobname = job.metadata.name
                jobstatus = job.status.conditions
                if job.status.succeeded ==1:
                    try:
                        response = self.batchApi.delete_namespaced_job(jobname, 
                                                            self.namespace, 
                                                            propagation_policy='Foreground')
                        logging.debug(response)
                    except ApiException as e:
                        print("Exception when calling BatchV1Api->delte_namespaced_job: %s\n" % e)
            else:
                logger.debug("{job.metadata.name} is not our job")
                continue

def executor():
    """
    Main function of the _toil_kubernetes_executor entrypoint.

    Runs inside the Toil container.

    Responsible for setting up the user script and running the command for the
    job (which may in turn invoke the Toil worker entrypoint).

    """

    logging.basicConfig(level=logging.DEBUG)
    log.debug("Starting executor")

    if len(sys.argv) != 2:
        log.error('Executor requires exactly one base64-encoded argument')
        sys.exit(1)

    # Take in a base64-encoded pickled dict as our first argument and decode it
    try:
        job = pickle.loads(base64.b64decode(sys.argv[1]))
    except:
        exc_info = sys.exc_info()
        log.error('Exception while unpickling task: ', exc_info=exc_info)
        sys.exit(1)

    if 'userScript' in job:
        job['userScript'].register()
    log.debug("Invoking command: '%s'", job['command'])
    # Construct the job's environment
    jobEnv = dict(os.environ, **job['environment'])
    log.debug('Using environment variables: %s', jobEnv.keys())
    
    # Start the child process
    child = subprocess.Popen(job.command,
                             preexec_fn=lambda: os.setpgrp(),
                             shell=True,
                             env=jobEnv)

    # Reporduce child's exit code
    sys.exit(child.wait())


