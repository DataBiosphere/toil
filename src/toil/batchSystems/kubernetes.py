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
import pytz
import string
import subprocess
import sys
import tempfile
import time
import uuid
import urllib3

from kubernetes.client.rest import ApiException
from six.moves.queue import Empty, Queue

from toil import applianceSelf, customDockerInitCmd
from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   BatchSystemSupport,
                                                   BatchSystemLocalSupport,
                                                   EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   UpdatedBatchJobInfo)
from toil.common import Toil
from toil.lib.humanize import human2bytes
from toil.lib.threading import LastProcessStandingArena
from toil.resource import Resource

from toil.lib.retry import retry

logger = logging.getLogger(__name__)
     
def retryable_kubernetes_errors(e):
    """
    A function that determins whether or not Toil should retry or stop given 
    exceptions thrown by Kubernetes. 
    """
    if isinstance(e, urllib3.exceptions.MaxRetryError) or \
        isinstance(e, ApiException):
        return True
    return False

def retry_kubernetes(retry_while=retryable_kubernetes_errors):
    """
    A wrapper that sends retryable Kubernetes predicates into a context-manager which will allow 
    Kubernetes to keep retrying until a False or an executable method is seen.  
    """
    return retry(predicate=retry_while)

def slow_down(seconds):
    """
    Toil jobs that have completed are not allowed to have taken 0 seconds, but
    Kubernetes timestamps things to the second. It is possible in Kubernetes for
    a pod to have identical start and end timestamps.

    This function takes a possibly 0 job length in seconds an enforces a minimum length to satisfy Toil.

    :param float seconds: Kubernetes timestamp difference

    :return: seconds, or a small positive number if seconds is 0
    :rtype: float
    """

    return max(seconds, sys.float_info.epsilon)

def utc_now():
    """
    Return a datetime in the UTC timezone corresponding to right now.
    """

    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)


class KubernetesBatchSystem(BatchSystemLocalSupport):

    @classmethod
    def supportsAutoDeployment(cls):
        return True

    @classmethod
    def supportsWorkerCleanup(cls):
        return True
   
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(KubernetesBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        # Turn down log level for Kubernetes modules and dependencies.
        # Otherwise if we are at debug log level, we dump every
        # request/response to Kubernetes, including tokens which we shouldn't
        # reveal on CI.
        logging.getLogger('kubernetes').setLevel(logging.ERROR)
        logging.getLogger('requests_oauthlib').setLevel(logging.ERROR)
        
        # This will hold the last time our Kubernetes credentials were refreshed
        self.credential_time = None
        # And this will hold our cache of API objects
        self._apis = {}
        
        # Get our namespace (and our Kubernetes credentials to make sure they exist)
        self.namespace = self._api('namespace')
        
        # Decide if we are going to mount a Kubernetes host path as /tmp in the workers.
        # If we do this and the work dir is the default of the temp dir, caches will be shared.
        self.host_path = config.kubernetesHostPath
        if self.host_path is None and os.environ.get("TOIL_KUBERNETES_HOST_PATH", None) is not None:
            # We can also take it from an environment variable
            self.host_path = os.environ.get("TOIL_KUBERNETES_HOST_PATH")

        # Make a Kubernetes-acceptable version of our username: not too long,
        # and all lowercase letters, numbers, or - or .
        acceptableChars = set(string.ascii_lowercase + string.digits + '-.')
        
        # Use TOIL_KUBERNETES_OWNER if present in env var
        if os.environ.get("TOIL_KUBERNETES_OWNER", None) is not None:
            username = os.environ.get("TOIL_KUBERNETES_OWNER")
        else:    
            username = ''.join([c for c in getpass.getuser().lower() if c in acceptableChars])[:100]
        
        # Create a prefix for jobs, starting with our username
        self.jobPrefix = '{}-toil-{}-'.format(username, uuid.uuid4())
        
        # Instead of letting Kubernetes assign unique job names, we assign our
        # own based on a numerical job ID. This functionality is managed by the
        # BatchSystemLocalSupport.

        # Here is where we will store the user script resource object if we get one.
        self.userScript = None

        # Ge the image to deploy from Toil's configuration
        self.dockerImage = applianceSelf()
        
        # Try and guess what Toil work dir the workers will use.
        # We need to be able to provision (possibly shared) space there.
        self.workerWorkDir = Toil.getToilWorkDir(config.workDir)
        if (config.workDir is None and
            os.getenv('TOIL_WORKDIR') is None and
            self.workerWorkDir == tempfile.gettempdir()):
            
            # We defaulted to the system temp directory. But we think the
            # worker Dockerfiles will make them use /var/lib/toil instead.
            # TODO: Keep this in sync with the Dockerfile.
            self.workerWorkDir = '/var/lib/toil'

        # Get the name of the AWS secret, if any, to mount in containers.
        # TODO: have some way to specify this (env var?)!
        self.awsSecretName = os.environ.get("TOIL_AWS_SECRET_NAME", None)

        # Set this to True to enable the experimental wait-for-job-update code
        self.enableWatching = True

        self.jobIds = set()
    
   
    def _api(self, kind, max_age_seconds = 5 * 60):
        """
        The Kubernetes module isn't clever enough to renew its credentials when
        they are about to expire. See
        https://github.com/kubernetes-client/python/issues/741.
        
        We work around this by making sure that every time we are about to talk
        to Kubernetes, we have fresh credentials. And we do that by reloading
        the config and replacing our Kubernetes API objects before we do any
        Kubernetes things.
        
        TODO: We can still get in trouble if a single watch or listing loop
        goes on longer than our credentials last, though.
        
        This method is the Right Way to get any Kubernetes API. You call it
        with the API you want ('batch', 'core', or 'customObjects') and it
        returns an API object with guaranteed fresh credentials.
        
        It also recognizes 'namespace' and returns our namespace as a string.
        
        max_age_seconds needs to be << your cluster's credential expiry time.
        """
        
        now = utc_now()
        
        if self.credential_time is None or (now - self.credential_time).total_seconds() > max_age_seconds:
            # Credentials need a refresh
            try:
                # Load ~/.kube/config or KUBECONFIG
                kubernetes.config.load_kube_config()
                # Worked. We're using kube config
                config_source = 'kube'
            except TypeError:
                # Didn't work. Try pod-based credentials in case we are in a pod.
                try:
                    kubernetes.config.load_incluster_config()
                    # Worked. We're using in_cluster config
                    config_source = 'in_cluster'
                except kubernetes.config.ConfigException:
                    raise RuntimeError('Could not load Kubernetes configuration from ~/.kube/config, $KUBECONFIG, or current pod.')
                  
        
        # Now fill in the API objects with these credentials
        self._apis['batch'] = kubernetes.client.BatchV1Api()
        self._apis['core'] = kubernetes.client.CoreV1Api()
        self._apis['customObjects'] = kubernetes.client.CustomObjectsApi()
        
        # And save the time
        self.credential_time = now
        
        if kind == 'namespace':
            # We just need the namespace string
            if config_source == 'in_cluster':
                # Our namespace comes from a particular file.
                with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", 'r') as fh:
                    return fh.read().strip()
            else:
                # Find all contexts and the active context.
                # The active context gets us our namespace.
                contexts, activeContext = kubernetes.config.list_kube_config_contexts()
                if not contexts:
                    raise RuntimeError("No Kubernetes contexts available in ~/.kube/config or $KUBECONFIG")
                    
                # Identify the namespace to work in
                return activeContext.get('context', {}).get('namespace', 'default')
                
        else:
            # We need an API object
            try:
                return self._apis[kind]
            except KeyError: 
                raise RuntimeError("Unknown Kubernetes API type: {}".format(kind))
    
    def _try_kubernetes(self, method, *args, **kwargs):
        """
        Kubernetes API can end abruptly and fail when it could dynamically backoff and retry.

        For example, calling self._api('batch').create_namespaced_job(self.namespace, job),
        Kubernetes can behave inconsistently and fail given a large job. See 
        https://github.com/DataBiosphere/toil/issues/2884 .
        
        This function gives Kubernetes more time to try an executable api.  
        """

        for attempt in retry_kubernetes():
            with attempt:
                return method(*args, **kwargs)

    def setUserScript(self, userScript):
        logger.info('Setting user script for deployment: {}'.format(userScript))
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
            
            # Send the worker cleanup info so that the last worker on a
            # host to shut down can clean up if warranted.
            job['workerCleanupInfo'] = self.workerCleanupInfo

            if self.userScript is not None:
                # If there's a user script resource be sure to send it along
                job['userScript'] = self.userScript

            # Encode it in a form we can send in a command-line argument.
            # Pickle in the highest protocol to prevent mixed Python2/3 workflows from trying to work
            # TODO: Make the appliance use/support Python 3
            # Make sure it is text so we can ship it to Kubernetes via JSON.
            encodedJob = base64.b64encode(pickle.dumps(job, pickle.HIGHEST_PROTOCOL)).decode('utf-8')

            # The Kubernetes API makes sense only in terms of the YAML format. Objects
            # represent sections of the YAML files. Except from our point of view, all
            # the internal nodes in the YAML structure are named and typed.

            # For docs, start at the root of the job hierarchy:
            # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md

            # Make a definition for the container's resource requirements.
            # Add on a bit for Kubernetes overhead (Toil worker's memory, hot deployed
            # user scripts).
            # Kubernetes needs some lower limit of memory to run the pod at all without
            # OOMing. We also want to provision some extra space so that when
            # we test _isPodStuckOOM we never get True unless the job has
            # exceeded jobNode.memory.
            requirements_dict = {'cpu': jobNode.cores,
                                 'memory': jobNode.memory + 1024 * 1024 * 512,
                                 'ephemeral-storage': jobNode.disk + 1024 * 1024 * 512}
            # Use the requirements as the limits, for predictable behavior, and because
            # the UCSC Kubernetes admins want it that way.
            limits_dict = requirements_dict
            resources = kubernetes.client.V1ResourceRequirements(limits=limits_dict,
                                                                 requests=requirements_dict)
            
            # Collect volumes and mounts
            volumes = []
            mounts = []
            
            if self.host_path is not None:
                # Provision Toil WorkDir from a HostPath volume, to share with other pods
                host_path_volume_name = 'workdir'
                # Use type='Directory' to fail if the host directory doesn't exist already.
                host_path_volume_source = kubernetes.client.V1HostPathVolumeSource(path=self.host_path, type='Directory')
                host_path_volume = kubernetes.client.V1Volume(name=host_path_volume_name,
                                                             host_path=host_path_volume_source)
                volumes.append(host_path_volume)
                host_path_volume_mount = kubernetes.client.V1VolumeMount(mount_path=self.workerWorkDir, name=host_path_volume_name)
                mounts.append(host_path_volume_mount)
            else:
                # Provision Toil WorkDir as an ephemeral volume
                ephemeral_volume_name = 'workdir'
                ephemeral_volume_source = kubernetes.client.V1EmptyDirVolumeSource()
                ephemeral_volume = kubernetes.client.V1Volume(name=ephemeral_volume_name,
                                                              empty_dir=ephemeral_volume_source)
                volumes.append(ephemeral_volume)
                ephemeral_volume_mount = kubernetes.client.V1VolumeMount(mount_path=self.workerWorkDir, name=ephemeral_volume_name)
                mounts.append(ephemeral_volume_mount)

            if self.awsSecretName is not None:
                # Also mount an AWS secret, if provided.
                # TODO: make this generic somehow
                secret_volume_name = 's3-credentials'
                secret_volume_source = kubernetes.client.V1SecretVolumeSource(secret_name=self.awsSecretName)
                secret_volume = kubernetes.client.V1Volume(name=secret_volume_name,
                                                           secret=secret_volume_source)
                volumes.append(secret_volume)
                secret_volume_mount = kubernetes.client.V1VolumeMount(mount_path='/root/.aws', name=secret_volume_name)
                mounts.append(secret_volume_mount)

            # Make a container definition
            container = kubernetes.client.V1Container(command=['_toil_kubernetes_executor', encodedJob],
                                                      image=self.dockerImage,
                                                      name="runner-container",
                                                      resources=resources,
                                                      volume_mounts=mounts)
            # Wrap the container in a spec
            pod_spec = kubernetes.client.V1PodSpec(containers=[container],
                                                   volumes=volumes,
                                                   restart_policy="Never")
            # Wrap the spec in a template
            template = kubernetes.client.V1PodTemplateSpec(spec=pod_spec)
            # Make another spec for the job, asking to run the template with no backoff
            job_spec = kubernetes.client.V1JobSpec(template=template, backoff_limit=0)
            # Make metadata to tag the job with info.
            # We use generate_name to ensure a unique name
            metadata = kubernetes.client.V1ObjectMeta(name=jobName)
            # And make the actual job
            job = kubernetes.client.V1Job(spec=job_spec,
                                          metadata=metadata,
                                          api_version="batch/v1",
                                          kind="Job")
            
            # Make the job
            launched = self._try_kubernetes(self._api('batch').create_namespaced_job, self.namespace, job)

            logger.debug('Launched job: %s', jobName)
            
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
            
            results = self._try_kubernetes(self._api('batch').list_namespaced_job, self.namespace, **kwargs)
            
            for job in results.items:
                if self._isJobOurs(job):
                    # This job belongs to us
                    yield job
                    
                    # Don't go over the limit
                    seen += 1
                    if limit is not None and seen >= limit:
                        return
                    
            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)

            if token is None:
                # There isn't one. We got everything.
                break
                
    def _getPodForJob(self, jobObject):
        """
        Get the pod that belongs to the given job, or None if the job's pod is
        missing. The pod knows about things like the job's exit code.

        :param kubernetes.client.V1Job jobObject: a Kubernetes job to look up
                                       pods for.

        :return: The pod for the job, or None if no pod is found.
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
            results = self._try_kubernetes(self._api('core').list_namespaced_pod, self.namespace, **kwargs)
        
            for pod in results.items:
                # Return the first pod we find
                return pod
                    
            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)
        
            if token is None:
                # There isn't one. We got everything.
                break
                
        # If we get here, no pages had any pods.
        return None

    def _getLogForPod(self, podObject):
        """
        Get the log for a pod.

        :param kubernetes.client.V1Pod podObject: a Kubernetes pod with one
                                       container to get the log from.

        :return: The log for the only container in the pod.
        :rtype: str

        """

        return self._try_kubernetes(self._api('core').read_namespaced_pod_log, podObject.metadata.name,
                                                         namespace=self.namespace)

    def _isPodStuckOOM(self, podObject, minFreeBytes=1024 * 1024 * 2):
        """
        Poll the current memory usage for the pod from the cluster.

        Return True if the pod looks to be in a soft/stuck out of memory (OOM)
        state, where it is using too much memory to actually make progress, but
        not enough to actually trigger the OOM killer to kill it. For some
        large memory limits, on some Kubernetes clusters, pods can get stuck in
        this state when their memory limits are high (approx. 200 Gi).

        We operationalize "OOM" as having fewer than minFreeBytes bytes free.

        We assume the pod has only one container, as Toil's pods do.

        :param kubernetes.client.V1Pod podObject: a Kubernetes pod with one
                                       container to check up on.
        :param int minFreeBytes: Minimum free bytes to not be OOM.

        :return: True if the pod is OOM, false otherwise.
        :rtype: bool
        """

        # Compose a query to get just the pod we care about
        query = 'metadata.name=' + podObject.metadata.name

        # Look for it
        # TODO: When the Kubernetes Python API actually wraps the metrics API, switch to that
        response = self._try_kubernetes(self._api('customObjects').\
                list_namespaced_custom_object, 
                'metrics.k8s.io', 'v1beta1',
                self.namespace, 'pods',
                field_selector=query)

        # Pull out the items
        items = response.get('items', [])

        if len(items) == 0:
            # If there's no statistics we can't say we're stuck OOM
            return False

        # Assume the first result is the right one, because of the selector
        # Assume it has exactly one pod, because we made it
        containers = items[0].get('containers', [{}])
        
        if len(containers) == 0:
            # If there are no containers (because none have started yet?), we can't say we're stuck OOM
            return False
        
        # Otherwise, assume it just has one container.
        # Grab the memory usage string, like 123Ki, and convert to bytes.
        # If anything is missing, assume 0 bytes used.
        bytesUsed = human2bytes(containers[0].get('usage', {}).get('memory', '0'))

        # Also get the limit out of the pod object's spec
        bytesAllowed = human2bytes(podObject.spec.containers[0].resources.limits['memory'])

        if bytesAllowed - bytesUsed < minFreeBytes:
            # This is too much!
            logger.warning('Pod %s has used %d of %d bytes of memory; reporting as stuck due to OOM.',
                           podObject.metadata.name, bytesUsed, bytesAllowed)

            return True




    def _getIDForOurJob(self, jobObject):
        """
        Get the JobID number that belongs to the given job that we own.

        :param kubernetes.client.V1Job jobObject: a Kubernetes job object that is a job we issued.

        :return: The JobID for the job.
        :rtype: int
        """

        return int(jobObject.metadata.name[len(self.jobPrefix):])
    

    def getUpdatedBatchJob(self, maxWait):

        entry = datetime.datetime.now()

        result = self._getUpdatedBatchJobImmediately()

        if result is not None or maxWait == 0:
            # We got something on the first try, or we only get one try
            return result

        # Otherwise we need to maybe wait.

        if self.enableWatching:
            # Try watching for something to happen and use that.

            w = kubernetes.watch.Watch()    

            if self.enableWatching:
                for j in self._ourJobObjects():
                    for event in w.stream(self._api('core').list_namespaced_pod, self.namespace, timeout_seconds=maxWait):
                        pod = event['object']
                        if pod.metadata.name.startswith(self.jobPrefix):
                            if pod.status.phase == 'Failed' or pod.status.phase == 'Succeeded':
                                containerStatuses =  pod.status.container_statuses
                                logger.debug("FINISHED")
                                if containerStatuses is None or len(containerStatuses) == 0: 
                                    logger.debug("No job container statuses for job %s" % (pod.metadata.owner_references[0].name))
                                    return UpdatedBatchJobInfo(jobID=int(pod.metadata.owner_references[0].name[len(self.jobPrefix):]), exitStatus=EXIT_STATUS_UNAVAILABLE_VALUE, wallTime=0, exitReason=None)

                                # Get termination onformation from the pod
                                termination = pod.status.container_statuses[0].state.terminated
                                logger.info("REASON: %s Exit Code: %s", termination.reason, termination.exit_code)
                                
                                if termination.exit_code != 0:
                                    # The pod failed. Dump information about it.
                                    logger.debug('Failed pod information: %s', str(pod))
                                    logger.warning('Log from failed pod: %s', self._getLogForPod(pod))
                                jobID = int(pod.metadata.owner_references[0].name[len(self.jobPrefix):])
                                terminated = pod.status.container_statuses[0].state.terminated
                                runtime = slow_down((terminated.finished_at - terminated.started_at).total_seconds())
                                result = UpdatedBatchJobInfo(jobID=jobID, exitStatus=terminated.exit_code, wallTime=runtime, exitReason=None)
                                self._try_kubernetes(self._api('batch').delete_namespaced_job, 
                                            pod.metadata.owner_references[0].name,
                                            self.namespace,
                                            propagation_policy='Foreground')

                                self._waitForJobDeath(pod.metadata.owner_references[0].name)
                                return result
                            else:
                                continue
        else:
            # Try polling instead
            while result is None and (datetime.datetime.now() - entry).total_seconds() < maxWait:
                # We still have nothing and we haven't hit the timeout.
                
                # Poll
                result = self._getUpdatedBatchJobImmediately()

                if result is None:
                    # Still nothing. Wait a second, or some fraction of our max wait time.
                    time.sleep(min(maxWait/2, 1.0))

            # When we get here, either we found something or we ran out of time
            return result


    def _getUpdatedBatchJobImmediately(self):
        """
        Return None if no updated (completed or failed) batch job is currently
        available, and jobID, exitCode, runtime if such a job can be found.
        """

        # See if a local batch job has updated and is available immediately
        local_tuple = self.getUpdatedLocalJob(0)
        if local_tuple:
            # If so, use it
            return local_tuple

        # Otherwise we didn't get a local job.

        # Go looking for other jobs
        
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
            # If no jobs are failed, look for jobs with pods that are stuck for various reasons.
            for j in self._ourJobObjects():
                pod = self._getPodForJob(j)

                if pod is None:
                    # Skip jobs with no pod
                    continue
                    
                # Containers can get stuck in Waiting with reason ImagePullBackOff

                # Get the statuses of the pod's containers
                containerStatuses = pod.status.container_statuses
                if containerStatuses is None or len(containerStatuses) == 0:
                    # Pod exists but has no container statuses
                    # This happens when the pod is just "Scheduled"
                    # ("PodScheduled" status event) and isn't actually starting
                    # to run yet.
                    # Can't be stuck in ImagePullBackOff
                    continue

                waitingInfo = getattr(getattr(pod.status.container_statuses[0], 'state', None), 'waiting', None)
                if waitingInfo is not None and waitingInfo.reason == 'ImagePullBackOff':
                    # Assume it will never finish, even if the registry comes back or whatever.
                    # We can get into this state when we send in a non-existent image.
                    # See https://github.com/kubernetes/kubernetes/issues/58384
                    jobObject = j
                    chosenFor = 'stuck'
                    logger.warning('Failing stuck job; did you try to run a non-existent Docker image?'
                                   ' Check TOIL_APPLIANCE_SELF.')
                    break

                # Pods can also get stuck nearly but not quite out of memory,
                # if their memory limits are high and they try to exhaust them.

                if self._isPodStuckOOM(pod):
                    # We found a job that probably should be OOM! Report it as stuck.
                    # Polling function takes care of the logging.
                    jobObject = j
                    chosenFor = 'stuck'
                    break

        if jobObject is None:
            # Say we couldn't find anything
            return None


        # Otherwise we got something.

        # Work out what the job's ID was (whatever came after our name prefix)
        jobID = int(jobObject.metadata.name[len(self.jobPrefix):])

        # Work out when the job was submitted. If the pod fails before actually
        # running, this is the basis for our runtime.
        jobSubmitTime = getattr(jobObject.status, 'start_time', None)
        if jobSubmitTime is None:
            # If somehow this is unset, say it was just now.
            jobSubmitTime = utc_now() 

        # Grab the pod
        pod = self._getPodForJob(jobObject)

        if pod is not None:
            if chosenFor == 'done' or chosenFor == 'failed':
                # The job actually finished or failed

                # Get the statuses of the pod's containers
                containerStatuses = pod.status.container_statuses

                # Get when the pod started (reached the Kubelet) as a datetime
                startTime = getattr(pod.status, 'start_time', None)
                if startTime is None:
                    # If the pod never made it to the kubelet to get a
                    # start_time, say it was when the job was submitted.
                    startTime = jobSubmitTime 
                
                if containerStatuses is None or len(containerStatuses) == 0:
                    # No statuses available.
                    # This happens when a pod is "Scheduled". But how could a
                    # 'done' or 'failed' pod be merely "Scheduled"?
                    # Complain so we can find out.
                    logger.warning('Exit code and runtime unavailable; pod has no container statuses')
                    logger.warning('Pod: %s', str(pod))
                    exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                    # Say it stopped now and started when it was scheduled/submitted.
                    # We still need a strictly positive runtime.
                    runtime = slow_down((utc_now() - startTime).total_seconds())
                else:
                    # Get the termination info from the pod's main (only) container
                    terminatedInfo = getattr(getattr(containerStatuses[0], 'state', None), 'terminated', None)
                    if terminatedInfo is None:
                        logger.warning('Exit code and runtime unavailable; pod stopped without container terminating')
                        logger.warning('Pod: %s', str(pod))
                        exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                        # Say it stopped now and started when it was scheduled/submitted.
                        # We still need a strictly positive runtime.
                        runtime = slow_down((utc_now() - startTime).total_seconds())
                    else:
                        # Extract the exit code
                        exitCode = terminatedInfo.exit_code

                        # Compute how long the job actually ran for (subtract
                        # datetimes). We need to look at the pod's start time
                        # because the job's start time is just when the job is
                        # created. And we need to look at the pod's end time
                        # because the job only gets a completion time if
                        # successful.
                        runtime = slow_down((terminatedInfo.finished_at - 
                                             pod.status.start_time).total_seconds())

                        if chosenFor == 'failed':
                            # Warn the user with the failed pod's log
                            # TODO: cut this down somehow?
                            logger.warning('Log from failed pod: %s', self._getLogForPod(pod))
            
            else:
                # The job has gotten stuck

                assert chosenFor == 'stuck'
                
                # Synthesize an exit code
                exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                # Say it ran from when the job was submitted to when the pod got stuck
                runtime = slow_down((utc_now() - jobSubmitTime).total_seconds())
        else:
            # The pod went away from under the job.
            logging.warning('Exit code and runtime unavailable; pod vanished')
            exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
            # Say it ran from when the job was submitted to when the pod vanished
            runtime = slow_down((utc_now() - jobSubmitTime).total_seconds())
        
        
        try:
            # Delete the job and all dependents (pods)
            self._try_kubernetes(self._api('batch').delete_namespaced_job, jobObject.metadata.name,
                                                     self.namespace,
                                                     propagation_policy='Foreground')
                                                
            # That just kicks off the deletion process. Foreground doesn't
            # actually block. See
            # https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/#foreground-cascading-deletion
            # We have to either wait until the deletion is done and we can't
            # see the job anymore, or ban the job from being "updated" again if
            # we see it. If we don't block on deletion, we can't use limit=1
            # on our query for succeeded jobs. So we poll for the job's
            # non-existence.
            self._waitForJobDeath(jobObject.metadata.name)
                    
        except kubernetes.client.rest.ApiException:
            # TODO: check to see if this is a 404 on the thing we tried to delete
            # If so, it is gone already and we don't need to delete it again.
            pass

        # Return the one finished job we found
        return UpdatedBatchJobInfo(jobID=jobID, exitStatus=exitCode, wallTime=runtime, exitReason=None)

    def _waitForJobDeath(self, jobName):
        """
        Block until the job with the given name no longer exists.
        """

        # We do some exponential backoff on the polling
        # TODO: use a wait instead of polling?
        backoffTime = 0.1
        maxBackoffTime = 6.4
        while True:
            try:
                # Look for the job
                self._try_kubernetes(self._api('batch').read_namespaced_job, jobName, self.namespace)
                # If we didn't 404, wait a bit with exponential backoff
                time.sleep(backoffTime)
                if backoffTime < maxBackoffTime:
                    backoffTime *= 2
            except kubernetes.client.rest.ApiException:
                # We finally got a failure!
                break
            
    def shutdown(self):
        
        # Shutdown local processes first
        self.shutdownLocal()
        
        # Clears jobs belonging to this run
        for job in self._ourJobObjects():
            jobName = job.metadata.name

            try:
                # Look at the pods and log why they failed, if they failed, for debugging.
                pod = self._getPodForJob(job)
                if pod.status.phase == 'Failed':
                    logger.debug('Failed pod encountered at shutdown: %s', str(pod))
            except:
                # Don't get mad if that doesn't work.
                pass

            # Kill jobs whether they succeeded or failed
            try:
                # Delete with background poilicy so we can quickly issue lots of commands
                response = self._try_kubernetes(self._api('batch').delete_namespaced_job, jobName, 
                                                                    self.namespace, 
                                                                    propagation_policy='Background')
                logger.debug('Killed job for shutdown: %s', jobName)
            except ApiException as e:
                logger.error("Exception when calling BatchV1Api->delte_namespaced_job: %s" % e)


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
        # We need a dict from jobID (integer) to seconds it has been running
        secondsPerJob = dict()
        for job in self._ourJobObjects():
            # Grab the pod for each job
            pod = self._getPodForJob(job)

            if pod is None:
                # Jobs whose pods are gone are not running
                continue

            if pod.status.phase == 'Running':
                # The job's pod is running

                # The only time we have handy is when the pod got assigned to a
                # kubelet, which is technically before it started running.
                runtime = (utc_now() - pod.status.start_time).total_seconds()

                # Save it under the stringified job ID
                secondsPerJob[self._getIDForOurJob(job)] = runtime
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
            # This doesn't block, but it does delete expeditiously.
            response = self._try_kubernetes(self._api('batch').delete_namespaced_job, jobName, 
                                                                self.namespace, 
                                                                propagation_policy='Foreground')
            logger.debug('Killed job by request: %s', jobName)

        for jobID in jobIDs:
            # Now we need to wait for all the jobs we killed to be gone.

            # Work out what the job would be named
            jobName = self.jobPrefix + str(jobID)

            # Block until it doesn't exist
            self._waitForJobDeath(jobName)

def executor():
    """
    Main function of the _toil_kubernetes_executor entrypoint.

    Runs inside the Toil container.

    Responsible for setting up the user script and running the command for the
    job (which may in turn invoke the Toil worker entrypoint).

    """

    logging.basicConfig(level=logging.DEBUG)
    logger.debug("Starting executor")
    
    # If we don't manage to run the child, what should our exit code be?
    exit_code = EXIT_STATUS_UNAVAILABLE_VALUE

    if len(sys.argv) != 2:
        logger.error('Executor requires exactly one base64-encoded argument')
        sys.exit(exit_code)

    # Take in a base64-encoded pickled dict as our first argument and decode it
    try:
        # Make sure to encode the text arguments to bytes before base 64 decoding
        job = pickle.loads(base64.b64decode(sys.argv[1].encode('utf-8')))
    except:
        exc_info = sys.exc_info()
        logger.error('Exception while unpickling task: ', exc_info=exc_info)
        sys.exit(exit_code)

    if 'environment' in job:
        # Adopt the job environment into the executor.
        # This lets us use things like TOIL_WORKDIR when figuring out how to talk to other executors.
        logger.debug('Adopting environment: %s', str(job['environment'].keys()))
        for var, value in job['environment'].items():
            os.environ[var] = value
    
    # Set JTRES_ROOT and other global state needed for resource
    # downloading/deployment to work.
    # TODO: Every worker downloads resources independently.
    # We should have a way to share a resource directory.
    logger.debug('Preparing system for resource download')
    Resource.prepareSystem()
    try:
        if 'userScript' in job:
            job['userScript'].register()
            
        # We need to tell other workers in this workflow not to do cleanup now that
        # we are here, or else wait for them to finish. So get the cleanup info
        # that knows where the work dir is.
        cleanupInfo = job['workerCleanupInfo']
        
        # Join a Last Process Standing arena, so we know which process should be
        # responsible for cleanup.
        # We need to use the real workDir, not just the override from cleanupInfo.
        # This needs to happen after the environment is applied.
        arena = LastProcessStandingArena(Toil.getToilWorkDir(cleanupInfo.workDir), 
            cleanupInfo.workflowID + '-kube-executor')
        arena.enter()
        try:
            
            # Start the child process
            logger.debug("Invoking command: '%s'", job['command'])
            child = subprocess.Popen(job['command'],
                                     preexec_fn=lambda: os.setpgrp(),
                                     shell=True)

            # Reproduce child's exit code
            exit_code = child.wait()
            
        finally:
            for _ in arena.leave():
                # We are the last concurrent executor to finish.
                # Do batch system cleanup.
                logger.debug('Cleaning up worker')
                BatchSystemSupport.workerCleanup(cleanupInfo)
    finally:
        logger.debug('Cleaning up resources')
        # TODO: Change resource system to use a shared resource directory for everyone.
        # Then move this into the last-process-standing cleanup
        Resource.cleanSystem()
        logger.debug('Shutting down')
        sys.exit(exit_code)


