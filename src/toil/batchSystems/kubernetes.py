#!/usr/bin/env python3

"""
run.py: run a command via Kubernetes
"""

from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str

import base64
import getpass
import kubernetes
import logging
import pickle
import subprocess
import uuid
import time
import logging
from kubernetes.client.rest import ApiException
from six.moves.queue import Empty, Queue

from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   BatchSystemLocalSupport)

log = logging.getLogger(__name__)


class KubernetesBatchSystem(AbstractBatchSystem):

    @classmethod
    def supportsAutoDeployment(cls):
        return True

    @classmethod
    def supportsWorkerCleanup(cls):
        return False
   
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(kubernetesBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

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

        # Here is where we will store the user script resource object if we get one.
        self.userScript = None

        # TODO: set this to TOIL_APPLIANCE_SELF, somehow, even though we aren't technically autoscaling.
        self.dockerImage = 'quay.io/uscs_cgl/toil:latest'
           
        self.executors = {}

        self.killJobIds = set()
       
        self.killedJobIds = set()

        self.intendedKill = set()
        
        self.jobQueues = Queue()
        
        # Required Api needed from kubernetes
        self.batchApi = kubernetes.client.BatchV1Api()

        self.deleteoptions = kubernetes.client.DeleteOptions()

        self.podApi = kubernetes.client.CoreV1Api()

    def setUserScript(self, userScript):
        self.userScript = userScript
    
    def issueBatchJob(self, jobNode):
        self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)

        # Make a job dict to send to the executor.
        # First just wrap the command and the environment to run it in
        job = {'command': jobNode.command,
               'environment': self.environment.copy()}

        if self.userScript is not None:
            # If there's a user script resource be sure to send it along
            job['userScript'] = self.userScript

        # Encode it in a form we can send in a command-line argument
        encodedJob = base64.encode(pickle.dumps(job))

        # TODO: remove some of these arguments and integrate the _runCommand method better into the object
        # TODO: Propagate the job's requirements here
        # TODO: Do something useful with the returned job like remembering we launched it
        self._runCommand(['_toil_kubernetes_executor', encodedJob], self.dockerImage, self.namespace, basename=self.jobPrefix)


    def _runCommand(command, image, namespace, cores=1.0, mem_bytes=1 * 1024 * 1024 * 1024, disk_bytes=10 * 1024 * 1024 * 1024, basename="job-"):
        """
        Run the given command (a list of strings) in the given Docker image
        specifier. Run the job under the given Kubernetes namespace name.
        
        Uses the given float number of CPU cores and the given number of bytes of
        memory and (ephemeral) disk to run the job.

        Returns the Kubernetes V1Job, which has a .metadata.name field that can be
        used to get its status.
        """

        # The Kubernetes API makes sense only in terms of the YAML format. Objects
        # represent sections of the YAML files. Except from our point of view, all
        # the internal nodes in the YAML structure are named and typed.

        # For docs, start at the root of the job hierarchy:
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md

        # Make a definition for the container's resource requirements
        requirements_dict = {'cpu': cores, 'memory': mem_bytes, 'ephemeral-storage': disk_bytes}
        resources = kubernetes.client.V1ResourceRequirements(limits=requirements_dict, requests=requirements_dict)
        # Make a volume to provision disk
        volume_name = 'tmp'
        volume_source = kubernetes.client.V1EmptyDirVolumeSource()
        volume = kubernetes.client.V1Volume(name=volume_name, empty_dir=volume_source)
        # Make a mount for the volume
        volume_mount = kubernetes.client.V1VolumeMount(mount_path='/tmp', name=volume_name)
        # Make a container definition
        container = kubernetes.client.V1Container(command=command, image=image, name="runner-container", resources=resources, volume_mounts=[volume_mount])
        # Wrap the container in a spec
        pod_spec = kubernetes.client.V1PodSpec(containers=[container], volumes=[volume], restart_policy="Never")
        # Wrap the spec in a template
        template = kubernetes.client.V1PodTemplateSpec(spec=pod_spec) # Make another spec for the job, asking to run the template with backoff job_spec = kubernetes.client.V1JobSpec(template=template, backoff_limit=1) # Make metadata to tag the job with info.  # We use generate_name to ensure a unique name metadata = kubernetes.client.V1ObjectMeta(generate_name=basename) # And make the actual job job = kubernetes.client.V1Job(spec=job_spec, metadata=metadata, api_version="batch/v1", kind="Job") # Get a versioned Kubernetes batch API api = kubernetes.client.BatchV1Api() 
        # Make the job
        launched = self.batchApi.create_namespaced_job(namespace, job)

        log.debug('Launched job: %s', str(launched))

        return launched

    def shutdown(self):
        # Clears batches of any namespaced jobs
        try:
            jobs = self.batchApi.list_namespaced_job(self.namespace,pretty=True,timeout_seconds=60)
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
        for job in jobs.items:
            logging.debug(job)
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            if job.status.succeeded ==1:
                try:
                    response = self.batchApi.delete_namespaced_job(jobname, 
                                                        self.namespace, 
                                                        deleteoptions, 
                                                        timeout_seconds=60,
                                                        propagation_policy='Background')
                    logging.debug(response)
                except ApiException as e:
                    print("Exception when calling BatchV1Api->delte_namespaced_job: %s\n" % e)
         
        # Clear worker pods 
        try:
            pods = self.podApi.list_namespaced_pod(self.namespace,
                                                    include_uninitialized=False,
                                                    pretty=True,
                                                    timeout_seconds=60)
        except ApiException as e:
            logging.error("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)

        for pod in pods.items:
            logging.debug("Pod {}".format(pod.metadata.name))
            podname = pod.metadata.name
            podstatus = pod.status.phase
            try:
                if podstatus == "succeeded":
                    response = self.podApi.delete_namespaced_pod(podname,
                                                             self.namespace,
                                                             self.deleteoptions)
                    logging.debug("Pod {} deleted".format(podname))
            except ApiException as e:
                logging.error("Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e)


    def getIssuedBatchJobIDs(self):
        try:
            got_list = self.batchApi.list_job_for_all_namespaces(pretty=True).items
        except ApiException:
            print("Exception when calling BatchV1Api->list_job_for_all_namespaces %s\n" % e)
            
        for job in got_list:
            if not job.metadata.name.startswith(self.jobPrefix):
                continue
            else:
                jobname = job.status.name
                jobstatus = job.status.conditions
                logging.debug("{jobname} Status: {jobstatus}")
            
    def killBatchjobs(self, jobIDs):
        # needed api to shutdown cluster
        
        # Clears batches of any namespaced jobs
        try:
            jobs = self.batchApi_batch.list_namespaced_job(self.namespace,pretty=True,timeout_seconds=60)
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
        for job in jobs.items:
            logging.debug(job)
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            if job.status.succeeded ==1:
                try:
                    response = self.batchApi.delete_namespaced_job(jobname, 
                                                        self.namespace, 
                                                        deleteoptions, 
                                                        timeout_seconds=60,
                                                        propagation_policy='Background')
                    logging.debug(response)
                except ApiException as e:
                    print("Exception when calling BatchV1Api->delte_namespaced_job: %s\n" % e)

def executor():
    """
    Main function of the _toil_kubernetes_executor entrypoint.

    Runs inside the Toil container.

    Responsible for setting up the user script and running the command for the job (which may in turn invoke the Toil worker entrypoint.

    """

    logging.basicConfig(level=logging.DEBUG)
    log.debug("Starting executor")

    if len(sys.argv) != 2:
        log.error('Executor requires exactly one base64-encoded argument')
        sys.exit(1)

    # Take in a base64-encoded pickled dict as our first argument and decode it
    try:
        job = pickle.loads(base64.decode(sys.argv[1]))
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


