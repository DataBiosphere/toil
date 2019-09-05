#!/usr/bin/env python3

"""
run.py: run a command via Kubernetes
"""

import getpass
import kubernetes
import uuid
import time
from pprint import pprint
from kubernetes.client.rest import ApiException


class kubernetesBatchSystem():    
   
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

    
    def issueBatchJob(self, jobNode):
        self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)

    def shutdown():
        api = kubernetes.client.BatchV1Api()
        print("HERE")
        got_list = api.list_job_for_all_namespaces(pretty=True).items
        
        for job in got_list:
            print(job.metadata.clustername)
            try:
                resp = api.delete_namespaced_job(job.metadata.name, job.metadata.namespace)
                pprint(resp)
            except ApiException as e:
                print("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
        
    def get_job(self, jobIDs):
         pass
         
    def killBatchjobs(self, jobIDs):
        pass
if __name__ == "__main__":
