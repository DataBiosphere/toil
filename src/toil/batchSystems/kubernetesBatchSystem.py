#!/usr/bin/env python3

"""
run.py: run a command via Kubernetes
"""

import kubernetes
import uuid
import time
from pprint import pprint
from kubernetes.client.rest import ApiException


class kubernetesBatchSystem():    
   
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(kubernetesBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

    
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
