#!/usr/bin/env python3
# Copyright (C) 2018-2022 UCSC Computational Genomics Lab
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
import argparse
import logging
import os
import subprocess
import sys

from typing import Union, Dict, List, Optional

import WDL

import toil.common
from toil.common import Config, Toil
from toil.job import Job, JobFunctionWrappingJob

logger = logging.getLogger(__name__)

class WDLWorkflowNodeJob(Job):
    """
    Job that evaluates a WDL workflow node.
    """
    
    def __init__(self, node: WDL.Tree.WorkflowNode) -> None:
        pass
        
    def run(self) -> None:
        pass
        
def to_toil(workflow: WDL.Tree.Workflow) -> Job:
    """
    Map a WDL workflow to Toil jobs, recursively.
    """
    
    # MiniWDL already knows the dependency graph among workflow elements
    wdl_id_to_job: Dict[str, Job] = {}
    
    # TODO: This won't work
    def process_node(node: WDL.Tree.WorkflowNode) -> Job:
        job = WDLWorkflowNodeJob(node)
        for child_node in node.body:
            job.addChild(process_node(child_node))
        wdl_id_to_job[node.workflow_node_id] = job
        for dependency in node.workflow_node_dependencies:
            wdl_id_to_job[dependency].addFollowOn(job)
    
    # We need to:
    # Get all node IDs
    # Until we are out of nodes
    # Find a node with no unresolved dependencies
    # Make a job for it that uses the return values of all jobs it depends on, and follows on from (or is a child of?) them
    # Resolve all dependencies on it
    # So basically we need to partial-order sort the DAG.
    
def main() -> None:
    """
    A Toil workflow to interpret WDL input files.
    """
    
    parser = argparse.ArgumentParser(description='Runs WDL files with toil.')
    toil.common.addOptions(parser)
    
    parser.add_argument("wdl_uri", type=str, help="WDL document URI")
    parser.add_argument("inputs_uri", type=str, help="WDL input JSON URI")
    
    options = parser.parse_args(sys.argv[1:])
    
    with Toil(options) as toil:
        if options.restart:
            toil.restart()
        else:
            # Load the WDL document
            document: WDL.Tree.Document = WDL.load(options.wdl_uri)
            
            if document.workflow is None:
                logger.crical("No workflow in document!")
                sys.exit(1)
            
        
            root_job = JobFunctionWrappingJob(run_wdl, wdl_text, inputs_text)
            toil.start(root_job)
    
    
    
if __name__ == "__main__":
    main()
     
    
    

