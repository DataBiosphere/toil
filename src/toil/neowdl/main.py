"""Toil's WDL Support."""
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

# For an overview of how this all works, see discussion in
# docs/architecture.rst
import argparse
import copy
import datetime
import functools
import json
import logging
import os
import stat
import sys
import tempfile
import textwrap
import urllib
import uuid
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Text,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib import parse as urlparse

from WDL.CLI import runner

import cwltool.builder
import cwltool.command_line_tool
import cwltool.errors
import cwltool.expression
import cwltool.load_tool
import cwltool.main
import cwltool.provenance
import cwltool.resolver
import cwltool.stdfsaccess
import schema_salad.ref_resolver
from cwltool.loghandler import _logger as cwllogger
from cwltool.loghandler import defaultStreamHandler
from cwltool.mutation import MutationManager
from cwltool.pathmapper import MapperEnt, PathMapper, downloadHttpFile
from cwltool.process import (
    Process,
    add_sizes,
    compute_checksums,
    fill_in_defaults,
    shortname,
)
from cwltool.secrets import SecretStore
from cwltool.software_requirements import (
    DependenciesConfiguration,
    get_container_from_software_requirements,
)
from cwltool.utils import (
    CWLObjectType,
    adjustDirObjs,
    adjustFileObjs,
    aslist,
    convert_pathsep_to_unix,
    get_listing,
    normalizeFilesDirs,
    visit_class,
)
from ruamel.yaml.comments import CommentedMap
from schema_salad import validate
from schema_salad.schema import Names
from schema_salad.sourceline import SourceLine

from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM
from toil.common import Config, Toil, addOptions
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.version import baseVersion

logger = logging.getLogger(__name__)
#
#
# class UnresolvedDict(dict):
#     """Tag to indicate a dict contains promises that must be resolved."""
#
#
# def resolve_dict_w_promises(dict_w_promises: dict, file_store: AbstractFileStore = None) -> dict:
#     """
#     for i in output:
#         do_eval(i)
#     return evaluated outputs
#
#     add as Job?
#     """
#
#
# def stage_files(files, file_store: AbstractFileStore) -> None:
#     """Copy input files out of the global file store and update location and path."""
#
#
# class WDLJobWrapper(Job):
#     """
#     Wrap a CWL job that uses dynamic resources requirement.
#
#     When executed, this creates a new child job which has the correct resource
#     requirement set.
#     """
#     def __init__(self, inputs):
#         """Store our context for later evaluation."""
#         self.inputs = inputs
#         super(WDLJobWrapper, self).__init__(cores=1, memory=1024 * 1024, disk=8 * 1024)
#
#     def run(self, file_store: AbstractFileStore) -> Any:
#         """Create a child job with the correct resource requirements set."""
#         args_that_miniwdl_needs = resolve_dict_w_promises(self.inputs, file_store)
#         realjob = WDLJob(args_that_miniwdl_needs)
#         self.addChild(realjob)
#         return realjob.rv()
#
#
# class WDLJob(Job):
#     """Execute a CWL tool using cwltool.executors.SingleJobExecutor."""
#     def __init__(self, args_that_miniwdl_needs):
#         """Things needed for miniwdl."""
#         self.args_that_miniwdl_needs = args_that_miniwdl_needs
#
#     def run(self, file_store: AbstractFileStore) -> Any:
#         logger.debug('Running WDL job.')
#         output, status = cwltool.executors.SingleJobExecutor().execute(
#             process=self.cwltool,
#             job_order_object=cwljob,
#             runtime_context=runtime_context,
#             logger=cwllogger,
#         )
#         ended_at = datetime.datetime.now()  # noqa F841
#         if status != "success":
#             raise cwltool.errors.WorkflowException(status)
#
#         adjustDirObjs(
#             output,
#             functools.partial(
#                 get_listing, cwltool.stdfsaccess.StdFsAccess(outdir), recursive=True
#             ),
#         )
#
#         adjustDirObjs(output, prepareDirectoryForUpload)
#
#         # write the outputs into the jobstore
#         adjustFileObjs(
#             output,
#             functools.partial(
#                 uploadFile,
#                 functools.partial(writeGlobalFileWrapper, file_store),
#                 index,
#                 existing,
#             ),
#         )
#
#         # metadata[process_uuid] = {
#         #     'started_at': started_at,
#         #     'ended_at': ended_at,
#         #     'job_order': cwljob,
#         #     'outputs': output,
#         #     'internal_name': self.jobName
#         # }
#         return output
#
#
# def makeJob(
#     tool: Process,
#     jobobj: dict,
#     runtime_context: cwltool.context.RuntimeContext,
#     conditional: Union[Conditional, None],
# ) -> tuple:
#     """
#     Create the correct Toil Job object for the CWL tool.
#
#     Types: workflow, job, or job wrapper for dynamic resource requirements.
#
#     :return: "wfjob, followOn" if the input tool is a workflow, and "job, job" otherwise
#     """
#     if tool.tool["class"] == "Workflow":
#         wfjob = CWLWorkflow(
#             cast(cwltool.workflow.Workflow, tool),
#             jobobj,
#             runtime_context,
#             conditional=conditional,
#         )
#         followOn = ResolveIndirect(wfjob.rv())
#         wfjob.addFollowOn(followOn)
#         return wfjob, followOn
#     else:
#         resourceReq, _ = tool.get_requirement("ResourceRequirement")
#         if resourceReq:
#             for req in (
#                 "coresMin",
#                 "coresMax",
#                 "ramMin",
#                 "ramMax",
#                 "tmpdirMin",
#                 "tmpdirMax",
#                 "outdirMin",
#                 "outdirMax",
#             ):
#                 r = resourceReq.get(req)
#                 if isinstance(r, str) and ("$(" in r or "${" in r):
#                     # Found a dynamic resource requirement so use a job wrapper
#                     job = CWLJobWrapper(
#                         cast(ToilCommandLineTool, tool),
#                         jobobj,
#                         runtime_context,
#                         conditional=conditional,
#                     )
#                     return job, job
#         job = CWLJob(tool, jobobj, runtime_context, conditional=conditional)  # type: ignore
#         return job, job
#
#
# class CWLScatter(Job):
#     """
#     Implement workflow scatter step.
#
#     When run, this creates a child job for each parameterization of the scatter.
#     """
#
#     def __init__(
#         self,
#         step: cwltool.workflow.WorkflowStep,
#         cwljob: dict,
#         runtime_context: cwltool.context.RuntimeContext,
#         conditional: Union[Conditional, None],
#     ):
#         """Store our context for later execution."""
#         super(CWLScatter, self).__init__(cores=1, memory=100*1024^2, disk=0)
#         self.step = step
#         self.cwljob = cwljob
#         self.runtime_context = runtime_context
#         self.conditional = conditional
#
#     def flat_crossproduct_scatter(
#         self, joborder: dict, scatter_keys: list, outputs: list, postScatterEval: Any
#     ) -> None:
#         """Cartesian product of the inputs, then flattened."""
#         scatter_key = shortname(scatter_keys[0])
#         for n in range(0, len(joborder[scatter_key])):
#             updated_joborder = copy.copy(joborder)
#             updated_joborder[scatter_key] = joborder[scatter_key][n]
#             if len(scatter_keys) == 1:
#                 updated_joborder = postScatterEval(updated_joborder)
#                 subjob, followOn = makeJob(
#                     tool=self.step.embedded_tool,
#                     jobobj=updated_joborder,
#                     runtime_context=self.runtime_context,
#                     conditional=self.conditional,
#                 )
#                 self.addChild(subjob)
#                 outputs.append(followOn.rv())
#             else:
#                 self.flat_crossproduct_scatter(
#                     updated_joborder, scatter_keys[1:], outputs, postScatterEval
#                 )
#
#     def nested_crossproduct_scatter(
#         self, joborder: dict, scatter_keys: list, postScatterEval: Any
#     ) -> list:
#         """Cartesian product of the inputs."""
#         scatter_key = shortname(scatter_keys[0])
#         outputs = []
#         for n in range(0, len(joborder[scatter_key])):
#             updated_joborder = copy.copy(joborder)
#             updated_joborder[scatter_key] = joborder[scatter_key][n]
#             if len(scatter_keys) == 1:
#                 updated_joborder = postScatterEval(updated_joborder)
#                 subjob, followOn = makeJob(
#                     tool=self.step.embedded_tool,
#                     jobobj=updated_joborder,
#                     runtime_context=self.runtime_context,
#                     conditional=self.conditional,
#                 )
#                 self.addChild(subjob)
#                 outputs.append(followOn.rv())
#             else:
#                 outputs.append(
#                     self.nested_crossproduct_scatter(
#                         updated_joborder, scatter_keys[1:], postScatterEval
#                     )
#                 )
#         return outputs
#
#     def run(self, file_store: AbstractFileStore) -> list:
#         """Generate the follow on scatter jobs."""
#         cwljob = resolve_dict_w_promises(self.cwljob, file_store)
#
#         if isinstance(self.step.tool["scatter"], str):
#             scatter = [self.step.tool["scatter"]]
#         else:
#             scatter = self.step.tool["scatter"]
#
#         scatterMethod = self.step.tool.get("scatterMethod", None)
#         if len(scatter) == 1:
#             scatterMethod = "dotproduct"
#         outputs = []
#
#         valueFrom = {
#             shortname(i["id"]): i["valueFrom"]
#             for i in self.step.tool["inputs"]
#             if "valueFrom" in i
#         }
#
#         def postScatterEval(job_dict: dict) -> Any:
#             shortio = {shortname(k): v for k, v in job_dict.items()}
#             for k in valueFrom:
#                 job_dict.setdefault(k, None)
#
#             def valueFromFunc(k: str, v: Any) -> Any:
#                 if k in valueFrom:
#                     return cwltool.expression.do_eval(
#                         valueFrom[k],
#                         shortio,
#                         self.step.requirements,
#                         None,
#                         None,
#                         {},
#                         context=v,
#                     )
#                 else:
#                     return v
#
#             return {k: valueFromFunc(k, v) for k, v in list(job_dict.items())}
#
#         if scatterMethod == "dotproduct":
#             for i in range(0, len(cwljob[shortname(scatter[0])])):
#                 copyjob = copy.copy(cwljob)
#                 for sc in [shortname(x) for x in scatter]:
#                     copyjob[sc] = cwljob[sc][i]
#                 copyjob = postScatterEval(copyjob)
#                 subjob, follow_on = makeJob(
#                     tool=self.step.embedded_tool,
#                     jobobj=copyjob,
#                     runtime_context=self.runtime_context,
#                     conditional=self.conditional,
#                 )
#                 self.addChild(subjob)
#                 outputs.append(follow_on.rv())
#         elif scatterMethod == "nested_crossproduct":
#             outputs = self.nested_crossproduct_scatter(cwljob, scatter, postScatterEval)
#         elif scatterMethod == "flat_crossproduct":
#             self.flat_crossproduct_scatter(cwljob, scatter, outputs, postScatterEval)
#         else:
#             if scatterMethod:
#                 raise validate.ValidationException(
#                     "Unsupported complex scatter type '%s'" % scatterMethod
#                 )
#             else:
#                 raise validate.ValidationException(
#                     "Must provide scatterMethod to scatter over multiple" " inputs."
#                 )
#
#         return outputs
#
#
# class CWLGather(Job):
#     """
#     Follows on to a scatter Job.
#
#     This gathers the outputs of each job in the scatter into an array for each
#     output parameter.
#     """
#
#     def __init__(
#         self,
#         step: cwltool.workflow.WorkflowStep,
#         outputs: Union[Mapping, MutableSequence],
#     ):
#         """Collect our context for later gathering."""
#         super(CWLGather, self).__init__(cores=1, memory=10*1024^2, disk=0)
#         self.step = step
#         self.outputs = outputs
#
#     @staticmethod
#     def extract(obj: Union[Mapping, MutableSequence], k: str) -> list:
#         """
#         Extract the given key from the obj.
#
#         If the object is a list, extract it from all members of the list.
#         """
#         if isinstance(obj, Mapping):
#             return obj.get(k)
#         elif isinstance(obj, MutableSequence):
#             cp = []
#             for item in obj:
#                 cp.append(CWLGather.extract(item, k))
#             return cp
#         else:
#             return []
#
#     def run(self, file_store: AbstractFileStore) -> Dict[str, Any]:
#         """Gather all the outputs of the scatter."""
#         outobj = {}
#
#         def sn(n):
#             if isinstance(n, Mapping):
#                 return shortname(n["id"])
#             if isinstance(n, str):
#                 return shortname(n)
#
#         for k in [sn(i) for i in self.step.tool["out"]]:
#             outobj[k] = self.extract(self.outputs, k)
#
#         return outobj
#
#
class WDLWorkflowJob(Job):
    """
    This is the Toil Root Job which will construct the graph of jobs as dependencies of itself.

    As it runs, it will run jobs that are ready, gather the completed Promise outputs of those
    jobs, resolve them, and them pass them as inputs to dependent jobs as they are ready.
    """
    def __init__(self, options):
        super(WDLWorkflowJob, self).__init__(cores=1, memory=100*1024^2, disk=0)
        self.options = options
        self.inputs = self.gather_inputs(options)

    def parse_input_file(self):
        return []

    def gather_inputs(self):
        # import files into filestore here
        inputs = self.options.inputs
        if self.options.input_file:
            inputs += self.parse_input_file()
        return inputs

    def run(self, file_store: AbstractFileStore):
        """Convert a WDL Workflow graph into a Toil job graph."""
        # `promises` dict
        # from: each parameter (workflow input or step output)
        #   that may be used as a "source" for a step input workflow output
        #   parameter
        # to: the job that will produce that value.
        promises: Dict[str, Job] = {}

        # `jobs` dict from step id to job that implements that step.
        jobs = {}

        for inp in self.inputs:
            promises[inp] = None

        for out in self.outputs:
            promises[out] = None

        all_outputs_fulfilled = False
        while not all_outputs_fulfilled:
            # Iteratively go over the workflow steps, scheduling jobs as their
            # dependencies can be fulfilled by upstream workflow inputs or
            # step outputs. Loop exits when the workflow outputs
            # are satisfied.

            all_outputs_fulfilled = True

            for step in graph:
                if step.tool["id"] not in jobs:
                    stepinputs_fufilled = True
                    for inp in step.tool["inputs"]:
                        for s in aslist(inp.get("source", [])):
                            if s not in promises:
                                stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        jobobj = {}

                        for inp in step.tool["inputs"]:
                            key = shortname(inp["id"])
                            if "source" in inp:
                                jobobj[key] = ResolveSource(
                                    name=f'{step.tool["id"]}/{key}',
                                    input=inp,
                                    source_key="source",
                                    promises=promises,
                                )

                            if "default" in inp:
                                jobobj[key] = DefaultWithSource(  # type: ignore
                                    copy.copy(inp["default"]), jobobj.get(key)
                                )

                            if "valueFrom" in inp and "scatter" not in step.tool:
                                jobobj[key] = StepValueFrom(  # type: ignore
                                    inp["valueFrom"],
                                    jobobj.get(key, JustAValue(None)),
                                    self.cwlwf.requirements,
                                )

                        conditional = Conditional(
                            expression=step.tool.get("when"),
                            outputs=step.tool["out"],
                            requirements=self.cwlwf.requirements,
                        )

                        if "scatter" in step.tool:
                            wfjob = CWLScatter(
                                step,
                                UnresolvedDict(jobobj),
                                self.runtime_context,
                                conditional=conditional,
                            )
                            followOn = CWLGather(step, wfjob.rv())
                            wfjob.addFollowOn(followOn)
                        else:
                            wfjob, followOn = makeJob(
                                tool=step.embedded_tool,
                                jobobj=UnresolvedDict(jobobj),
                                runtime_context=self.runtime_context,
                                conditional=conditional,
                            )

                        jobs[step.tool["id"]] = followOn

                        connected = False
                        for inp in step.tool["inputs"]:
                            for s in aslist(inp.get("source", [])):
                                if (
                                    isinstance(promises[s], (CWLJobWrapper, CWLGather))
                                    and not promises[s].hasFollowOn(wfjob)
                                    # promises[s] job has already added wfjob as a followOn prior
                                    and not wfjob.hasPredecessor(promises[s])
                                ):
                                    promises[s].addFollowOn(wfjob)
                                    connected = True
                                if not isinstance(
                                    promises[s], (CWLJobWrapper, CWLGather)
                                ) and not promises[s].hasChild(wfjob):
                                    promises[s].addChild(wfjob)
                                    connected = True
                        if not connected:
                            # Workflow step is default inputs only & isn't connected
                            # to other jobs, so add it as child of this workflow.
                            self.addChild(wfjob)

                        for out in step.tool["outputs"]:
                            promises[out["id"]] = followOn

                for inp in step.tool["inputs"]:
                    for source in aslist(inp.get("source", [])):
                        if source not in promises:
                            all_outputs_fulfilled = False

            # may need a test
            for out in self.cwlwf.tool["outputs"]:
                if "source" in out:
                    if out["source"] not in promises:
                        all_outputs_fulfilled = False

        outobj = {}
        for out in self.cwlwf.tool["outputs"]:
            key = shortname(out["id"])
            outobj[key] = ResolveSource(
                name="Workflow output '%s'" % key,
                input=out,
                source_key="outputSource",
                promises=promises,
            )

        return UnresolvedDict(outobj)


from toil.neowdl.options import add_run_options

class WDLJob:
    def __init__(self):
        pass

    def run(self):
        pass

def make_wdl_job(options):
    pass

def resolve_promises(outobj):
    pass

def move_outputs_to_outdir(outobj):
    pass

def main(args: Union[List[str]] = None, stdout: TextIO = sys.stdout):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser('Neo-Toil-miniWDL-Runner')
    parser = add_run_options(parser)
    parser.add_argument("--jobStore", "--jobstore", dest="jobStore", type=str)
    parser.add_argument("--outdir", type=str, default=os.getcwd())
    parser.add_argument("--version", action="version", version=baseVersion)

    options = parser.parse_args(args)

    with Toil(options) as toil:
        if options.restart:
            outobj = toil.restart()
        else:
            wdl_job = make_wdl_job(options)
            outobj = toil.start(wdl_job)

        outobj = resolve_promises(outobj)
        outobj = move_outputs_to_outdir(outobj)
        stdout.write(json.dumps(outobj, indent=4))
    return 0
