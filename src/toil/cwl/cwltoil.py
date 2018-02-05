# Implement support for Common Workflow Language (CWL) for Toil.
#
# Copyright (C) 2015 Curoverse, Inc
# Copyright (C) 2016 UCSC Computational Genomics Lab
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

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
from toil.job import Job
from toil.common import Toil
from toil.version import baseVersion
from toil.lib.bioio import setLoggingFromOptions

import argparse
import cwltool.errors
import cwltool.load_tool
import cwltool.main
import cwltool.workflow
import cwltool.expression
import cwltool.builder
import cwltool.resolver
import cwltool.stdfsaccess
import cwltool.draft2tool

from cwltool.pathmapper import (PathMapper, adjustDirObjs, adjustFileObjs,
                                get_listing, MapperEnt, visit_class,
                                normalizeFilesDirs)
from cwltool.process import (shortname, fillInDefaults, compute_checksums,
                             collectFilesAndDirs)
from cwltool.software_requirements import (
        DependenciesConfiguration, get_container_from_software_requirements)
from cwltool.utils import aslist
import schema_salad.validate as validate
import schema_salad.ref_resolver
import os
import tempfile
import json
import sys
import logging
import copy
import functools
from typing import Text


# Python 3 compatibility imports
from six import iteritems, string_types
import six.moves.urllib.parse as urlparse

cwllogger = logging.getLogger("cwltool")

# Define internal jobs we should avoid submitting to batch systems and logging
CWL_INTERNAL_JOBS = ("CWLJob", "CWLJobWrapper", "CWLWorkflow", "CWLScatter", "CWLGather",
                     "ResolveIndirect")

# The job object passed into CWLJob and CWLWorkflow
# is a dict mapping to tuple of (key, dict)
# the final dict is derived by evaluating each
# tuple looking up the key in the supplied dict.
#
# This is necessary because Toil jobs return a single value (a dict)
# but CWL permits steps to have multiple output parameters that may
# feed into multiple other steps.  This transformation maps the key in the
# output object to the correct key of the input object.


class IndirectDict(dict):
    """Type tag to indicate a dict is an IndirectDict that needs to resolved."""
    pass


class MergeInputs(object):
    """Base type for workflow step inputs that are connected to multiple upstream
    inputs that must be merged into a single array.
    """
    def __init__(self, sources):
        self.sources = sources

    def resolve(self):
        raise NotImplementedError()


class MergeInputsNested(MergeInputs):
    """Merge workflow step inputs that are connected to multiple upstream inputs
    based on the merge_nested behavior (as described in the CWL spec).
    """
    def resolve(self):
        return [v[1][v[0]] for v in self.sources]


class MergeInputsFlattened(MergeInputs):
    """Merge workflow step inputs that are connected to multiple upstream inputs
    based on the merge_flattened behavior (as described in the CWL spec).
    """

    def resolve(self):
        r = []
        for v in self.sources:
            v = v[1][v[0]]
            if isinstance(v, list):
                r.extend(v)
            else:
                r.append(v)
        return r


class StepValueFrom(object):
    """A workflow step input which has a valueFrom expression attached to it, which
    is evaluated to produce the actual input object for the step.
    """

    def __init__(self, expr, inner, req):
        self.expr = expr
        self.inner = inner
        self.req = req

    def do_eval(self, inputs, ctx):
        return cwltool.expression.do_eval(self.expr, inputs, self.req,
                                          None, None, {}, context=ctx)


def _resolve_indirect_inner(d):
    """Resolve the contents an indirect dictionary (containing promises) to produce
    a dictionary actual values, including merging multiple sources into a
    single input.

    """

    if isinstance(d, IndirectDict):
        r = {}
        for k, v in list(d.items()):
            if isinstance(v, MergeInputs):
                r[k] = v.resolve()
            else:
                r[k] = v[1].get(v[0])
        return r
    else:
        return d


def resolve_indirect(d):
    """Resolve the contents an indirect dictionary (containing promises) and
    evaluate expressions to produce the dictionary of actual values.

    """

    inner = IndirectDict() if isinstance(d, IndirectDict) else {}
    needEval = False
    for k, v in iteritems(d):
        if isinstance(v, StepValueFrom):
            inner[k] = v.inner
            needEval = True
        else:
            inner[k] = v
    res = _resolve_indirect_inner(inner)
    if needEval:
        ev = {}
        for k, v in iteritems(d):
            if isinstance(v, StepValueFrom):
                ev[k] = v.do_eval(res, res[k])
            else:
                ev[k] = res[k]
        return ev
    else:
        return res


class ToilPathMapper(PathMapper):
    """ToilPathMapper keeps track of a file's symbolic identifier (the Toil
    FileStore token), its local path on the host (the value returned by
    readGlobalFile) and the the location of the file inside the Docker
    container.

    """

    def __init__(self, referenced_files, basedir, stagedir,
                 separateDirs=True,
                 get_file=None,
                 stage_listing=False):
        self.get_file = get_file
        self.stage_listing = stage_listing
        super(ToilPathMapper, self).__init__(referenced_files, basedir,
                                             stagedir, separateDirs=separateDirs)

    def visit(self, obj, stagedir, basedir, copy=False, staged=False):
        # type: (Dict[Text, Any], Text, Text, bool, bool) -> None
        tgt = os.path.join(stagedir, obj["basename"])
        if obj["location"] in self._pathmap:
            return
        if obj["class"] == "Directory":
            if obj["location"].startswith("file://"):
                resolved = schema_salad.ref_resolver.uri_file_path(obj["location"])
            else:
                resolved = obj["location"]
            self._pathmap[obj["location"]] = MapperEnt(resolved, tgt, "WritableDirectory" if copy else "Directory", staged)
            if obj["location"].startswith("file://") and not self.stage_listing:
                staged = False
            self.visitlisting(obj.get("listing", []), tgt, basedir, copy=copy, staged=staged)
        elif obj["class"] == "File":
            loc = obj["location"]
            if "contents" in obj and obj["location"].startswith("_:"):
                self._pathmap[obj["location"]] = MapperEnt(obj["contents"], tgt, "CreateFile", staged)
            else:
                resolved = self.get_file(loc) if self.get_file else loc
                if resolved.startswith("file:"):
                    resolved = schema_salad.ref_resolver.uri_file_path(resolved)
                self._pathmap[loc] = MapperEnt(resolved, tgt, "WritableFile" if copy else "File", staged)
                self.visitlisting(obj.get("secondaryFiles", []), stagedir, basedir, copy=copy, staged=staged)


class ToilCommandLineTool(cwltool.draft2tool.CommandLineTool):
    """Subclass the cwltool command line tool to provide the custom
    Toil.PathMapper.

    """

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return ToilPathMapper(reffiles, kwargs["basedir"], stagedir,
                              separateDirs=kwargs.get("separateDirs", True),
                              get_file=kwargs["toil_get_file"])


def toilMakeTool(toolpath_object, **kwargs):
    """Factory function passed to load_tool() which creates instances of the
    custom ToilCommandLineTool.

    """

    if isinstance(toolpath_object, dict) and toolpath_object.get("class") == "CommandLineTool":
        return ToilCommandLineTool(toolpath_object, **kwargs)
    return cwltool.workflow.defaultMakeTool(toolpath_object, **kwargs)


class ToilFsAccess(cwltool.stdfsaccess.StdFsAccess):
    """Custom filesystem access class which handles toil filestore references.

    """

    def __init__(self, basedir, fileStore=None):
        self.fileStore = fileStore
        super(ToilFsAccess, self).__init__(basedir)

    def _abs(self, p):
        if p.startswith("toilfs:"):
            return self.fileStore.readGlobalFile(p[7:])
        else:
            return super(ToilFsAccess, self)._abs(p)


def toilGetFile(fileStore, index, existing, fileStoreID):
    """Get path to input file from Toil jobstore."""

    if not fileStoreID.startswith("toilfs:"):
        return schema_salad.ref_resolver.file_uri(fileStoreID)
    srcPath = fileStore.readGlobalFile(fileStoreID[7:])
    index[srcPath] = fileStoreID
    existing[fileStoreID] = srcPath
    return schema_salad.ref_resolver.file_uri(srcPath)


def writeFile(writeFunc, index, existing, x):
    """Write a file into the Toil jobstore.

    'existing' is a set of files retrieved as inputs from toilGetFile. This ensures
    they are mapped back as the same name if passed through.

    """
    # Toil fileStore reference
    if x.startswith("toilfs:"):
        return x
    # File literal outputs with no path, we don't write these and will fail
    # with unsupportedRequirement when retrieving later with getFile
    elif x.startswith("_:"):
        return x
    else:
        x = existing.get(x, x)
        if x not in index:
            if not urlparse.urlparse(x).scheme:
                rp = os.path.realpath(x)
            else:
                rp = x
            try:
                index[x] = "toilfs:" + writeFunc(rp)
                existing[index[x]] = x
            except Exception as e:
                cwllogger.error("Got exception '%s' while copying '%s'", e, x)
                raise
        return index[x]


def uploadFile(uploadfunc, fileindex, existing, uf, skip_broken=False):
    """Update a file object so that the location is a reference to the toil file
    store, writing it to the file store if necessary.

    """

    if uf["location"].startswith("toilfs:") or uf["location"].startswith("_:"):
        return
    if uf["location"] in fileindex:
        uf["location"] = fileindex[uf["location"]]
        return
    if not uf["location"] and uf["path"]:
        uf["location"] = schema_salad.ref_resolver.file_uri(uf["path"])
    if not os.path.isfile(uf["location"][7:]):
        if skip_broken:
            return
        else:
            raise cwltool.errors.WorkflowException("File is missing: %s" % uf["location"])
    uf["location"] = writeFile(uploadfunc,
                               fileindex,
                               existing,
                               uf["location"])


def writeGlobalFileWrapper(fileStore, fileuri):
    """Wrap writeGlobalFile to accepts file:// URIs"""
    return fileStore.writeGlobalFile(schema_salad.ref_resolver.uri_file_path(fileuri))


class ResolveIndirect(Job):
    """A helper Job which accepts an indirect dict (containing promises) and
    produces a dictionary of actual values.

    """

    def __init__(self, cwljob):
        super(ResolveIndirect, self).__init__()
        self.cwljob = cwljob

    def run(self, fileStore):
        return resolve_indirect(self.cwljob)


def toilStageFiles(fileStore, cwljob, outdir, index, existing, export):
    """Copy input files out of the global file store and update location and
    path."""

    jobfiles = []  # type: List[Dict[Text, Any]]
    collectFilesAndDirs(cwljob, jobfiles)
    pm = ToilPathMapper(jobfiles, "", outdir, separateDirs=False, stage_listing=True)
    for f, p in pm.items():
        if not p.staged:
            continue
        if not os.path.exists(os.path.dirname(p.target)):
            os.makedirs(os.path.dirname(p.target), 0o0755)
        if p.type == "File":
            fileStore.exportFile(p.resolved[7:], "file://" + p.target)
        elif p.type == "Directory" and not os.path.exists(p.target):
            os.makedirs(p.target, 0o0755)
        elif p.type == "CreateFile":
            with open(p.target, "wb") as n:
                n.write(p.resolved.encode("utf-8"))

    def _check_adjust(f):
        f["location"] = schema_salad.ref_resolver.file_uri(pm.mapper(f["location"])[1])
        if "contents" in f:
            del f["contents"]
        return f

    visit_class(cwljob, ("File", "Directory"), _check_adjust)


class CWLJobWrapper(Job):
    """Wrap a CWL job that uses dynamic resources requirement.  When executed, this
    creates a new child job which has the correct resource requirement set.

    """

    def __init__(self, tool, cwljob, **kwargs):
        super(CWLJobWrapper, self).__init__(cores=1,
                                            memory=1024*1024,
                                            disk=8*1024)
        self.cwltool = remove_pickle_problems(tool)
        self.cwljob = cwljob
        self.kwargs = kwargs

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)
        options = copy.deepcopy(self.kwargs)
        options['jobobj'] = cwljob
        realjob = CWLJob(self.cwltool, cwljob, **options)
        for child in self._children:
            cwllogger.debug("CWLJobWrapper child: {}".format(child))
            realjob.addFollowOn(child)
        self.addChild(realjob)
        return realjob.rv()


class CWLJob(Job):
    """Execute a CWL tool using cwltool.main.single_job_executor"""

    def __init__(self, tool, cwljob, **kwargs):
        if 'builder' in kwargs:
            builder = kwargs["builder"]
        else:
            builder = cwltool.builder.Builder()
            builder.job = cwljob
            builder.requirements = []
            builder.outdir = None
            builder.tmpdir = None
            builder.timeout = kwargs.get('eval_timeout')
            builder.resources = {}
        req = tool.evalResources(builder, {})
        self.cwltool = remove_pickle_problems(tool)
        # pass the default of None if basecommand is empty
        unitName = self.cwltool.tool.get("baseCommand", None)
        if isinstance(unitName, (list, tuple)):
            unitName = ' '.join(unitName)
        super(CWLJob, self).__init__(cores=req["cores"],
                                     memory=(req["ram"]*(2**20)),
                                     disk=((req["tmpdirSize"]*(2**20)) + (req["outdirSize"]*(2**20))),
                                     unitName=unitName)

        self.cwljob = cwljob
        try:
            self.jobName = str(self.cwltool.tool['id'])
        except KeyError:
            # fall back to the Toil defined class name if the tool doesn't have an identifier
            pass
        self.step_inputs = kwargs.get("step_inputs", self.cwltool.tool["inputs"])
        self.executor_options = kwargs

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)
        fillInDefaults(self.step_inputs, cwljob)

        opts = copy.deepcopy(self.executor_options)
        # Exports temporary directory for batch systems that reset TMPDIR
        os.environ["TMPDIR"] = os.path.realpath(opts.pop("tmpdir", None) or fileStore.getLocalTempDir())
        outdir = os.path.join(fileStore.getLocalTempDir(), "out")
        os.mkdir(outdir)
        tmp_outdir_prefix = os.path.join(opts.pop("workdir", None) or os.environ["TMPDIR"], "out_tmpdir")

        index = {}
        existing = {}
        opts.update({'t': self.cwltool, 'job_order_object': cwljob,
            'basedir': os.getcwd(), 'outdir': outdir,
            'tmp_outdir_prefix': tmp_outdir_prefix,
            'tmpdir_prefix': fileStore.getLocalTempDir(),
            'make_fs_access': functools.partial(ToilFsAccess, fileStore=fileStore),
            'toil_get_file': functools.partial(toilGetFile, fileStore, index, existing),
            'no_match_user': False})
        del opts['job_order']

        # Run the tool
        (output, status) = cwltool.main.single_job_executor(**opts)
        if status != "success":
            raise cwltool.errors.WorkflowException(status)

        adjustDirObjs(output, functools.partial(get_listing,
                                                cwltool.stdfsaccess.StdFsAccess(outdir),
                                                recursive=True))

        adjustFileObjs(output, functools.partial(uploadFile,
                                                 functools.partial(writeGlobalFileWrapper, fileStore),
                                                 index, existing))

        return output


def makeJob(tool, jobobj, **kwargs):
    """Create the correct Toil Job object for the CWL tool (workflow, job, or job
    wrapper for dynamic resource requirements.)

    """

    if tool.tool["class"] == "Workflow":
        options = copy.deepcopy(kwargs)
        options.update({'cwlwf': tool, 'cwljob': jobobj})
        wfjob = CWLWorkflow(**options)
        followOn = ResolveIndirect(wfjob.rv())
        wfjob.addFollowOn(followOn)
        return (wfjob, followOn)
    else:
        # get_requirement
        resourceReq, _ = tool.get_requirement("ResourceRequirement")
        if resourceReq:
            for req in ("coresMin", "coresMax", "ramMin", "ramMax",
                        "tmpdirMin", "tmpdirMax", "outdirMin", "outdirMax"):
                r = resourceReq.get(req)
                if isinstance(r, string_types) and ("$(" in r or "${" in r):
                    # Found a dynamic resource requirement so use a job wrapper.
                    options = copy.deepcopy(kwargs)
                    options.update({
                        'tool': tool,
                        'cwljob': jobobj})
                    job = CWLJobWrapper(**options)
                    return (job, job)
        options = copy.deepcopy(kwargs)
        options.update({
            'tool': tool,
            'cwljob': jobobj})
        job = CWLJob(**options)
        return (job, job)


class CWLScatter(Job):
    """Implement workflow scatter step.  When run, this creates a child job for
    each parameterization of the scatter.

    """

    def __init__(self, step, cwljob, **kwargs):
        super(CWLScatter, self).__init__()
        self.step = step
        self.cwljob = cwljob
        self.executor_options = kwargs

    def flat_crossproduct_scatter(self, joborder, scatter_keys, outputs, postScatterEval):
        scatter_key = shortname(scatter_keys[0])
        for n in range(0, len(joborder[scatter_key])):
            jo = copy.copy(joborder)
            jo[scatter_key] = joborder[scatter_key][n]
            if len(scatter_keys) == 1:
                jo = postScatterEval(jo)
                (subjob, followOn) = makeJob(self.step.embedded_tool, jo, **self.executor_options)
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                self.flat_crossproduct_scatter(jo, scatter_keys[1:], outputs, postScatterEval)

    def nested_crossproduct_scatter(self, joborder, scatter_keys, postScatterEval):
        scatter_key = shortname(scatter_keys[0])
        outputs = []
        for n in range(0, len(joborder[scatter_key])):
            jo = copy.copy(joborder)
            jo[scatter_key] = joborder[scatter_key][n]
            if len(scatter_keys) == 1:
                jo = postScatterEval(jo)
                (subjob, followOn) = makeJob(self.step.embedded_tool, jo, **self.executor_options)
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                outputs.append(self.nested_crossproduct_scatter(jo, scatter_keys[1:], postScatterEval))
        return outputs

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)

        if isinstance(self.step.tool["scatter"], string_types):
            scatter = [self.step.tool["scatter"]]
        else:
            scatter = self.step.tool["scatter"]

        scatterMethod = self.step.tool.get("scatterMethod", None)
        if len(scatter) == 1:
            scatterMethod = "dotproduct"
        outputs = []

        valueFrom = {shortname(i["id"]): i["valueFrom"] for i in self.step.tool["inputs"] if "valueFrom" in i}

        def postScatterEval(io):
            shortio = {shortname(k): v for k, v in iteritems(io)}
            for k in valueFrom:
                io.setdefault(k, None)

            def valueFromFunc(k, v):
                if k in valueFrom:
                    return cwltool.expression.do_eval(
                            valueFrom[k], shortio, self.step.requirements,
                            None, None, {}, context=v)
                else:
                    return v
            return {k: valueFromFunc(k, v) for k, v in list(io.items())}

        if scatterMethod == "dotproduct":
            for i in range(0, len(cwljob[shortname(scatter[0])])):
                copyjob = copy.copy(cwljob)
                for sc in [shortname(x) for x in scatter]:
                    copyjob[sc] = cwljob[sc][i]
                copyjob = postScatterEval(copyjob)
                (subjob, followOn) = makeJob(self.step.embedded_tool, copyjob, **self.executor_options)
                self.addChild(subjob)
                outputs.append(followOn.rv())
        elif scatterMethod == "nested_crossproduct":
            outputs = self.nested_crossproduct_scatter(cwljob, scatter, postScatterEval)
        elif scatterMethod == "flat_crossproduct":
            self.flat_crossproduct_scatter(cwljob, scatter, outputs, postScatterEval)
        else:
            if scatterMethod:
                raise validate.ValidationException(
                    "Unsupported complex scatter type '%s'" % scatterMethod)
            else:
                raise validate.ValidationException(
                    "Must provide scatterMethod to scatter over multiple inputs")

        return outputs


class CWLGather(Job):
    """Follows on to a scatter.  This gathers the outputs of each job in the
    scatter into an array for each output parameter.

    """

    def __init__(self, step, outputs):
        super(CWLGather, self).__init__()
        self.step = step
        self.outputs = outputs

    def allkeys(self, obj, keys):
        if isinstance(obj, dict):
            for k in list(obj.keys()):
                keys.add(k)
        elif isinstance(obj, list):
            for l in obj:
                self.allkeys(l, keys)

    def extract(self, obj, k):
        if isinstance(obj, dict):
            return obj.get(k)
        elif isinstance(obj, list):
            cp = []
            for l in obj:
                cp.append(self.extract(l, k))
            return cp
        else:
            return []

    def run(self, fileStore):
        outobj = {}

        def sn(n):
            if isinstance(n, dict):
                return shortname(n["id"])
            if isinstance(n, string_types):
                return shortname(n)

        for k in [sn(i) for i in self.step.tool["out"]]:
            outobj[k] = self.extract(self.outputs, k)

        return outobj


class SelfJob(object):
    """Fake job object to facilitate implementation of CWLWorkflow.run()"""

    def __init__(self, j, v):
        self.j = j
        self.v = v

    def rv(self):
        return self.v

    def addChild(self, c):
        return self.j.addChild(c)

    def hasChild(self, c):
        return self.j.hasChild(c)


def remove_pickle_problems(obj):
    """doc_loader does not pickle correctly, causing Toil errors, remove from objects.
    """
    if hasattr(obj, "doc_loader"):
        obj.doc_loader = None
    if hasattr(obj, "embedded_tool"):
        obj.embedded_tool = remove_pickle_problems(obj.embedded_tool)
    if hasattr(obj, "steps"):
        obj.steps = [remove_pickle_problems(s) for s in obj.steps]
    return obj


class CWLWorkflow(Job):
    """Traverse a CWL workflow graph and create a Toil job graph with appropriate
    dependencies.

    """

    def __init__(self, cwlwf, cwljob, **kwargs):
        super(CWLWorkflow, self).__init__()
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.executor_options = kwargs
        if "step_inputs" in self.executor_options:
            del self.executor_options["step_inputs"]
        self.cwlwf = remove_pickle_problems(self.cwlwf)

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)

        # `promises` dict
        # from: each parameter (workflow input or step output)
        #   that may be used as a "source" for a step input workflow output
        #   parameter
        # to: the job that will produce that value.
        promises = {}

        # `jobs` dict from step id to job that implements that step.
        jobs = {}

        for inp in self.cwlwf.tool["inputs"]:
            promises[inp["id"]] = SelfJob(self, cwljob)

        alloutputs_fufilled = False
        while not alloutputs_fufilled:
            # Iteratively go over the workflow steps, scheduling jobs as their
            # dependencies can be fufilled by upstream workflow inputs or
            # step outputs.  Loop exits when the workflow outputs
            # are satisfied.

            alloutputs_fufilled = True

            for step in self.cwlwf.steps:
                if step.tool["id"] not in jobs:
                    stepinputs_fufilled = True
                    for inp in step.tool["inputs"]:
                        if "source" in inp:
                            for s in aslist(inp["source"]):
                                if s not in promises:
                                    stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        jobobj = {}

                        for inp in step.tool["inputs"]:
                            key = shortname(inp["id"])
                            if "source" in inp:
                                if inp.get("linkMerge") or len(aslist(inp["source"])) > 1:
                                    linkMerge = inp.get("linkMerge", "merge_nested")
                                    if linkMerge == "merge_nested":
                                        jobobj[key] = (
                                            MergeInputsNested([(shortname(s), promises[s].rv())
                                                               for s in aslist(inp["source"])]))
                                    elif linkMerge == "merge_flattened":
                                        jobobj[key] = (
                                            MergeInputsFlattened([(shortname(s), promises[s].rv())
                                                                  for s in aslist(inp["source"])]))
                                    else:
                                        raise validate.ValidationException(
                                            "Unsupported linkMerge '%s'", linkMerge)
                                else:
                                    jobobj[key] = (shortname(inp["source"]),
                                                   promises[inp["source"]].rv())
                            elif "default" in inp:
                                d = copy.copy(inp["default"])
                                jobobj[key] = ("default", {"default": d})

                            if "valueFrom" in inp and "scatter" not in step.tool:
                                if key in jobobj:
                                    jobobj[key] = StepValueFrom(inp["valueFrom"],
                                                                jobobj[key],
                                                                self.cwlwf.requirements)
                                else:
                                    jobobj[key] = StepValueFrom(inp["valueFrom"],
                                                                ("None", {"None": None}),
                                                                self.cwlwf.requirements)

                        if "scatter" in step.tool:
                            wfjob = CWLScatter(step, IndirectDict(jobobj), **self.executor_options)
                            followOn = CWLGather(step, wfjob.rv())
                            wfjob.addFollowOn(followOn)
                        else:
                            (wfjob, followOn) = makeJob(step.embedded_tool, IndirectDict(jobobj),
                                                        step_inputs=step.tool["inputs"],
                                                        **self.executor_options)

                        jobs[step.tool["id"]] = followOn

                        connected = False
                        for inp in step.tool["inputs"]:
                            for s in aslist(inp.get("source", [])):
                                if not promises[s].hasChild(wfjob):
                                    promises[s].addChild(wfjob)
                                    connected = True
                        if not connected:
                            # workflow step has default inputs only, isn't connected to other jobs,
                            # so add it as child of workflow.
                            self.addChild(wfjob)

                        for out in step.tool["outputs"]:
                            promises[out["id"]] = followOn

                for inp in step.tool["inputs"]:
                    for s in aslist(inp.get("source", [])):
                        if s not in promises:
                            alloutputs_fufilled = False

            # may need a test
            for out in self.cwlwf.tool["outputs"]:
                if "source" in out:
                    if out["source"] not in promises:
                        alloutputs_fufilled = False

        outobj = {}
        for out in self.cwlwf.tool["outputs"]:
            outobj[shortname(out["id"])] = (shortname(out["outputSource"]), promises[out["outputSource"]].rv())

        return IndirectDict(outobj)


cwltool.process.supportedProcessRequirements = ("DockerRequirement",
                                                "ExpressionEngineRequirement",
                                                "InlineJavascriptRequirement",
                                                "InitialWorkDirRequirement",
                                                "SchemaDefRequirement",
                                                "EnvVarRequirement",
                                                "CreateFileRequirement",
                                                "SubworkflowFeatureRequirement",
                                                "ScatterFeatureRequirement",
                                                "ShellCommandRequirement",
                                                "MultipleInputFeatureRequirement",
                                                "StepInputExpressionRequirement",
                                                "ResourceRequirement")


def unsupportedRequirementsCheck(requirements):
    """Check for specific requirement cases we don't support.
    """
    pass


def visitSteps(t, op):
    if isinstance(t, cwltool.workflow.Workflow):
        for s in t.steps:
            op(s.tool)
            visitSteps(s.embedded_tool, op)


def main(args=None, stdout=sys.stdout):
    parser = argparse.ArgumentParser()
    Job.Runner.addToilOptions(parser)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", nargs=argparse.REMAINDER)

    # Will override the "jobStore" positional argument, enables
    # user to select jobStore or get a default from logic one below.
    parser.add_argument("--jobStore", type=str)
    parser.add_argument("--not-strict", action="store_true")
    parser.add_argument("--no-container", action="store_true")
    parser.add_argument("--quiet", dest="logLevel", action="store_const", const="ERROR")
    parser.add_argument("--basedir", type=str)
    parser.add_argument("--outdir", type=str, default=os.getcwd())
    parser.add_argument("--version", action='version', version=baseVersion)
    parser.add_argument("--user-space-docker-cmd",
                        help="(Linux/OS X only) Specify a user space docker "
                        "command (like udocker or dx-docker) that will be "
                        "used to call 'pull' and 'run'")
    parser.add_argument("--preserve-environment", type=str, nargs='+',
                    help="Preserve specified environment variables when running CommandLineTools",
                    metavar=("VAR1 VAR2"),
                    default=("PATH",),
                    dest="preserve_environment")
    # help="Dependency resolver configuration file describing how to adapt 'SoftwareRequirement' packages to current system."
    parser.add_argument("--beta-dependency-resolvers-configuration", default=None)
    # help="Defaut root directory used by dependency resolvers configuration."
    parser.add_argument("--beta-dependencies-directory", default=None)
    # help="Use biocontainers for tools without an explicitly annotated Docker container."
    parser.add_argument("--beta-use-biocontainers", default=None, action="store_true")
    # help="Short cut to use Conda to resolve 'SoftwareRequirement' packages."
    parser.add_argument("--beta-conda-dependencies", default=None, action="store_true")
    parser.add_argument("--tmpdir-prefix", type=Text,
                        help="Path prefix for temporary directories",
                        default="tmp")
    parser.add_argument("--tmp-outdir-prefix", type=Text,
                        help="Path prefix for intermediate output directories",
                        default="tmp")

    # mkdtemp actually creates the directory, but
    # toil requires that the directory not exist,
    # so make it and delete it and allow
    # toil to create it again (!)
    workdir = tempfile.mkdtemp()
    os.rmdir(workdir)

    if args is None:
        args = sys.argv[1:]

    options = parser.parse_args([workdir] + args)

    use_container = not options.no_container

    setLoggingFromOptions(options)
    if options.logLevel:
        cwllogger.setLevel(options.logLevel)

    outdir = os.path.abspath(options.outdir)
    fileindex = {}
    existing = {}
    make_tool_kwargs = {}
    conf_file = getattr(options, "beta_dependency_resolvers_configuration", None)  # Text
    use_conda_dependencies = getattr(options, "beta_conda_dependencies", None)  # Text
    job_script_provider = None
    if conf_file or use_conda_dependencies:
        dependencies_configuration = DependenciesConfiguration(options)  # type: DependenciesConfiguration
        job_script_provider = dependencies_configuration

    options.default_container = None
    make_tool_kwargs["find_default_container"] = functools.partial(find_default_container, options)

    with Toil(options) as toil:
        if options.restart:
            outobj = toil.restart()
        else:
            toil.config.linkImports = False
            useStrict = not options.not_strict
            make_tool_kwargs["hints"] = [{
                "class": "ResourceRequirement",
                "coresMin": toil.config.defaultCores,
                "ramMin": toil.config.defaultMemory / (2**20),
                "outdirMin": toil.config.defaultDisk / (2**20),
                "tmpdirMin": 0
            }]
            try:
                t = cwltool.load_tool.load_tool(options.cwltool, toilMakeTool,
                                                kwargs=make_tool_kwargs,
                                                resolver=cwltool.resolver.tool_resolver,
                                                strict=useStrict)
                unsupportedRequirementsCheck(t.requirements)
            except cwltool.process.UnsupportedRequirement as e:
                logging.error(e)
                return 33

            if type(t) == int:
                return t

            options.workflow = options.cwltool
            options.job_order = options.cwljob
            options.tool_help = None
            options.debug = options.logLevel == "DEBUG"
            job, options.basedir, loader = cwltool.main.load_job_order(
                options, sys.stdin, None, [], options.job_order)
            job = cwltool.main.init_job_order(job, options, t, loader=loader)

            fillInDefaults(t.tool["inputs"], job)

            def pathToLoc(p):
                if "location" not in p and "path" in p:
                    p["location"] = p["path"]
                    del p["path"]

            def importFiles(tool):
                visit_class(tool, ("File", "Directory"), pathToLoc)
                normalizeFilesDirs(tool)
                adjustDirObjs(tool, functools.partial(get_listing,
                                                      cwltool.stdfsaccess.StdFsAccess(""),
                                                      recursive=True))
                adjustFileObjs(tool, functools.partial(uploadFile,
                                                       toil.importFile,
                                                       fileindex, existing, skip_broken=True))

            t.visit(importFiles)

            for inp in t.tool["inputs"]:
                def setSecondary(fileobj):
                    if isinstance(fileobj, dict) and fileobj.get("class") == "File":
                        if "secondaryFiles" not in fileobj:
                            fileobj["secondaryFiles"] = [{
                                "location": cwltool.builder.substitute(fileobj["location"], sf), "class": "File"}
                                                         for sf in inp["secondaryFiles"]]

                    if isinstance(fileobj, list):
                        for e in fileobj:
                            setSecondary(e)

                if shortname(inp["id"]) in job and inp.get("secondaryFiles"):
                    setSecondary(job[shortname(inp["id"])])

            importFiles(job)
            visitSteps(t, importFiles)

            try:
                make_opts = copy.deepcopy(vars(options))
                make_opts.update({'tool': t, 'jobobj': {},
                    'use_container': use_container,
                    'tmpdir': os.path.realpath(outdir),
                    'job_script_provider': job_script_provider})

                (wf1, wf2) = makeJob(**make_opts)
            except cwltool.process.UnsupportedRequirement as e:
                logging.error(e)
                return 33

            wf1.cwljob = job
            outobj = toil.start(wf1)

        outobj = resolve_indirect(outobj)

        toilStageFiles(toil, outobj, outdir, fileindex, existing, True)

        visit_class(outobj, ("File",), functools.partial(compute_checksums, cwltool.stdfsaccess.StdFsAccess("")))

        stdout.write(json.dumps(outobj, indent=4))

    return 0


def find_default_container(args, builder):
    default_container = None
    if args.default_container:
        default_container = args.default_container
    elif args.beta_use_biocontainers:
        default_container = get_container_from_software_requirements(args, builder)

    return default_container
