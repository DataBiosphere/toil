# Implement support for Common Workflow Language (CWL) for Toil.
#
# Copyright (C) 2015 Curoverse, Inc
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

from toil.job import Job
from toil.version import version

from argparse import ArgumentParser
import cwltool.main
import cwltool.workflow
import cwltool.expression
from cwltool.process import adjustFiles, shortname
from cwltool.aslist import aslist
import schema_salad.validate as validate
import schema_salad.ref_resolver
import os
import tempfile
import json
import sys
import logging
import copy
import shutil
import functools

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
    pass

class MergeInputs(object):
    def __init__(self, sources):
        self.sources = sources
    def resolve(self):
        raise NotImplementedError()

class MergeInputsNested(MergeInputs):
    def resolve(self):
        return [v[1][v[0]] for v in self.sources]

class MergeInputsFlattened(MergeInputs):
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
    def __init__(self, expr, inner, req):
        self.expr = expr
        self.inner = inner
        self.req = req

    def do_eval(self, inputs, ctx):
        return cwltool.expression.do_eval(self.expr, inputs, self.req,
                                          None, None, {}, context=ctx)

def resolve_indirect_inner(d):
    if isinstance(d, IndirectDict):
        r = {}
        for k, v in d.items():
            if isinstance(v, MergeInputs):
                r[k] = v.resolve()
            else:
                r[k] = v[1][v[0]]
        return r
    else:
        return d

def resolve_indirect(d):
    inner = IndirectDict() if isinstance(d, IndirectDict) else {}
    needEval = False
    for k, v in d.iteritems():
        if isinstance(v, StepValueFrom):
            inner[k] = v.inner
            needEval = True
        else:
            inner[k] = v
    res = resolve_indirect_inner(inner)
    if needEval:
        ev = {}
        for k, v in d.iteritems():
            if isinstance(v, StepValueFrom):
                ev[k] = v.do_eval(res, res[k])
            else:
                ev[k] = v
        return ev
    else:
        return res

def getFile(fileStore, dir, fileStoreID, fileName, index=None, copy=False):
    srcPath = fileStore.readGlobalFile(fileStoreID)
    dstPath = os.path.join(dir, fileName)
    if srcPath != dstPath:
        if copy:
            shutil.copyfile(srcPath, dstPath)
        else:
            if os.path.exists(dstPath):
                if index.get(dstPath, None) != fileStoreID:
                    raise Exception("Conflicting filesStoreID %s and %s both trying to link to %s" % (index.get(dstPath, None), fileStoreID, dstPath))
            else:
                os.symlink(srcPath, dstPath)
        index[dstPath] = fileStoreID
    return dstPath

def writeFile(fileStore, index, x):
    if x not in index:
        index[x] = (fileStore.writeGlobalFile(x), x.split('/')[-1])
    return index[x]

class StageJob(Job):
    """File staging job to put local files into the global file store.

    This currently will break if you try and run this on a cluster because the
    main() method can't stage files before Job.Runner.startToil(), and the
    staging job could run on a compute node where it doesn't have direct access
    to the input files of the head node.

    """

    def __init__(self, cwlwf, cwljob, basedir):
        Job.__init__(self)
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.basedir = basedir

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)
        builder = self.cwlwf._init_job(cwljob, self.basedir)
        adjustFiles(builder.job, functools.partial(writeFile, fileStore, {}))
        return builder.job


class FinalJob(Job):
    """Wrap-up job to write output JSON and copy output files from global file
    store to current working directory.

    This currently will break if you try and run this on a cluster because the
    main() method can't access files produced by Job.Runner.startToil(), and
    the staging job could run on a compute node where it doesn't have direct
    access to the output directory of the head node.

    """

    def __init__(self, cwljob, outdir):
        Job.__init__(self)
        self.cwljob = cwljob
        self.outdir = outdir

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)

        index={}
        adjustFiles(cwljob, lambda x: getFile(fileStore, self.outdir, *x, index=index, copy=True))
        with open(os.path.join(self.outdir, "cwl.output.json"), "w") as f:
            json.dump(cwljob, f, indent=4)
        return True


class ResolveIndirect(Job):
    def __init__(self, cwljob):
        Job.__init__(self)
        self.cwljob = cwljob

    def run(self, fileStore):
        return resolve_indirect(self.cwljob)


class CWLJob(Job):
    """Execute a CWL tool wrapper."""

    def __init__(self, cwltool, cwljob):
        Job.__init__(self)
        self.cwltool = cwltool
        self.cwljob = cwljob

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)

        inpdir = os.path.join(fileStore.getLocalTempDir(), "inp")
        outdir = os.path.join(fileStore.getLocalTempDir(), "out")
        tmpdir = os.path.join(fileStore.getLocalTempDir(), "tmp")
        os.mkdir(inpdir)
        os.mkdir(outdir)
        os.mkdir(tmpdir)

        # Copy input files out of the global file store.

        index={}
        adjustFiles(cwljob, lambda x: getFile(fileStore, inpdir, *x, index=index))

        logging.getLogger("cwltool").setLevel(logging.DEBUG)

        output = cwltool.main.single_job_executor(self.cwltool, cwljob,
                                                  os.getcwd(), None,
                                                  outdir=outdir,
                                                  tmpdir=tmpdir,
                                                  use_container=True)

        # Copy output files into the global file store.
        adjustFiles(output, functools.partial(writeFile, fileStore, {}))

        return output


def makeJob(tool, jobobj):
    if tool.tool["class"] == "Workflow":
        wfjob = CWLWorkflow(tool, jobobj)
        followOn = ResolveIndirect(wfjob.rv())
        wfjob.addFollowOn(followOn)
        return (wfjob, followOn)
    else:
        job = CWLJob(tool, jobobj)
        return (job, job)


class CWLScatter(Job):
    def __init__(self, step, cwljob):
        Job.__init__(self)
        self.step = step
        self.cwljob = cwljob
        self.valueFrom = {shortname(i["id"]): i["valueFrom"] for i in step.tool["inputs"] if "valueFrom" in i}

    def valueFromFunc(self, k, v):
        if k in self.valueFrom:
            return cwltool.expression.do_eval(self.valueFrom[k], self.vfinputs, self.step.requirements,
                                              None, None, {}, context=v)
        else:
            return v

    def flat_crossproduct_scatter(self, joborder, scatter_keys, outputs):
        scatter_key = shortname(scatter_keys[0])
        l = len(joborder[scatter_key])
        for n in xrange(0, l):
            jo = copy.copy(joborder)
            jo[scatter_key] = self.valueFromFunc(scatter_key, joborder[scatter_key][n])
            if len(scatter_keys) == 1:
                (subjob, followOn) = makeJob(self.step.embedded_tool, jo)
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                self.flat_crossproduct_scatter(jo, scatter_keys[1:], outputs)

    def nested_crossproduct_scatter(self, joborder, scatter_keys):
        scatter_key = shortname(scatter_keys[0])
        l = len(joborder[scatter_key])
        outputs = []
        for n in xrange(0, l):
            jo = copy.copy(joborder)
            jo[scatter_key] = self.valueFromFunc(scatter_key, joborder[scatter_key][n])
            if len(scatter_keys) == 1:
                (subjob, followOn) = makeJob(self.step.embedded_tool, jo)
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                outputs.append(self.nested_crossproduct_scatter(jo, scatter_keys[1:]))
        return outputs

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)

        if isinstance(self.step.tool["scatter"], basestring):
            scatter = [self.step.tool["scatter"]]
        else:
            scatter = self.step.tool["scatter"]

        scatterMethod = self.step.tool.get("scatterMethod", None)
        if len(scatter) == 1:
            scatterMethod = "dotproduct"
        outputs = []

        self.vfinputs = cwljob

        if scatterMethod == "dotproduct":
            for i in xrange(0, len(cwljob[shortname(scatter[0])])):
                copyjob = copy.copy(cwljob)
                for sc in scatter:
                    scatter_key = shortname(sc)
                    copyjob[scatter_key] = self.valueFromFunc(scatter_key, cwljob[scatter_key][i])
                (subjob, followOn) = makeJob(self.step.embedded_tool, copyjob)
                self.addChild(subjob)
                outputs.append(followOn.rv())
        elif scatterMethod == "nested_crossproduct":
            outputs = self.nested_crossproduct_scatter(cwljob, scatter)
        elif scatterMethod == "flat_crossproduct":
            self.flat_crossproduct_scatter(cwljob, scatter, outputs)
        else:
            if scatterMethod:
                raise validate.ValidationException(
                    "Unsupported complex scatter type '%s'" % scatterMethod)
            else:
                raise validate.ValidationException(
                    "Must provide scatterMethod to scatter over multiple inputs")

        return outputs


class CWLGather(Job):
    def __init__(self, step, outputs):
        Job.__init__(self)
        self.step = step
        self.outputs = outputs

    def allkeys(self, obj, keys):
        if isinstance(obj, dict):
            for k in obj.keys():
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

    def run(self, fileStore):
        outobj = {}
        keys = set()
        self.allkeys(self.outputs, keys)

        logging.warn("Outputs is %s", self.outputs)
        for k in keys:
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


class CWLWorkflow(Job):
    """Traverse a CWL workflow graph and schedule a Toil job graph."""

    def __init__(self, cwlwf, cwljob):
        Job.__init__(self)
        self.cwlwf = cwlwf
        self.cwljob = cwljob

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

                        # TODO: Handle multiple inbound links
                        # (both are discussed in section 5.1.2 in CWL spec draft-2)

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
                                    jobobj[key] = (
                                    shortname(inp["source"]), promises[inp["source"]].rv())
                            elif "default" in inp:
                                d = copy.copy(inp["default"])
                                adjustFiles(d, lambda x: x.replace("file://", ""))
                                adjustFiles(d, functools.partial(writeFile, fileStore, {}))
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
                            wfjob = CWLScatter(step, IndirectDict(jobobj))
                            followOn = CWLGather(step, wfjob.rv())
                            wfjob.addFollowOn(followOn)
                        else:
                            (wfjob, followOn) = makeJob(step.embedded_tool, IndirectDict(jobobj))

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
            outobj[shortname(out["id"])] = (shortname(out["source"]), promises[out["source"]].rv())

        return IndirectDict(outobj)


supportedProcessRequirements = ("DockerRequirement",
                                "ExpressionEngineRequirement",
                                "InlineJavascriptRequirement",
                                "SchemaDefRequirement",
                                "EnvVarRequirement",
                                "CreateFileRequirement",
                                "SubworkflowFeatureRequirement",
                                "ScatterFeatureRequirement",
                                "ShellCommandRequirement",
                                "MultipleInputFeatureRequirement",
                                "StepInputExpressionRequirement")


def checkRequirements(rec):
    if isinstance(rec, dict):
        if "requirements" in rec:
            for r in rec["requirements"]:
                if r["class"] not in supportedProcessRequirements:
                    raise validate.ValidationException("Unsupported requirement %s" % r["class"])
        # if "scatter" in rec:
        #     if isinstance(rec["scatter"], list) and rec["scatter"] > 1:
        #         raise Exception("Unsupported complex scatter type '%s'" % rec.get("scatterMethod"))
        for d in rec:
            checkRequirements(rec[d])
    if isinstance(rec, list):
        for d in rec:
            checkRequirements(d)


def main(args=None):
    parser = ArgumentParser()
    Job.Runner.addToilOptions(parser)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", type=str)

    # Will override the "jobStore" positional argument, enables
    # user to select jobStore or get a default from logic one below.
    parser.add_argument("--jobStore", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    parser.add_argument("--no-container", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--basedir", type=str)
    parser.add_argument("--outdir", type=str, default=os.getcwd())
    parser.add_argument("--version", action='version', version=version)

    # mkdtemp actually creates the directory, but
    # toil requires that the directory not exist,
    # so make it and delete it and allow
    # toil to create it again (!)
    workdir = tempfile.mkdtemp()
    os.rmdir(workdir)

    if args is None:
        args = sys.argv[1:]

    options = parser.parse_args([workdir] + args)

    if options.quiet:
        options.logLevel = "WARNING"

    uri = "file://" + os.path.abspath(options.cwljob)

    t = cwltool.main.load_tool(options.cwltool, False, True,
                               cwltool.workflow.defaultMakeTool,
                               True)

    if options.conformance_test:
        loader = schema_salad.ref_resolver.Loader({})
    else:
        jobloaderctx = {"path": {"@type": "@id"}, "format": {"@type": "@id"}}
        jobloaderctx.update(t.metadata.get("$namespaces", {}))
        loader = schema_salad.ref_resolver.Loader(jobloaderctx)

    job, _ = loader.resolve_ref(uri)

    if type(t) == int:
        return t

    try:
        checkRequirements(t.tool)
    except Exception as e:
        logging.error(e)
        return 33

    jobobj = {}
    for inp in t.tool["inputs"]:
        if shortname(inp["id"]) in job:
            pass
        elif shortname(inp["id"]) not in job and "default" in inp:
            job[shortname(inp["id"])] = copy.copy(inp["default"])
        elif shortname(inp["id"]) not in job and inp["type"][0] == "null":
            pass
        else:
            raise validate.ValidationException("Missing inputs `%s`" % shortname(inp["id"]))

    adjustFiles(job, lambda x: x.replace("file://", ""))

    if options.conformance_test:
        sys.stdout.write(json.dumps(
            cwltool.main.single_job_executor(t, job, options.basedir, options,
                                             conformance_test=True), indent=4))
        return 0

    if not options.basedir:
        options.basedir = os.path.dirname(os.path.abspath(options.cwljob))

    outdir = options.outdir

    staging = StageJob(t, job, os.path.dirname(os.path.abspath(options.cwljob)))

    (wf1, wf2) = makeJob(t, staging.rv())

    staging.addFollowOn(wf1)
    wf2.addFollowOn(FinalJob(wf2.rv(), outdir))

    Job.Runner.startToil(staging, options)

    with open(os.path.join(outdir, "cwl.output.json"), "r") as f:
        sys.stdout.write(f.read())

    return 0
