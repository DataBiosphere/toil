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

from toil.job import Job
from toil.common import Toil
from toil.version import baseVersion
from toil.lib.bioio import setLoggingFromOptions

from argparse import ArgumentParser
import cwltool.errors
import cwltool.load_tool
import cwltool.main
import cwltool.workflow
import cwltool.expression
import cwltool.builder
import cwltool.resolver
import cwltool.stdfsaccess
from cwltool.pathmapper import adjustFiles
from cwltool.process import shortname, adjustFilesWithSecondary, fillInDefaults, compute_checksums
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

# Python 3 compatibility imports
from six.moves import xrange
from six import iteritems, string_types
import six.moves.urllib.parse as urlparse

cwllogger = logging.getLogger("cwltool")

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
    for k, v in iteritems(d):
        if isinstance(v, StepValueFrom):
            inner[k] = v.inner
            needEval = True
        else:
            inner[k] = v
    res = resolve_indirect_inner(inner)
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

def getFile(fileStore, dir, fileTuple, index=None, export=False, primary=None, rename_collision=False,
            existing={}):
    """Extract input file from Toil jobstore.

    Uses standard filestore to retrieve file, then provides a symlink to it
    for running. If export is True (for final outputs), it gets copied to
    the final location.

    Keeps track of files being used locally with 'existing'
    """
    # File literal outputs with no path, from writeFile
    if fileTuple is None:
        raise cwltool.process.UnsupportedRequirement("CWL expression file inputs not yet supported in Toil")
    fileStoreID, fileName = fileTuple

    if rename_collision is False:
        if primary:
            dir = os.path.dirname(primary)
        else:
            dir = tempfile.mkdtemp(dir=dir)

    dstPath = os.path.join(dir, fileName)
    if rename_collision:
        n = 1
        while os.path.exists(dstPath):
            n += 1
            stem, ext = os.path.splitext(dstPath)
            stem = "%s_%s" % (stem, n)
            dstPath = stem + ext

    if export:
        fileStore.exportFile(fileStoreID, "file://" + dstPath)
    else:
        srcPath = fileStore.readGlobalFile(fileStoreID)
        if srcPath != dstPath:
            if os.path.exists(dstPath):
                if index.get(dstPath, None) != fileStoreID:
                    raise Exception("Conflicting filesStoreID %s and %s both trying to link to %s" % (index.get(dstPath, None), fileStoreID, dstPath))
            else:
                os.symlink(srcPath, dstPath)
                existing[srcPath] = dstPath
            index[dstPath] = fileStoreID
    return dstPath

def writeFile(writeFunc, index, existing, x):
    """Write output files back into Toil jobstore.

    'existing' is a set of files retrieved as inputs from getFile. This ensures
    they are mapped back as the same name if passed through.
    """
    # Toil fileStore references are tuples of pickle and internal file
    if isinstance(x, tuple):
        return x
    # File literal outputs with no path, we don't write these and will fail
    # with unsupportedRequirement when retrieving later with getFile
    elif x.startswith("_:"):
        return None
    else:
        if x not in index:
            x = existing.get(x, x)
            if not urlparse.urlparse(x).scheme:
                rp = os.path.realpath(x)
            else:
                rp = x
            try:
                index[x] = (writeFunc(rp), os.path.basename(x))
            except Exception as e:
                cwllogger.error("Got exception '%s' while copying '%s'", e, x)
                raise
        return index[x]

def computeFileChecksums(fs_access, f):
    # File literal inputs with no path, no checksum
    if isinstance(f, dict) and f.get("location", "").startswith("_:"):
        return f
    else:
        return compute_checksums(fs_access, f)

def addFilePartRefs(p):
    """Provides new v1.0 functionality for referencing file parts.
    """
    if p.get("class") == "File" and p.get("path"):
        dirname, basename = os.path.split(p["path"])
        nameroot, nameext = os.path.splitext(basename)
        for k, v in [("dirname", dirname,), ("basename", basename),
                     ("nameroot", nameroot), ("nameext", nameext)]:
            p[k] = v
    return p

def locToPath(p):
    """Back compatibility -- handle converting locations into paths.
    """
    if "path" not in p and "location" in p:
        p["path"] = p["location"].replace("file:", "")
    return p

def pathToLoc(p):
    """Associate path with location.

    v1.0 should be specifying location but older YAML uses path
    -- this provides back compatibility.
    """
    if "path" in p:
        p["location"] = p["path"]
    return p

class ResolveIndirect(Job):
    def __init__(self, cwljob):
        super(ResolveIndirect, self).__init__()
        self.cwljob = cwljob

    def run(self, fileStore):
        return resolve_indirect(self.cwljob)


class CWLJob(Job):
    """Execute a CWL tool wrapper."""

    def __init__(self, tool, cwljob, **kwargs):
        if 'builder' in kwargs:
            builder = kwargs["builder"]
        else:
            builder = cwltool.builder.Builder()
            builder.job = {}
            builder.requirements = []
            builder.outdir = None
            builder.tmpdir = None
            builder.timeout = 0
            builder.resources = {}
        req = tool.evalResources(builder, {})
        self.cwltool = remove_pickle_problems(tool)
        # pass the default of None if basecommand is empty
        unitName = self.cwltool.tool.get("baseCommand", None)
        if isinstance(unitName, (list, tuple)):
            unitName = ' '.join(unitName)
        super(CWLJob, self).__init__(cores=req["cores"],
                                     memory=(req["ram"]*1024*1024),
                                     disk=((req["tmpdirSize"]*1024*1024) + (req["outdirSize"]*1024*1024)),
                                     unitName=unitName)
        #super(CWLJob, self).__init__()
        self.cwljob = cwljob
        try:
            self.jobName = str(self.cwltool.tool['id'])
        except KeyError:
            # fall back to the Toil defined class name if the tool doesn't have an identifier
            pass
        self.executor_options = kwargs

    def run(self, fileStore):
        cwljob = resolve_indirect(self.cwljob)
        fillInDefaults(self.cwltool.tool["inputs"], cwljob)

        inpdir = os.path.join(fileStore.getLocalTempDir(), "inp")
        outdir = os.path.join(fileStore.getLocalTempDir(), "out")
        tmpdir = os.path.join(fileStore.getLocalTempDir(), "tmp")
        os.mkdir(inpdir)
        os.mkdir(outdir)
        os.mkdir(tmpdir)

        # Copy input files out of the global file store, ensure path/location synchronized
        index = {}
        existing = {}
        adjustFilesWithSecondary(cwljob, functools.partial(getFile, fileStore, inpdir, index=index,
                                                           existing=existing))
        cwltool.pathmapper.adjustFileObjs(cwljob, pathToLoc)
        cwltool.pathmapper.adjustFileObjs(cwljob, addFilePartRefs)

        # Run the tool
        opts = copy.deepcopy(self.executor_options)
        # Exports temporary directory for batch systems that reset TMPDIR
        os.environ["TMPDIR"] = os.path.realpath(opts.pop("tmpdir", None) or tmpdir)
        (output, status) = cwltool.main.single_job_executor(self.cwltool, cwljob,
                                                            basedir=os.getcwd(),
                                                            outdir=outdir,
                                                            tmpdir=tmpdir,
                                                            tmpdir_prefix="tmp",
                                                            make_fs_access=cwltool.stdfsaccess.StdFsAccess,
                                                            **opts)
        if status != "success":
            raise cwltool.errors.WorkflowException(status)
        cwltool.pathmapper.adjustDirObjs(output, locToPath)
        cwltool.pathmapper.adjustFileObjs(output, locToPath)
        cwltool.pathmapper.adjustFileObjs(output, functools.partial(computeFileChecksums,
                                                                    cwltool.stdfsaccess.StdFsAccess(outdir)))
        # Copy output files into the global file store.
        adjustFiles(output, functools.partial(writeFile, fileStore.writeGlobalFile, {}, existing))
        return output


def makeJob(tool, jobobj, **kwargs):
    if tool.tool["class"] == "Workflow":
        wfjob = CWLWorkflow(tool, jobobj, **kwargs)
        followOn = ResolveIndirect(wfjob.rv())
        wfjob.addFollowOn(followOn)
        return (wfjob, followOn)
    else:
        job = CWLJob(tool, jobobj, **kwargs)
        return (job, job)


class CWLScatter(Job):
    def __init__(self, step, cwljob, **kwargs):
        super(CWLScatter, self).__init__()
        self.step = step
        self.cwljob = cwljob
        self.executor_options = kwargs

    def flat_crossproduct_scatter(self, joborder, scatter_keys, outputs, postScatterEval):
        scatter_key = shortname(scatter_keys[0])
        l = len(joborder[scatter_key])
        for n in xrange(0, l):
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
        l = len(joborder[scatter_key])
        outputs = []
        for n in xrange(0, l):
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
            def valueFromFunc(k, v):
                if k in valueFrom:
                    return cwltool.expression.do_eval(
                            valueFrom[k], shortio, self.step.requirements,
                            None, None, {}, context=v)
                else:
                    return v
            return {k: valueFromFunc(k, v) for k,v in io.items()}

        if scatterMethod == "dotproduct":
            for i in xrange(0, len(cwljob[shortname(scatter[0])])):
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
    def __init__(self, step, outputs):
        super(CWLGather, self).__init__()
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
    """Traverse a CWL workflow graph and schedule a Toil job graph."""

    def __init__(self, cwlwf, cwljob, **kwargs):
        super(CWLWorkflow, self).__init__()
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.executor_options = kwargs
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
                                    jobobj[key] = (
                                    shortname(inp["source"]), promises[inp["source"]].rv())
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

def unsupportedInputCheck(p):
    """Check for file inputs we don't current support in Toil:

    - Directories
    - File literals
    """
    if p.get("class") == "Directory":
        raise cwltool.process.UnsupportedRequirement("CWL Directory inputs not yet supported in Toil")
    if p.get("contents") and (not p.get("path") and not p.get("location")):
        raise cwltool.process.UnsupportedRequirement("CWL File literals not yet supported in Toil")

def unsupportedRequirementsCheck(requirements):
    """Check for specific requirement cases we don't support.
    """
    for r in requirements:
        if r["class"] == "InitialWorkDirRequirement":
            for l in r.get("listing", []):
                if isinstance(l, dict) and l.get("writable"):
                    raise cwltool.process.UnsupportedRequirement("CWL writable InitialWorkDirRequirement not yet supported in Toil")

def unsupportedDefaultCheck(tool):
    """Check for file-based defaults, which don't get staged correctly in Toil.
    """
    for inp in tool["in"]:
        if isinstance(inp, dict) and "default" in inp:
            if isinstance(inp["default"], dict) and inp["default"].get("class") == "File":
                raise cwltool.process.UnsupportedRequirement("CWL default file inputs not yet supported in Toil")

def main(args=None, stdout=sys.stdout):
    parser = ArgumentParser()
    Job.Runner.addToilOptions(parser)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", type=str, nargs="?", default=None)

    # Will override the "jobStore" positional argument, enables
    # user to select jobStore or get a default from logic one below.
    parser.add_argument("--jobStore", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    parser.add_argument("--not-strict", action="store_true")
    parser.add_argument("--no-container", action="store_true")
    parser.add_argument("--quiet", dest="logLevel", action="store_const", const="ERROR")
    parser.add_argument("--basedir", type=str)
    parser.add_argument("--outdir", type=str, default=os.getcwd())
    parser.add_argument("--version", action='version', version=baseVersion)
    parser.add_argument("--preserve-environment", type=str, nargs='+',
                    help="Preserve specified environment variables when running CommandLineTools",
                    metavar=("VAR1 VAR2"),
                    default=("PATH",),
                    dest="preserve_environment")

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

    useStrict = not options.not_strict
    try:
        t = cwltool.load_tool.load_tool(options.cwltool, cwltool.workflow.defaultMakeTool,
                                        resolver=cwltool.resolver.tool_resolver, strict=useStrict)
        unsupportedRequirementsCheck(t.requirements)
    except cwltool.process.UnsupportedRequirement as e:
        logging.error(e)
        return 33

    if options.conformance_test:
        loader = schema_salad.ref_resolver.Loader({})
    else:
        jobloaderctx = {"path": {"@type": "@id"}, "format": {"@type": "@id"}}
        jobloaderctx.update(t.metadata.get("$namespaces", {}))
        loader = schema_salad.ref_resolver.Loader(jobloaderctx)

    if options.cwljob:
        uri = (options.cwljob if urlparse.urlparse(options.cwljob).scheme
               else "file://" + os.path.abspath(options.cwljob))
        job, _ = loader.resolve_ref(uri, checklinks=False)
    else:
        job = {}

    try:
        cwltool.pathmapper.adjustDirObjs(job, unsupportedInputCheck)
        cwltool.pathmapper.adjustFileObjs(job, unsupportedInputCheck)
    except cwltool.process.UnsupportedRequirement as e:
        logging.error(e)
        return 33

    cwltool.pathmapper.adjustDirObjs(job, pathToLoc)
    cwltool.pathmapper.adjustFileObjs(job, pathToLoc)

    if type(t) == int:
        return t

    fillInDefaults(t.tool["inputs"], job)

    if options.conformance_test:
        adjustFiles(job, lambda x: x.replace("file://", ""))
        stdout.write(json.dumps(
            cwltool.main.single_job_executor(t, job, basedir=options.basedir,
                                             tmpdir_prefix="tmp",
                                             conformance_test=True, use_container=use_container,
                                             preserve_environment=options.preserve_environment), indent=4))
        return 0

    if not options.basedir:
        options.basedir = os.path.dirname(os.path.abspath(options.cwljob or options.cwltool))

    outdir = options.outdir

    with Toil(options) as toil:
        def importDefault(tool):
            cwltool.pathmapper.adjustDirObjs(tool, locToPath)
            cwltool.pathmapper.adjustFileObjs(tool, locToPath)
            adjustFiles(tool, lambda x: "file://%s" % x if not urlparse.urlparse(x).scheme else x)
            adjustFiles(tool, functools.partial(writeFile, toil.importFile, {}, {}))
        t.visit(importDefault)

        if options.restart:
            outobj = toil.restart()
        else:
            basedir = os.path.dirname(os.path.abspath(options.cwljob or options.cwltool))
            builder = t._init_job(job, basedir=basedir, use_container=use_container)
            (wf1, wf2) = makeJob(t, {}, use_container=use_container,
                    preserve_environment=options.preserve_environment,
                    tmpdir=os.path.realpath(outdir), builder=builder)
            try:
                if isinstance(wf1, CWLWorkflow):
                    [unsupportedDefaultCheck(s.tool) for s in wf1.cwlwf.steps]
            except cwltool.process.UnsupportedRequirement as e:
                logging.error(e)
                return 33

            cwltool.pathmapper.adjustDirObjs(builder.job, locToPath)
            cwltool.pathmapper.adjustFileObjs(builder.job, locToPath)
            adjustFiles(builder.job, lambda x: "file://%s" % os.path.abspath(os.path.join(basedir, x))
                        if not urlparse.urlparse(x).scheme else x)
            cwltool.pathmapper.adjustDirObjs(builder.job, pathToLoc)
            cwltool.pathmapper.adjustFileObjs(builder.job, pathToLoc)
            cwltool.pathmapper.adjustFileObjs(builder.job, addFilePartRefs)
            adjustFiles(builder.job, functools.partial(writeFile, toil.importFile, {}, {}))
            wf1.cwljob = builder.job
            outobj = toil.start(wf1)

        outobj = resolve_indirect(outobj)

        try:
            adjustFilesWithSecondary(outobj, functools.partial(getFile, toil, outdir, index={}, existing={},
                                                               export=True, rename_collision=True))
            cwltool.pathmapper.adjustFileObjs(outobj, pathToLoc)
        except cwltool.process.UnsupportedRequirement as e:
            logging.error(e)
            return 33

        stdout.write(json.dumps(outobj, indent=4))

    return 0
