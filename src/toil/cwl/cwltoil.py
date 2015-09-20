#
# Implement support for Common Workflow Language (CWL) in Toil.
#

from toil.job import Job
from argparse import ArgumentParser
import cwltool.main
import cwltool.workflow
import schema_salad.ref_resolver
import os
import tempfile
import json

def shortname(n):
    """Trim the leading namespace to get just the final name part of a parameter."""
    return n.split("#")[-1].split("/")[-1].split(".")[-1]

def adjustFiles(rec, op):
    """Apply a mapping function to each File path in the object `rec`."""

    if isinstance(rec, dict):
        if rec.get("class") == "File":
            rec["path"] = op(rec["path"])
        for d in rec:
            adjustFiles(rec[d], op)
    if isinstance(rec, list):
        for d in rec:
            adjustFiles(d, op)


class StageJob(Job):
    """File staging job to put local files into the global file store.

    This currently will break if you try and run this on a cluster because the
    main() method can't stage files before Job.Runner.startToil(), and the
    staging job could run on a compute node where it doesn't have direct access
    to the input files of the head node.

    """

    def __init__(self, cwlwf, cwljob, basedir):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.basedir = basedir

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}
        builder = self.cwlwf._init_job(cwljob, self.basedir)
        adjustFiles(builder.job, lambda x: (fileStore.writeGlobalFile(x), x.split('/')[-1]))
        print
        return {k: (k, builder.job) for k, v in builder.job.items()}


class FinalJob(Job):
    """Wrap-up job to write output JSON and copy output files from global file
    store to current working directory.

    This currently will break if you try and run this on a cluster because the
    main() method can't access files produced by Job.Runner.startToil(), and
    the staging job could run on a compute node where it doesn't have direct
    access to the output directory of the head node.

    """

    def __init__(self, cwljob, outdir):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwljob = cwljob
        self.outdir = outdir

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}
        adjustFiles(cwljob, lambda x: fileStore.readGlobalFile(x[0], os.path.join(self.outdir, x[1])))
        with open(os.path.join(self.outdir, "cwl.output.json"), "w") as f:
            json.dump(cwljob, f, indent=4)
        return True


class CWLJob(Job):
    """Execute a CWL tool wrapper."""

    def __init__(self, cwltool, cwljob):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwltool = cwltool
        self.cwljob = cwljob

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}

        inpdir = os.path.join(fileStore.getLocalTempDir(), "inp")
        outdir = os.path.join(fileStore.getLocalTempDir(), "out")
        tmpdir = os.path.join(fileStore.getLocalTempDir(), "tmp")
        os.mkdir(inpdir)
        os.mkdir(outdir)
        os.mkdir(tmpdir)

        # Copy input files out of the global file store.
        print cwljob

        adjustFiles(cwljob, lambda x: fileStore.readGlobalFile(x[0], os.path.join(inpdir, x[1])))

        output = cwltool.main.single_job_executor(self.cwltool, cwljob,
                                                  os.getcwd(), None,
                                                  outdir=outdir,
                                                  tmpdir=tmpdir)

        # Copy output files into the global file store.
        adjustFiles(output, lambda x: (fileStore.writeGlobalFile(x), x.split('/')[-1]))

        return output


class SelfJob(object):
    """Fake job object to facilitate implementation of CWLWorkflow.run()"""

    def __init__(self, j, v):
        self.j = j
        self.v = v
        self._children = j._children

    def rv(self):
        return self.v

    def addChild(self, c):
        self.j.addChild(c)


class CWLWorkflow(Job):
    """Traverse a CWL workflow graph and schedule a Toil job graph."""

    def __init__(self, cwlwf, cwljob):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwlwf = cwlwf
        self.cwljob = cwljob

    def run(self, fileStore):
        # The job object passed into CWLJob and CWLWorkflow
        # is a dict mapping to tuple of (key, dict)
        # the final dict is derived by evaluating each
        # tuple looking up the key in the supplied dict.
        #
        # This is necessary because Toil jobs return a single value (a dict)
        # but CWL permits steps to have multiple output parameters that may
        # feed into multiple other steps.  This transformation maps the key in the
        # output object to the correct key of the input object.
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}

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
                        if "source" in inp and inp["source"] not in promises:
                            stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        jobobj = {}

                        # TODO: Handle multiple inbound links
                        # TODO: Handle scatter/gather
                        # (both are discussed in section 5.1.2 in CWL spec draft-2)

                        for inp in step.tool["inputs"]:
                            if "source" in inp:
                                jobobj[shortname(inp["id"])] = (shortname(inp["source"]), promises[inp["source"]].rv())
                            elif "default" in inp:
                                jobobj[shortname(inp["id"])] = ("default", {"default": inp["default"]})

                        if step.embedded_tool.tool["class"] == "Workflow":
                            job = CWLWorkflow(step.embedded_tool, jobobj)
                        else:
                            job = CWLJob(step.embedded_tool, jobobj)

                        jobs[step.tool["id"]] = job

                        for inp in step.tool["inputs"]:
                            if "source" in inp:
                                if job not in promises[inp["source"]]._children:
                                    promises[inp["source"]].addChild(job)

                        for out in step.tool["outputs"]:
                            promises[out["id"]] = job

                for inp in step.tool["inputs"]:
                    if "source" in inp:
                        if inp["source"] not in promises:
                            alloutputs_fufilled = False

            for out in self.cwlwf.tool["outputs"]:
                if "source" in out:
                    if out["source"] not in promises:
                        alloutputs_fufilled = False

        outobj = {}
        for out in self.cwlwf.tool["outputs"]:
            outobj[shortname(out["id"])] = (shortname(out["source"]), promises[out["source"]].rv())

        return outobj

supportedProcessRequirements = ["DockerRequirement",
                                "ExpressionEngineRequirement",
                                "SchemaDefRequirement",
                                "EnvVarRequirement",
                                "CreateFileRequirement",
                                "SubworkflowFeatureRequirement"]

def checkRequirements(rec):
    if isinstance(rec, dict):
        if "requirements" in rec:
            for r in rec["requirements"]:
                if r["class"] not in supportedProcessRequirements:
                    raise Exception("Unsupported requirement %s" % r["class"])
        for d in rec:
            checkRequirements(rec[d])
    if isinstance(rec, list):
        for d in rec:
            checkRequirements(d)


def main():
    parser = ArgumentParser()
    Job.Runner.addToilOptions(parser)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", type=str)

    # TODO: support cwl-runner standard CLI interface (cwl-runner workflow.cwl jobinput.json)
    # requires selection of default toil jobStore instead of requiring as 1st command line item.
    #
    # TODO: support cwltest standard CLI interface to support conformance testing cwltoil
    # (see cwltool/cwltest.py)
    options = parser.parse_args()

    uri = "file://" + os.path.abspath(options.cwljob)
    loader = schema_salad.ref_resolver.Loader({
        "@base": uri,
        "path": {
            "@type": "@id"
        }
    })
    job, _ = loader.resolve_ref(uri)

    adjustFiles(job, lambda x: x.replace("file://", ""))

    t = cwltool.main.load_tool(options.cwltool, False, False, cwltool.workflow.defaultMakeTool, True)

    checkRequirements(t.tool)

    jobobj = {}
    for inp in t.tool["inputs"]:
        if shortname(inp["id"]) in job:
            pass
        elif shortname(inp["id"]) not in job and "default" in inp:
            job[shortname(inp["id"])] = inp["default"]
        else:
            raise Exception("Missing inputs `%s`" % shortname(inp["id"]))

        jobobj[shortname(inp["id"])] = (shortname(inp["id"]), job)

    if type(t) == int:
        return t

    staging = StageJob(t, jobobj, os.path.dirname(os.path.abspath(options.cwljob)))

    if t.tool["class"] == "Workflow":
        wf = CWLWorkflow(t, staging.rv())
    else:
        wf = CWLJob(t, staging.rv())

    staging.addFollowOn(wf)
    wf.addFollowOn(FinalJob(wf.rv(), os.getcwd()))

    Job.Runner.startToil(staging,  options)


if __name__=="__main__":
    main()
