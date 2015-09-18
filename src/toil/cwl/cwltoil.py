from toil.job import Job
from argparse import ArgumentParser
import cwltool.main
import cwltool.workflow
import schema_salad.ref_resolver
import os

# class HelloWorld(Job):
#     def __init__(self):
#         Job.__init__(self,  memory=100000, cores=2, disk=20000)
#     def run(self, fileStore):
#         fileID = self.addChildJobFn(childFn, cores=1, memory="1M", disk="10M").rv()
#         self.addFollowOn(FollowOn(fileID))

# def childFn(job):
#     with job.fileStore.writeGlobalFileStream() as (fH, fileID):
#         fH.write("Hello, World!")
#         return fileID

# class FollowOn(Job):
#     def __init__(sel):
#         Job.__init__(self)

#     def run(self, fileStore):
#         tempDir = fileStore.getLocalTempDir()
#         tempFilePath = "/".join([tempDir,"LocalCopy"])
#         with fileStore.readGlobalFileStream(self.fileId) as globalFile:
#             with open(tempFilePath, "w") as localFile:
#                 localFile.write(globalFile.read())

def shortname(n):
    return n.split("#")[-1].split("/")[-1].split(".")[-1]

def adjustFiles(rec, op):
    if isinstance(rec, dict):
        if rec.get("class") == "File":
            rec["path"] = op(rec["path"])
        else:
            for d in rec:
                adjustFiles(rec[d], op)
    if isinstance(rec, list):
        for d in rec:
            adjustFiles(d, op)

class StageJob(Job):
    def __init__(self, cwljob):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwljob = cwljob

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}
        adjustFiles(cwljob, lambda x: fileStore.writeGlobalFile(x))
        return {k: (k, cwljob) for k, v in cwljob.items()}


class CWLJob(Job):
    def __init__(self, cwltool, cwljob):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwltool = cwltool
        self.cwljob = cwljob

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}

        adjustFiles(cwljob, lambda x: fileStore.readGlobalFile(x))

        output = cwltool.main.single_job_executor(self.cwltool, cwljob,
                                                  os.getcwd(), None,
                                                  outdir=os.path.join(fileStore.getLocalTempDir(), "out"),
                                                  tmpdir=os.path.join(fileStore.getLocalTempDir(), "tmp"))

        adjustFiles(output, lambda x: fileStore.writeGlobalFile(x))

        return output

class SelfJob(object):
    def __init__(self, j, v):
        self.j = j
        self.v = v

    def rv(self):
        return self.v

    def addChild(self, c):
        self.j.addChild(c)

def blub(fileStore):
    pass

class CWLWorkflow(Job):
    def __init__(self, cwlwf, cwljob):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
        self.cwlwf = cwlwf
        self.cwljob = cwljob

    def run(self, fileStore):
        cwljob = {k: v[1][v[0]] for k, v in self.cwljob.items()}
        promises = {}
        jobs = {}

        for inp in self.cwlwf.tool["inputs"]:
            promises[inp["id"]] = SelfJob(self, cwljob)

        fufilled = False
        while not fufilled:
            for step in self.cwlwf.steps:
                if step.tool["id"] not in jobs:
                    fufilled = True
                    for inp in step.tool["inputs"]:
                        if inp["source"] not in promises:
                            fufilled = False
                    if fufilled:
                        jobobj = {}
                        for inp in step.tool["inputs"]:
                            jobobj[shortname(inp["id"])] = (shortname(inp["source"]), promises[inp["source"]].rv())

                        job = CWLJob(step.embedded_tool, jobobj)
                        jobs[step.tool["id"]] = job

                        for inp in step.tool["inputs"]:
                            promises[inp["source"]].addChild(job)

                        for out in step.tool["outputs"]:
                            promises[out["id"]] = job

            fufilled = True
            for out in step.tool["outputs"]:
                if "source" in out:
                    if out["source"] not in promises:
                        fufilled = False

        self.addFollowOn(Job.wrapFn(blub, None))

        return True


def main():
    parser = ArgumentParser()
    Job.Runner.addToilOptions(parser)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", type=str)
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

    staging = StageJob(jobobj)

    if t.tool["class"] == "Workflow":
        wf = CWLWorkflow(t, staging.rv())
    else:
        wf = CWLJob(t, staging.rv())

    staging.addFollowOn(wf)

    Job.Runner.startToil(staging,  options)


if __name__=="__main__":
    main()
