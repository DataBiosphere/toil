from toil.job import Job

align = Job.wrapJobFn(dockerCall,
                      tool='quay.io/ucsc_cgl/bwa',
                      workDir=job.tempDir,
                      parameters=['index', '/data/reference.fa'])

if __name__=="__main__":
    options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
       toil.start(align)
