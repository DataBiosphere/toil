.. _cwl:

CWL in Toil
===========

The Common Workflow Language (CWL) is an emerging standard for writing workflows
that are portable across multiple workflow engines and platforms.
Toil has full support for the CWL v1.0.1 specification.

Running CWL Locally
-------------------

To run in local batch mode, provide the CWL file and the input object file::

    $ toil-cwl-runner example.cwl example-job.yml

For a simple example of CWL with Toil see :ref:`cwlquickstart`.

Running CWL in the Cloud
------------------------

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.

To run a CWL workflow in AWS with toil see :ref:`awscwl`.

.. _File literals: http://www.commonwl.org/v1.0/CommandLineTool.html#File
.. _Directory: http://www.commonwl.org/v1.0/CommandLineTool.html#Directory
.. _secondaryFiles: http://www.commonwl.org/v1.0/CommandLineTool.html#CommandInputParameter
.. _InitialWorkDirRequirement: http://www.commonwl.org/v1.0/CommandLineTool.html#InitialWorkDirRequirement

Running CWL within Toil Scripts
------------------------------------

A CWL workflow can be run indirectly in a native Toil script. However, this is not the :ref:`standard <cwl>` way to run
CWL workflows with Toil and doing so comes at the cost of job efficency. For some use cases, such as running one process on
multiple files, it may be useful. For example, if you want to run a CWL workflow with 3 YML files specifying different
samples inputs, it could look something like::

    from toil.job import Job
    from toil.common import Toil
    import subprocess
    import os


    def initialize_jobs(job):
        job.fileStore.logToMaster('initialize_jobs')


    def runQC(job, cwl_file, cwl_filename, yml_file, yml_filename, outputs_dir, output_num):
        job.fileStore.logToMaster("runQC")
        tempDir = job.fileStore.getLocalTempDir()

        cwl = job.fileStore.readGlobalFile(cwl_file, userPath=os.path.join(tempDir, cwl_filename))
        yml = job.fileStore.readGlobalFile(yml_file, userPath=os.path.join(tempDir, yml_filename))

        subprocess.check_call(["cwltool", cwl, yml])

        output_filename = "output.txt"
        output_file = job.fileStore.writeGlobalFile(output_filename)
        job.fileStore.readGlobalFile(output_file, userPath=os.path.join(outputs_dir, "sample_" + output_num + "_" + output_filename))
        return output_file


    if __name__ == "__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        with Toil(options) as toil:

            # specify the folder where the cwl and yml files live
            inputs_dir = "/tmp/"
            # specify where you wish the outputs to be written
            outputs_dir = "/tmp/"

            job0 = Job.wrapJobFn(initialize_jobs)

            cwl_filename = "hello.cwl"
            cwl_file = toil.importFile("file://" + os.path.join(inputs_dir, cwl_filename))

            # add list of yml config inputs here or import and construct from file
            yml_files = ["hello1.yml", "hello2.yml", "hello3.yml"]
            i = 0
            for yml in yml_files:
                i = i + 1
                yml_file = toil.importFile("file://" + os.path.join(inputs_dir, yml))
                yml_filename = yml
                job = Job.wrapJobFn(runQC, cwl_file, cwl_filename, yml_file, yml_filename, outputs_dir, output_num=str(i))
                job0.addChild(job)

            toil.start(job0)

