.. _cwl:

CWL in Toil
===========

The Common Workflow Language (CWL) is an emerging standard for writing workflows
that are portable across multiple workflow engines and platforms.
Toil has full support for the CWL v1.0, v1.1, and v1.2 standards.

Running CWL Locally
-------------------

The `toil-cwl-runner` command provides cwl-parsing functionality using cwltool, and leverages the job-scheduling and
batch system support of Toil.

To run in local batch mode, provide the CWL file and the input object file::

    $ toil-cwl-runner example.cwl example-job.yml

For a simple example of CWL with Toil see :ref:`cwlquickstart`.

Note for macOS + Docker + Toil
++++++++++++++++++++++++++++++

When invoking CWL documents that make use of Docker containers if you see errors that
look like
::

    docker: Error response from daemon: Mounts denied:
    The paths /var/...tmp are not shared from OS X and are not known to Docker.

you may need to add
::

    export TMPDIR=/tmp/docker_tmp

either in your startup file (``.bashrc``) or add it manually in your shell before invoking
toil.


Detailed Usage Instructions
---------------------------

Help information can be found by using this toil command:
::

    $ toil-cwl-runner -h

A more detailed example shows how we can specify both Toil and cwltool arguments for our workflow:
::

    $ toil-cwl-runner \
        --singularity \
        --jobStore my_jobStore \
        --batchSystem lsf \
        --workDir `pwd` \
        --outdir `pwd` \
        --logFile cwltoil.log \
        --writeLogs `pwd` \
        --logLevel DEBUG \
        --retryCount 2 \
        --maxLogFileSize 20000000000 \
        --stats \
        standard_bam_processing.cwl \
        inputs.yaml

In this example, we set the following options, which are all passed to Toil:

``--singularity``: Specifies that all jobs with Docker format containers
specified should be run using the Singularity container engine instead of the
Docker container engine.

``--jobStore``: Path to a folder which doesn't exist yet, which will contain the
Toil jobstore and all related job-tracking information.

``--batchSystem``: Use the specified HPC or Cloud-based cluster platform.

``--workDir``: The directory where all temporary files will be created for the
workflow. A subdirectory of this will be set as the ``$TMPDIR`` environment
variable and this subdirectory can be referenced using the CWL parameter
reference ``$(runtime.tmpdir)`` in CWL tools and workflows.

``--outdir``: Directory where final ``File`` and ``Directory`` outputs will be
written. References to these and other output types will be in the JSON object
printed to the stdout stream after workflow execution.

``--logFile``: Path to the main logfile with logs from all jobs.

``--writeLogs``: Directory where all job logs will be stored.

``--retryCount``: How many times to retry each Toil job.

``--maxLogFileSize``: Logs that get larger than this value will be truncated.

``--stats``: Save resources usages in json files that can be collected with the
``toil stats`` command after the workflow is done.

``--disable-streaming``: Does not allow streaming of input files. This is enabled
by default for files marked with ``streamable`` flag True and only for remote files
when the jobStore is not on local machine.

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
CWL workflows with Toil and doing so comes at the cost of job efficiency. For some use cases, such as running one process on
multiple files, it may be useful. For example, if you want to run a CWL workflow with 3 YML files specifying different
samples inputs, it could look something like:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_cwlexample.py

Running CWL workflows with InplaceUpdateRequirement
---------------------------------------------------

Some CWL workflows use the ``InplaceUpdateRequirement`` feature, which requires
that operations on files have visible side effects that Toil's file store
cannot support. If you need to run a workflow like this, you can make sure that
all of your worker nodes have a shared filesystem, and use the
``--bypass-file-store`` option to ``toil-cwl-runner``. This will make it leave
all CWL intermediate files on disk and share them between jobs using file
paths, instead of storing them in the file store and downloading them when jobs
need them.

Toil & CWL Tips
---------------

**See logs for just one job by using the full log file**

This requires knowing the job's toil-generated ID, which can be found in the log files.
::

    cat cwltoil.log | grep jobVM1fIs

**Grep for full tool commands from toil logs**

This gives you a more concise view of the commands being run (note that this information is only available from
Toil when running with `--logDebug`).
::

    pcregrep -M "\[job .*\.cwl.*$\n(.*        .*$\n)*" cwltoil.log
    #         ^allows for multiline matching

**Find Bams that have been generated for specific step while pipeline is running:**
::

    find . | grep -P '^./out_tmpdir.*_MD\.bam$'

**See what jobs have been run**
::

    cat log/cwltoil.log | grep -oP "\[job .*.cwl\]" | sort | uniq

or:
::

    cat log/cwltoil.log | grep -i "issued job"

**Get status of a workflow**
::

    $ toil status /home/johnsoni/TEST_RUNS_3/TEST_run/tmp/jobstore-09ae0acc-c800-11e8-9d09-70106fb1697e
    <hostname> 2018-10-04 15:01:44,184 MainThread INFO toil.lib.bioio: Root logger is at level 'INFO', 'toil' logger at level 'INFO'.
    <hostname> 2018-10-04 15:01:44,185 MainThread INFO toil.utils.toilStatus: Parsed arguments
    <hostname> 2018-10-04 15:01:47,081 MainThread INFO toil.utils.toilStatus: Traversing the job graph gathering jobs. This may take a couple of minutes.

    Of the 286 jobs considered, there are 179 jobs with children, 107 jobs ready to run, 0 zombie jobs, 0 jobs with services, 0 services, and 0 jobs with log files currently in file:/home/user/jobstore-09ae0acc-c800-11e8-9d09-70106fb1697e.

**Toil Stats**

You can get run statistics broken down by CWL file. This only works once the workflow is finished:
::

    $ toil stats /path/to/jobstore

The output will contain CPU, memory, and walltime information for all CWL job types:
::

    <hostname> 2018-10-15 12:06:19,003 MainThread INFO toil.lib.bioio: Root logger is at level 'INFO', 'toil' logger at level 'INFO'.
    <hostname> 2018-10-15 12:06:19,004 MainThread INFO toil.utils.toilStats: Parsed arguments
    <hostname> 2018-10-15 12:06:19,004 MainThread INFO toil.utils.toilStats: Checking if we have files for toil
    <hostname> 2018-10-15 12:06:19,004 MainThread INFO toil.utils.toilStats: Checked arguments
    Batch System: lsf
    Default Cores: 1  Default Memory: 10485760K
    Max Cores: 9.22337e+18
    Total Clock: 106608.01  Total Runtime: 86634.11
    Worker
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
         1659 |     0.00    0.80  264.87 12595.59 439424.40 |     0.00    0.46   449.05 42240.74 744968.80 |  -35336.69     0.16   -184.17  4230.65 -305544.39 |      48K    223K   1020K  40235K 1692300K
    Job
     Worker Jobs  |     min    med    ave    max
                  |    1077   1077   1077   1077
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
         1077 |     0.04    1.18  407.06 12593.43 438404.73 |     0.01    0.28   691.17 42240.35 744394.14 |  -35336.83     0.27   -284.11  4230.49 -305989.41 |     135K    268K   1633K  40235K 1759734K
     ResolveIndirect
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
          205 |     0.04    0.07    0.16     2.29     31.95 |     0.01    0.02     0.02     0.14      3.60 |       0.02     0.05      0.14     2.28      28.35 |     190K    266K    256K    314K   52487K
     CWLGather
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
           40 |     0.05    0.17    0.29     1.90     11.62 |     0.01    0.02     0.02     0.05      0.80 |       0.03     0.14      0.27     1.88      10.82 |     188K    265K    250K    316K   10039K
     CWLWorkflow
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
          205 |     0.09    0.40    0.98    13.70    200.82 |     0.04    0.15     0.16     1.08     31.78 |       0.04     0.26      0.82    12.62     169.04 |     190K    270K    257K    316K   52826K
     file:///home/johnsoni/pipeline_0.0.39/ACCESS-Pipeline/cwl_tools/expression_tools/group_waltz_files.cwl
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
           99 |     0.29    0.49    0.59     2.50     58.11 |     0.14    0.26     0.29     1.04     28.95 |       0.14     0.22      0.29     1.48      29.16 |     135K    135K    135K    136K   13459K
     file:///home/johnsoni/pipeline_0.0.39/ACCESS-Pipeline/cwl_tools/expression_tools/make_sample_output_dirs.cwl
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
           11 |     0.34    0.52    0.74     2.63      8.18 |     0.20    0.30     0.41     1.17      4.54 |       0.14     0.20      0.33     1.45       3.65 |     136K    136K    136K    136K    1496K
     file:///home/johnsoni/pipeline_0.0.39/ACCESS-Pipeline/cwl_tools/expression_tools/consolidate_files.cwl
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
            8 |     0.31    0.59    0.71     1.80      5.69 |     0.18    0.35     0.37     0.63      2.94 |       0.13     0.27      0.34     1.17       2.75 |     136K    136K    136K    136K    1091K
     file:///home/johnsoni/pipeline_0.0.39/ACCESS-Pipeline/cwl_tools/bwa-mem/bwa-mem.cwl
        Count |                                       Time* |                                        Clock |                                              Wait |                                    Memory
            n |      min    med*     ave      max     total |      min     med      ave      max     total |        min      med       ave      max      total |      min     med     ave     max    total
           22 |   895.76 3098.13 3587.34 12593.43  78921.51 |  2127.02 7910.31  8123.06 16959.13 178707.34 |  -11049.84 -3827.96  -4535.72    19.49  -99785.83 |    5659K   5950K   5854K   6128K  128807K

**Understanding toil log files**

There is a `worker_log.txt` file for each job, this file is written to while the job is running, and deleted after the job finishes. The contents are printed to the main log file and transferred to a log file in the `--logDir` folder once the job is completed successfully.

The new log file will be named something like:
::

    file:<path to cwl tool>.cwl_<job ID>.log

    file:---home-johnsoni-pipeline_1.1.14-ACCESS--Pipeline-cwl_tools-marianas-ProcessLoopUMIFastq.cwl_I-O-jobfGsQQw000.log

This is the toil job command with spaces replaced by dashes.
