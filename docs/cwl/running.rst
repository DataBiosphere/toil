.. _runCwl:

Running CWL Workflows
=====================

The `toil-cwl-runner` command provides CWL parsing functionality using cwltool, and leverages the job-scheduling and
batch system support of Toil. You can use it to run CWL workflows locally or in the cloud.

Running CWL Locally
-------------------

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

``--logFile``: Path to the main logfile.

``--writeLogs``: Directory where job logs will be stored. At ``DEBUG`` log level, this will contain logs for each Toil job run, as well as ``stdout``/``stderr`` logs for each CWL ``CommandLineTool`` that didn't use the ``stdout``/``stderr`` directives to redirect output.

``--retryCount``: How many times to retry each Toil job.

``--maxLogFileSize``: Logs that get larger than this value will be truncated.

``--stats``: Save resources usages in json files that can be collected with the
``toil stats`` command after the workflow is done.

Extra Toil CWL Options
++++++++++++++++++++++

Besides the normal Toil options and the options supported by cwltool, toil-cwl-runner adds some of its own options:

  --bypass-file-store   Do not use Toil's file store system and assume all paths are accessible in place from all nodes. This can avoid possibly-redundant file copies into Toil's job store storage, and is required for CWL's ``InplaceUpdateRequirement``. But, it allows a failed job execution to leave behind a partially-modified state, which means that a restarted workflow might not work correctly.
  --reference-inputs    Do not copy remote inputs into Toil's file store and assume they are accessible in place from all nodes.
  --disable-streaming   Do not allow streaming of job input files. By default, files marked with ``streamable`` True are streamed from remote job stores.
  --cwl-default-ram     Apply CWL specification default ramMin.
  --no-cwl-default-ram  Do not apply CWL specification default ramMin, so that Toil --defaultMemory applies.


Running CWL in the Cloud
------------------------

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.

To run a CWL workflow in AWS with toil see :ref:`awscwl`.

.. _File literals: http://www.commonwl.org/v1.0/CommandLineTool.html#File
.. _Directory: http://www.commonwl.org/v1.0/CommandLineTool.html#Directory
.. _secondaryFiles: http://www.commonwl.org/v1.0/CommandLineTool.html#CommandInputParameter
.. _InitialWorkDirRequirement: http://www.commonwl.org/v1.0/CommandLineTool.html#InitialWorkDirRequirement

.. _inplaceupdaterequirement:

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

This will report resource usage information for all the CWL jobs executed by the workflow.

See :ref:`cli_stats` for an explanation of what the different fields mean.

**Understanding toil log files**

There is a `worker_log.txt` file for each Toil job. This file is written to while the job is running, and uploaded at the end if the job finishes or if running at debug log level. If uploaded, the contents are printed to the main log file and transferred to a log file in the `--logDir` folder.

The new log file will be named something like:
::

    CWLJob_<name of the CWL job>_<attempt number>.log

Standard output/error files will be named like:
::

    <name of the CWL job>.stdout_<attempt number>.log

If you have a workflow ``revsort.cwl`` which has a step ``rev`` which calls the tool ``revtool.cwl``, the CWL job name ends up being all those parts strung together with ``.``: ``revsort.cwl.rev.revtool.cwl``.
