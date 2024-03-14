.. _debugging:

Toil Debugging
==============

Toil has a number of tools to assist in debugging.  Here we provide help in working through potential problems that a user might encounter in attempting to run a workflow.

Reading the Log
---------------

Usually, at the end of a failed Toil worklfow, Toil will reproduce the job logs for the jobs that failed. You can look at the end of your workflow log and use the job logs to identify which jobs are failing and why.

Finding Failed Jobs in the Jobstore 
-----------------------------------

The ``toil status`` command (:ref:`cli_status`) can be used with the ``--failed`` option to list all failed jobs in a Toil job store.

You can also use it with the ``--logs`` option to retrieve per-job logs from the job store, for failed jobs that left them. These logs might be useful for diagnosing and fixing the problem.

Running a Job Locally
---------------------

If you have a failing job's ID or name, you can reproduce its failure on your local machine with ``toil debug-job``. See :ref:`cli_debug_job`.

For example, say you have this WDL workflow in ``test.wdl``. This workflow **cannot succeed**, due to the typo in the echo command::

    version 1.0
    workflow test {
      call hello
    }
    task hello {
      input {
      }
      command <<<
        set -e
        echoo "Hello"
      >>>
      output {
      }
    }

You could try to run it with::

    toil-wdl-runner --jobStore ./store test.wdl

But it will fail.

If you want to reproduce the failure later, or on another machine, you can first find out what jobs failed with ``toil status``::

    toil status --failed --noAggStats ./store

This will produce something like:

    [2024-03-14T17:45:15-0400] [MainThread] [I] [toil.utils.toilStatus] Traversing the job graph gathering jobs. This may take a couple of minutes.
    Failed jobs:
    'WDLTaskJob' test.hello.command kind-WDLTaskJob/instance-r9u6_dcs v6

And we can see a failed job with the display name ``test.hello.command``, which describes the job's location in the WDL workflow as the command section of the ``hello`` task called from the ``test`` workflow. (If you are writing a Toil Python script, this is the job's ``displayName``.) We can then run that job again locally by name with::

    toil debug-job ./store test.hello.command

If there were multiple failed jobs with that name (perhaps because of a WDL scatter), we would need to select one by Toil job ID instead::

    toil debug-job ./store kind-WDLTaskJob/instance-r9u6_dcs

And if we know there's only one failed WDL task, we can just tell Toil to rerun the failed ``WDLTaskJob`` by Python class name::

    toil debug-job ./store WDLTaskJob

Any of these will run the job (including any containers) on the local machine, where its execution can be observed live or monitored with a debugger.
    

Introspecting the Job Store
---------------------------

Note: Currently these features are only implemented for use locally (single machine) with the fileJobStore.

To view what files currently reside in the jobstore, run the following command::

    $ toil debug-file file:path-to-jobstore-directory \
          --listFilesInJobStore

When run from the commandline, this should generate a file containing the contents of the job store (in addition to
displaying a series of log messages to the terminal).  This file is named "jobstore_files.txt" by default and will be
generated in the current working directory.

If one wishes to copy any of these files to a local directory, one can run for example::

    $ toil debug-file file:path-to-jobstore \
          --fetch overview.txt *.bam *.fastq \
          --localFilePath=/home/user/localpath

To fetch ``overview.txt``, and all ``.bam`` and ``.fastq`` files.  This can be used to recover previously used input and output
files for debugging or reuse in other workflows, or use in general debugging to ensure that certain outputs were imported
into the jobStore.

Stats and Status
----------------
See :ref:`cli_stats` and :ref:`cli_status` for more about gathering statistics about job success, runtime, and resource usage from workflows.

Using a Python debugger
-----------------------

If you execute a workflow using the :code:`--debugWorker` flag, or if you use ``toil debug-job``, Toil will run the job in the process you started from the command line. This means
you can either use `pdb <https://docs.python.org/3/library/pdb.html>`_, or an `IDE that supports debugging Python <https://wiki.python.org/moin/PythonDebuggingTools#IDEs_with_Debug_Capabilities>`_ to interact with the Python process as it runs your job. Note that the :code:`--debugWorker` flag will
only work with the :code:`single_machine` batch system (the default), and not any of the custom job schedulers.
