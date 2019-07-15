.. _debugging:

Toil Debugging
==============

Toil has a number of tools to assist in debugging.  Here we provide help in working through potential problems that a user might encounter in attempting to run a workflow.

Introspecting the Jobstore
--------------------------

Note: Currently these features are only implemented for use locally (single machine) with the fileJobStore.

To view what files currently reside in the jobstore, run the following command::

    $ toil debug-file file:path-to-jobstore-directory --listFilesInJobStore

When run from the commandline, this should generate a file containing the contents of the job store (in addition to
displaying a series of log messages to the terminal).  This file is named "jobstore_files.txt" by default and will be
generated in the current working directory.

If one wishes to copy any of these files to a local directory, one can run for example::

    $ toil debug-file file:path-to-jobstore --fetch overview.txt *.bam *.fastq --localFilePath=/home/user/localpath

To fetch ``overview.txt``, and all ``.bam`` and ``.fastq`` files.  This can be used to recover previously used input and output
files for debugging or reuse in other workflows, or use in general debugging to ensure that certain outputs were imported
into the jobStore.

Stats and Status
----------------
See :ref:`cli_status` for more about gathering statistics about job success, runtime, and resource usage from workflows.

Using a Python debugger
-----------------------

If you execute a workflow using the :code:`--debugWorker` flag, Toil will not fork in order to run jobs, which means
you can either use `pdb <https://docs.python.org/3/library/pdb.html>`_, or an `IDE that supports debugging Python <https://wiki.python.org/moin/PythonDebuggingTools#IDEs_with_Debug_Capabilities>`_ as you would normally. Note that the :code:`--debugWorker` flag will
only work with the :code:`singleMachine` batch system (the default), and not any of the custom job schedulers.