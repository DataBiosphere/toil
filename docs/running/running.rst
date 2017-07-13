.. _running:

Running Toil workflows
======================

This section describes how to run Toil workflows.

.. _quickstart:

Toil Quickstart
---------------

A Toil workflow can be run with just three steps.
 
1. Install Toil (see :ref:`installation-ref`)

2. Copy and paste the following code block into ``HelloWorld.py``:

   .. code-block:: python

      from toil.job import Job

      def helloWorld(message, memory="2G", cores=2, disk="3G"):
          return "Hello, world!, here's a message: %s" % message

      j = Job.wrapFn(helloWorld, "You did it!")

      if __name__=="__main__":
          parser = Job.Runner.getDefaultArgumentParser()
          options = parser.parse_args()
          print Job.Runner.startToil(j, options) #Prints Hello, world!, ...

3. Specify a job store and run the workflow like so::

       $ python HelloWorld.py file:my-job-store

Now you have run Toil on the ``singleMachine`` batch system (the default) using
the ``file`` job store. The job store is a place where intermediate files are
written to during the workflow's execution. The ``file`` job store is a job
store that uses the files and directories on a locally-attached filesystem - in
this case, a directory called ``my-job-store`` in the directory that
``HelloWorld.py`` is run from. (Read more about :ref:`jobStoreInterface`.)

Run ``python HelloWorld.py --help`` to see a complete list of available options.

For something beyond a "Hello, world!" example, refer to :ref:`runningDetail`.


CWL Quickstart
--------------

The `Common Workflow Language`_ (CWL) is an emerging standard for writing
workflows that are portable across multiple workflow engines and platforms. To
run workflows written using CWL, first ensure that Toil is installed with the
``cwl`` extra (see :ref:`extras`). This will install the ``cwl-runner`` and
``cwltoil`` executables (these are identical - ``cwl-runner`` is the portable
name for the default system CWL runner).

To learn more about CWL, see the `CWL User Guide`_. Toil has nearly full
support for the stable v1.0 specification, only lacking the following features:

- `Directory`_ inputs and outputs in pipelines. Currently, directory inputs must
  be enumerated as Files.
- `File literals`_ that specify only ``contents`` to a File without an explicit
  file name.
- Writable `InitialWorkDirRequirement`_ objects. Standard readable inputs do work.
- Complex file inputs – from ExpressionTool or a default value, both of which do
  not yet get cleanly staged into Toil file management.

To run in local batch mode, provide the CWL file and the input object file::

    $ cwltoil example.cwl example-job.yml

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.

.. _File literals: http://www.commonwl.org/v1.0/CommandLineTool.html#File
.. _Directory: http://www.commonwl.org/v1.0/CommandLineTool.html#Directory
.. _secondaryFiles: http://www.commonwl.org/v1.0/CommandLineTool.html#CommandInputParameter
.. _CWL User Guide: http://www.commonwl.org/v1.0/UserGuide.html
.. _InitialWorkDirRequirement: http://www.commonwl.org/v1.0/CommandLineTool.html#InitialWorkDirRequirement


.. _runningDetail:


A real-world example
--------------------

For a more detailed example and explanation, we've developed a sample pipeline
that merge-sorts a temporary file.

1. Download :download:`the example code <toil-sort-example.py>`.

2. Run it with the default settings::

      $ python toil-sort-example.py file:jobStore

3. Run with custom options::

      $ python toil-sort-example.py file:jobStore --num-lines=5000 --line-length=10 --workDir=/tmp/

The ``if __name__ == '__main__'`` boilerplate is required to enable Toil to
import the job functions defined in the script into the context of a Toil
*worker* process. By invoking the script you created the *leader process*. A
worker process is a separate process whose sole purpose is to host the
execution of one or more jobs defined in that script. When using the
single-machine batch system (the default), the worker processes will be running
on the same machine as the leader process. With full-fledged batch systems like
Mesos the worker processes will typically be started on separate machines. The
boilerplate ensures that the pipeline is only started once–on the leader–but
not when its job functions are imported and executed on the individual workers.

Typing ``python toil-sort-example.py --help`` will show the complete list of
arguments for the workflow which includes both Toil's and ones defined inside
``toil-sort-example.py``. A complete explanation of Toil's arguments can be
found in :ref:`commandRef`.

Logging
~~~~~~~

By default, Toil logs a lot of information related to the current environment
in addition to messages from the batch system and jobs. This can be configured
with the ``--logLevel`` flag. For example, to only log ``CRITICAL`` level
messages to the screen::

   $ python toil-sort-examply.py file:jobStore --logLevel=critical

This hides most of the information we get from the Toil run. For more detail,
we can run the pipeline with ``--logLevel=debug`` to see a comprehensive
output. For more information, see :ref:`loggingRef`.


Error handling and resuming pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Toil, you can recover gracefully from a bug in your pipeline without losing
any progress from successfully-completed jobs. To demonstrate this, let's add
a bug to our example code to see how Toil handles a failure and how we can
resume a pipeline after that happens. Add a bad assertion to line 30 of the
example (the first line of ``down()``):

.. code-block:: python

   def down(job, input_file_store_id, n, down_checkpoints):
       ...
       assert 1 == 2, "Test error!"

When we run the pipeline, Toil will show a detailed failure log with a traceback::

   $ python toil-sort-example.py file:jobStore
   ...
   ---TOIL WORKER OUTPUT LOG---
   ...
   m/j/jobonrSMP    Traceback (most recent call last):
   m/j/jobonrSMP      File "toil/src/toil/worker.py", line 340, in main
   m/j/jobonrSMP        job._runner(jobGraph=jobGraph, jobStore=jobStore, fileStore=fileStore)
   m/j/jobonrSMP      File "toil/src/toil/job.py", line 1270, in _runner
   m/j/jobonrSMP        returnValues = self._run(jobGraph, fileStore)
   m/j/jobonrSMP      File "toil/src/toil/job.py", line 1217, in _run
   m/j/jobonrSMP        return self.run(fileStore)
   m/j/jobonrSMP      File "toil/src/toil/job.py", line 1383, in run
   m/j/jobonrSMP        rValue = userFunction(*((self,) + tuple(self._args)), **self._kwargs)
   m/j/jobonrSMP      File "toil/example.py", line 30, in down
   m/j/jobonrSMP        assert 1 == 2, "Test error!"
   m/j/jobonrSMP    AssertionError: Test error!

If we try and run the pipeline again, Toil will give us an error message saying
that a job store of the same name already exists. By default, in the event of a
failure, the job store is preserved so that it can be restarted from its last
successful job. We can restart the pipeline by running::

   $ python toil-sort-example.py file:jobStore --restart

We can also change the number of times Toil will attempt to retry a failed job::

   $ python toil-sort-example.py --retryCount 2 --restart

You'll now see Toil attempt to rerun the failed job until it runs out of tries.
``--retryCount`` is useful for non-systemic errors, like downloading a file that
may experience a sporadic interruption, or some other non-deterministic failure.

To successfully restart our pipeline, we can edit our script to comment out
line 30, or remove it, and then run

::

   $ python toil-sort-example.py --restart

The pipeline will run successfully, and the job store will be removed on the
pipeline's completion.


Collecting statistics
~~~~~~~~~~~~~~~~~~~~~

A Toil pipeline can be run with the ``--stats`` flag to allows collection of
statistics::

   $ python toil-sort-example.py --stats

Once the pipeline finishes, the job store will be left behind, allowing us to
get information on the total runtime and stats pertaining to each job function::

   $ toil stats file:jobStore
   ...
   Batch System: singleMachine
   Default Cores: 1  Default Memory: 2097152K
   ...

Once we're done, we can clean up the job store by running

::

   $ toil clean file:jobStore
