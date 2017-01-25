.. highlight:: console

.. _running:

Running Toil workflows
======================

.. _quickstart:

Quickstart: A simple workflow
-----------------------------

Starting with Python, a Toil workflow can be run with just three steps.

1. Install Toil (see :ref:`installation-ref`)::

      $ pip install toil

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


Running CWL workflows
---------------------

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
- `InitialWorkDirRequirement`_ to create files together within a specific work
  directory. Collecting associated files using `secondaryFiles`_ is a good
  workaround.
- `File literals`_ that specify only ``contents`` to a File without an explicit
  file name.
- Complex file inputs – from ExpressionTool or a default value, both of which do
  not yet get cleanly staged into Toil file management.

To run in local batch mode, provide the CWL file and the input object file::

    $ cwltoil example.cwl example-job.yml

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.

.. _File literals: http://www.commonwl.org/v1.0/CommandLineTool.html#File
.. _Directory: http://www.commonwl.org/v1.0/CommandLineTool.html#Directory
.. _InitialWorkDirRequirement: http://www.commonwl.org/v1.0/CommandLineTool.html#InitialWorkDirRequirement
.. _secondaryFiles: http://www.commonwl.org/v1.0/CommandLineTool.html#CommandInputParameter
.. _CWL User Guide: http://www.commonwl.org/v1.0/UserGuide.html


.. _runningDetail:


A real-world example
--------------------

For a more detailed example and explanation, we'll walk through running a
pipeline that performs merge-sort on a temporary file.

1. Copy and paste the following code into ``toil-sort-example.py``::

        from __future__ import absolute_import
        from argparse import ArgumentParser
        import os
        import logging
        import random
        import shutil

        from toil.job import Job


        def setup(job, input_file, n, down_checkpoints):
            """Sets up the sort.
            """
            # Write the input file to the file store
            input_filestore_id = job.fileStore.writeGlobalFile(input_file, True)
            job.fileStore.logToMaster(" Starting the merge sort ")
            job.addFollowOnJobFn(cleanup, job.addChildJobFn(down,
                                                            input_filestore_id, n,
                                                            down_checkpoints=down_checkpoints,
                                                            memory='1000M').rv(), input_file)


        def down(job, input_file_store_id, n, down_checkpoints):
            """Input is a file and a range into that file to sort and an output location in which
            to write the sorted file.
            If the range is larger than a threshold N the range is divided recursively and
            a follow on job is then created which merges back the results else
            the file is sorted and placed in the output.
            """
            # Read the file
            input_file = job.fileStore.readGlobalFile(input_file_store_id, cache=False)
            length = os.path.getsize(input_file)
            if length > n:
                # We will subdivide the file
                job.fileStore.logToMaster("Splitting file: %s of size: %s"
                                          % (input_file_store_id, length), level=logging.CRITICAL)
                # Split the file into two copies
                mid_point = get_midpoint(input_file, 0, length)
                t1 = job.fileStore.getLocalTempFile()
                with open(t1, 'w') as fH:
                    copy_subrange_of_file(input_file, 0, mid_point + 1, fH)
                t2 = job.fileStore.getLocalTempFile()
                with open(t2, 'w') as fH:
                    copy_subrange_of_file(input_file, mid_point + 1, length, fH)
                # Call down recursively
                return job.addFollowOnJobFn(up, job.addChildJobFn(down, job.fileStore.writeGlobalFile(t1), n,
                                            down_checkpoints=down_checkpoints, memory='1000M').rv(),
                                            job.addChildJobFn(down, job.fileStore.writeGlobalFile(t2), n,
                                                              down_checkpoints=down_checkpoints,
                                                              memory='1000M').rv()).rv()
            else:
                # We can sort this bit of the file
                job.fileStore.logToMaster("Sorting file: %s of size: %s"
                                          % (input_file_store_id, length), level=logging.CRITICAL)
                # Sort the copy and write back to the fileStore
                output_file = job.fileStore.getLocalTempFile()
                sort(input_file, output_file)
                return job.fileStore.writeGlobalFile(output_file)


        def up(job, input_file_id_1, input_file_id_2):
            """Merges the two files and places them in the output.
            """
            with job.fileStore.writeGlobalFileStream() as (fileHandle, output_id):
                with job.fileStore.readGlobalFileStream(input_file_id_1) as inputFileHandle1:
                    with job.fileStore.readGlobalFileStream(input_file_id_2) as inputFileHandle2:
                        merge(inputFileHandle1, inputFileHandle2, fileHandle)
                        job.fileStore.logToMaster("Merging %s and %s to %s"
                                                  % (input_file_id_1, input_file_id_2, output_id))
                # Cleanup up the input files - these deletes will occur after the completion is successful.
                job.fileStore.deleteGlobalFile(input_file_id_1)
                job.fileStore.deleteGlobalFile(input_file_id_2)
                return output_id


        def cleanup(job, temp_output_id, output_file):
            """Copies back the temporary file to input once we've successfully sorted the temporary file.
            """
            tempFile = job.fileStore.readGlobalFile(temp_output_id)
            shutil.copy(tempFile, output_file)
            job.fileStore.logToMaster("Finished copying sorted file to output: %s" % output_file)


        # convenience functions
        def sort(in_file, out_file):
            """Sorts the given file.
            """
            filehandle = open(in_file, 'r')
            lines = filehandle.readlines()
            filehandle.close()
            lines.sort()
            filehandle = open(out_file, 'w')
            for line in lines:
                filehandle.write(line)
            filehandle.close()


        def merge(filehandle_1, filehandle_2, output_filehandle):
            """Merges together two files maintaining sorted order.
            """
            line2 = filehandle_2.readline()
            for line1 in filehandle_1.readlines():
                while line2 != '' and line2 <= line1:
                    output_filehandle.write(line2)
                    line2 = filehandle_2.readline()
                output_filehandle.write(line1)
            while line2 != '':
                output_filehandle.write(line2)
                line2 = filehandle_2.readline()


        def copy_subrange_of_file(input_file, file_start, file_end, output_filehandle):
            """Copies the range (in bytes) between fileStart and fileEnd to the given
            output file handle.
            """
            with open(input_file, 'r') as fileHandle:
                fileHandle.seek(file_start)
                data = fileHandle.read(file_end - file_start)
                assert len(data) == file_end - file_start
                output_filehandle.write(data)


        def get_midpoint(file, file_start, file_end):
            """Finds the point in the file to split.
            Returns an int i such that fileStart <= i < fileEnd
            """
            filehandle = open(file, 'r')
            mid_point = (file_start + file_end) / 2
            assert mid_point >= file_start
            filehandle.seek(mid_point)
            line = filehandle.readline()
            assert len(line) >= 1
            if len(line) + mid_point < file_end:
                return mid_point + len(line) - 1
            filehandle.seek(file_start)
            line = filehandle.readline()
            assert len(line) >= 1
            assert len(line) + file_start <= file_end
            return len(line) + file_start - 1


        def make_file_to_sort(file_name, lines, line_length):
            with open(file_name, 'w') as fileHandle:
                for _ in xrange(lines):
                    line = "".join(random.choice('actgACTGNXYZ') for _ in xrange(line_length - 1)) + '\n'
                    fileHandle.write(line)


        def main():
            parser = ArgumentParser()
            Job.Runner.addToilOptions(parser)

            parser.add_argument('--num-lines', default=1000, help='Number of lines in file to sort.', type=int)
            parser.add_argument('--line-length', default=50, help='Length of lines in file to sort.', type=int)
            parser.add_argument("--N",
                                help="The threshold below which a serial sort function is used to sort file. "
                                "All lines must of length less than or equal to N or program will fail",
                                default=10000)

            options = parser.parse_args()

            if int(options.N) <= 0:
                raise RuntimeError("Invalid value of N: %s" % options.N)

            make_file_to_sort(file_name='file_to_sort.txt', lines=options.num_lines, line_length=options.line_length)

            # Now we are ready to run
            Job.Runner.startToil(Job.wrapJobFn(setup, os.path.abspath('file_to_sort.txt'), int(options.N), False,
                                               memory='1000M'), options)

        if __name__ == '__main__':
            main()

2. Run with default settings::

      $ python toil-sort-example.py file:jobStore.

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


Environment variable options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are several environment variables that affect the way Toil runs.

+------------------------+----------------------------------------------------+
| TOIL_WORKDIR           | An absolute path to a directory where Toil will    |
|                        | write its temporary files. This directory must     |
|                        | exist on each worker node and may be set to a      |
|                        | different value on each worker. The ``--workDir``  |
|                        | command line option overrides this. On Mesos nodes,|
|                        | ``TOIL_WORKDIR`` generally defaults to the Mesos   |
|                        | sandbox, except on CGCloud-provisioned nodes where |
|                        | it defaults to ``/var/lib/mesos``. In all other    |
|                        | cases, the system's `standard temporary directory`_|
|                        | is used.                                           |
+------------------------+----------------------------------------------------+
| TOIL_TEST_TEMP         | An absolute path to a directory where Toil tests   |
|                        | will write their temporary files. Defaults to the  |
|                        | system's `standard temporary directory`_.          |
+------------------------+----------------------------------------------------+
| TOIL_TEST_INTEGRATIVE  | If ``True``, this allows the integration tests to  |
|                        | run. Only valid when running the tests from the    |
|                        | source directory via ``make test`` or              |
|                        | ``make test_parallel``.                            |
+------------------------+----------------------------------------------------+
| TOIL_TEST_EXPERIMENTAL | If ``True``, this allows tests on experimental     |
|                        | features to run (such as the Google and Azure) job |
|                        | stores. Only valid when running tests from the     |
|                        | source directory via ``make test`` or              |
|                        | ``make test_parallel``.                            |
+------------------------+----------------------------------------------------+
| TOIL_APPLIANCE_SELF    | The tag of the Toil appliance version to use. See  |
|                        | :ref:`Autoscaling` and :meth:`toil.applianceSelf`  |
|                        | for more information.                              |
+------------------------+----------------------------------------------------+
| TOIL_AWS_ZONE          | The EC2 zone to provision nodes in (if using       |
|                        | Toil's provisioner.                                |
+------------------------+----------------------------------------------------+
| TOIL_AWS_AMI           | ID of the AMI to use in node provisioning. If in   |
|                        | doubt, don't set this variable.                    |
+------------------------+----------------------------------------------------+
| TOIL_AWS_NODE_DEBUG    | Determines whether to preserve nodes that have     |
|                        | failed health checks. If set to ``True``, nodes    |
|                        | that fail EC2 health checks won't immediately be   |
|                        | terminated so they can be examined and the cause   |
|                        | of failure determined. If any EC2 nodes are left   |
|                        | behind in this manner, the security group will     |
|                        | also be left behind by necessity as it cannot be   |
|                        | deleted until all associated nodes have been       |
|                        | terminated.                                        |
+------------------------+----------------------------------------------------+
| TOIL_SLURM_ARGS        | Arguments for sbatch for the slurm batch system.   |
|                        | Do not pass CPU or memory specifications here.     |
|                        | Instead, define resource requirements for the job. |
|                        | There is no default value for this variable.       |
+------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_ARGS   | Arguments for qsub for the gridengine batch        |
|                        | system. Do not pass CPU or memory specifications   |
|                        | here. Instead, define resource requirements for    |
|                        | the job. There is no default value for this        |
|                        | variable.                                          |
+------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_PE     | Parallel environment arguments for qsub and for    |
|                        | the gridengine batch system. There is no default   |
|                        | value for this variable.                           |
+------------------------+----------------------------------------------------+

.. _standard temporary directory: https://docs.python.org/2/library/tempfile.html#tempfile.gettempdir


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
