.. _running:

Example Toil Workflows
======================

.. _quickstart:

Toil Quickstart
---------------

A Toil workflow can be run with just three steps.
 
1. Install Toil (see :ref:`installation-ref`)

2. Copy and paste the following code block into ``helloWorld.py``:

   .. code-block:: python

      from toil.common import Toil
      from toil.job import Job

      def helloWorld(message, memory="1G", cores=1, disk="1G"):
          return "Hello, world!, here's a message: %s" % message

      j = Job.wrapFn(helloWorld, "You did it!")

      if __name__ == "__main__":
          parser = Job.Runner.getDefaultArgumentParser()
          options = parser.parse_args()
          with Toil(options) as toil:
              output = toil.start(j)
          print output

3. Specify a job store and run the workflow like so::

       (venv) $ python helloWorld.py file:my-job-store

.. note::

   Don't actually type ``(venv) $`` in at the beginning of each command. This is intended only to remind the user that
   they should have their :ref:`virtual environment <venvPrep>` running.

Congratulations! You've run your first Toil workflow on the ``singleMachine`` batch system (the default) using the
``file`` job store.

The batch system is what schedules the jobs Toil creates. Toil supports many different kinds of batch systems
(such as `Apache Mesos`_ and Grid Engine) which makes it easy to run your workflow in all kinds of places.
The ``singleMachine`` batch system is primarily used to prepare and debug workflows on the
local machine. Once ready, they can be run on a full-fledged batch system (see :ref:`batchsysteminterface`).

Usually, a workflow will generate files, and Toil
needs a place to keep track of things. The job store is where Toil keeps all of the intermediate files shared
between jobs. The argument you passed in to your script ``file:my-job-store`` indicated where. The ``file:``
part just tells Toil you are using the ``file`` job store, which means everything is kept in a temporary directory
called ``my-job-store``. (Read more about :ref:`jobStoreInterface`.)

Toil is totally customizable! Run ``python helloWorld.py --help`` to see a complete list of available options.

For something beyond a "Hello, world!" example, refer to :ref:`runningDetail`.

.. _Apache Mesos: https://mesos.apache.org/gettingstarted/

.. _cwlquickstart:

CWL Quickstart
--------------

The `Common Workflow Language`_ (CWL) is an emerging standard for writing
workflows that are portable across multiple workflow engines and platforms.
Running CWL workflows using Toil is easy.

#. First ensure that Toil is installed with the
   ``cwl`` extra (see :ref:`extras`).  ::

       (venv) $ pip install toil[cwl]

   This installs the ``cwltoil`` and ``cwl-runner`` executables. These are identical -
   ``cwl-runner`` is the portable name for the default system CWL runner.

#. Copy and paste the following code block into ``example.cwl``:

   .. code-block:: yaml

       cwlVersion: v1.0
       class: CommandLineTool
       baseCommand: echo
       stdout: output.txt
       inputs:
         message:
           type: string
           inputBinding:
             position: 1
       outputs:
         output:
           type: stdout

   and this code into ``example-job.yaml``:

   .. code-block:: yaml

        message: Hello world!

#. To run the workflow simply enter ::

        (venv) $ cwltoil example.cwl example-job.yaml

   Your output will be in ``output.txt`` ::

        (venv) $ cat output.txt
        Hello world!

To learn more about CWL, see the `CWL User Guide`_ (from where this example was
shamelessly borrowed).

To run this workflow on an AWS cluster have a look at :ref:`awscwl`.

For information on using CWL with Toil see the section :ref:`cwl`

.. _CWL User Guide: http://www.commonwl.org/v1.0/UserGuide.html

.. _runningDetail:

Real-World Example
------------------

For a more detailed example and explanation, we've developed a sample pipeline
that merge-sorts a temporary file.

Download :download:`the example code <../../src/toil/test/sort/sort.py>`.

First let's just run it and see what happens.

#. Run it with the default settings::

      $ python sort.py file:jobStore

   The workflow created a file called ``sortedFile.txt`` in your current directory.
   Have a look at it and notice that it contains a whole lot of sorted lines!

   This workflow does a smart merge sort on a file it generates. A file called ``fileToSort.txt``. The sort is *smart*
   because each step of the process---splitting the file into separate chunks, sorting these chunks, and merging them
   back together---is compartmentalized into a **job**. Each job can specify it's own resource requirements and will
   only be run after the jobs it depends upon have run. Jobs without dependencies will be run in parallel.

#. Run with custom options::

      $ python sort.py file:jobStore --numLines=5000 --lineLength=10 --workDir=/tmp/

   Here we see that we can add our own options to a Toil script. The first two
   options determine the number of lines and how many characters are in each line.
   The last option is a built-in Toil option where temporary files unique to a
   job are kept.

To understand the details of what's going on inside.
Let's start with the ``main()`` function. It looks like a lot of code, but don't worry, we'll break it down piece by
piece.

.. literalinclude:: ../../src/toil/test/sort/sort.py
    :pyobject: main

First we make a parser to process command line arguments using the `argparse`_ module. It's important that we add the
call to :func:`Job.Runner.addToilOptions` to initialize our parser with all of Toil's default options. Then we add
the command line arguments unique to this workflow, and parse the input. The help message listed with the arguments
should give you a pretty good idea of what they can do.

Next we do a little bit of verification of the input arguments. The option ``--fileToSort`` allows you to specify a file
that needs to be sorted. If this option isn't given, it's here that we make our own file with the call to
:func:`makeFileToSort`.

Finally we come to the context manager that initializes the workflow. We create a path to the input file prepended with
``'file://'`` as per the documentation for :func:`toil.common.Toil` when staging a file that is stored locally. Notice
that we have to check whether or not the workflow is restarting so that we don't import the file more than once.
Finally we can kick off the workflow by calling :func:`toil.common.Toil.start` on the job ``setup``. When the workflow
ends we capture its output (the sorted file's fileID) and use that in :func:`toil.common.Toil.exportFile` to move the
sorted file from the job store back into "userland".

Next let's look at the job that begins the actual workflow, ``setup``.

.. literalinclude:: ../../src/toil/test/sort/sort.py
    :pyobject: setup

``setup`` really only does two things. First it writes to the logs using :func:`Job.FileStore.logToMaster` and then
calls :func:`addChildJobFn`. Child jobs run directly after the current job. This function turns the 'job function'
``down`` into an actual job and passes in the inputs including an optional resource requirement, ``memory``. The job
doesn't actually get run until the call to :func:`Job.rv`. Once the job ``down`` finishes, its output is returned here.

Now we can look at what ``down`` does.

.. literalinclude:: ../../src/toil/test/sort/sort.py
    :pyobject: down

Down is the recursive part of the workflow. First we read the file into the local filestore by calling
:func:`Job.FileStore.readGlobalFile`. This puts a copy of the file in the temp directory for this particular job. This
storage will disappear once this job ends. For a detailed explanation of the filestore, job store, and their interfaces
have a look at :ref:`managingFiles`.

Next ``down`` checks the base case of the recursion: is the length of the input file less than ``N`` (remember ``N``
was an option we added to the workflow in ``main``). In the base case, we just sort the file, and return the file ID
of this new sorted file.

If the base case fails, then the file is split into two new tempFiles using :func:`Job.FileStore.getLocalTempFile` and
the helper function ``copySubRangeOfFile``. Finally we add a follow on Job ``up`` with :func:`Job.addFollowOnJobFn`.
We've already seen child jobs. A follow-on Job is a job that runs after the current job and *all* of its children (and their children and follow-ons) have
completed. Using a follow-on makes sense because ``up`` is responsible for merging the files together and we don't want
to merge the files together until we *know* they are sorted. Again, the return value of the follow-on job is requested
using :func:`Job.rv`.

Looking at ``up``

.. literalinclude:: ../../src/toil/test/sort/sort.py
    :pyobject: up

we see that the two input files are merged together and the output is written to a new file using
:func:`job.FileStore.writeGlobalFileStream`. After a little cleanup, the output file is returned.

Once the final ``up`` finishes and all of the ``rv()`` promises are fulfilled, ``main`` receives the sorted file's ID
which it uses in ``exportFile`` to send it to the user.

There are other things in this example that we didn't go over such as :ref:`checkpoints` and the details of much of the
the :ref:`api`.

.. _argparse: https://docs.python.org/2.7/library/argparse.html

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

Typing ``python sort.py --help`` will show the complete list of
arguments for the workflow which includes both Toil's and ones defined inside
``sort.py``. A complete explanation of Toil's arguments can be
found in :ref:`commandRef`.


Logging
~~~~~~~

By default, Toil logs a lot of information related to the current environment
in addition to messages from the batch system and jobs. This can be configured
with the ``--logLevel`` flag. For example, to only log ``CRITICAL`` level
messages to the screen::

   $ python sort.py file:jobStore --logLevel=critical

This hides most of the information we get from the Toil run. For more detail,
we can run the pipeline with ``--logLevel=debug`` to see a comprehensive
output. For more information, see :ref:`loggingRef`.


Error Handling and Resuming Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Toil, you can recover gracefully from a bug in your pipeline without losing
any progress from successfully-completed jobs. To demonstrate this, let's add
a bug to our example code to see how Toil handles a failure and how we can
resume a pipeline after that happens. Add a bad assertion at line 52 of the
example (the first line of ``down()``):

.. code-block:: python

   def down(job, inputFileStoreID, N, downCheckpoints, memory=sortMemory):
       ...
       assert 1 == 2, "Test error!"

When we run the pipeline, Toil will show a detailed failure log with a traceback::

   $ python sort.py file:jobStore
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
failure, the job store is preserved so that the workflow can be restarted,
starting from the previously failed jobs. We can restart the pipeline by running::

   $ python sort.py file:jobStore --restart

We can also change the number of times Toil will attempt to retry a failed job::

   $ python sort.py --retryCount 2 --restart

You'll now see Toil attempt to rerun the failed job until it runs out of tries.
``--retryCount`` is useful for non-systemic errors, like downloading a file that
may experience a sporadic interruption, or some other non-deterministic failure.

To successfully restart our pipeline, we can edit our script to comment out
line 30, or remove it, and then run

::

   $ python sort.py --restart

The pipeline will run successfully, and the job store will be removed on the
pipeline's completion.


Collecting Statistics
~~~~~~~~~~~~~~~~~~~~~

A Toil pipeline can be run with the ``--stats`` flag to allows collection of
statistics::

   $ python sort.py --stats

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


Launch a Toil Workflow in AWS
-----------------------------
After having installed the ``aws`` extra for Toil during the :ref:`installation-ref` and set up AWS (see :ref:`prepare_aws-ref`),
the user can run the basic ``helloWorld.py`` script (:ref:`quickstart`) on a VM in AWS just by modifying the run command.


#. Launch a cluster in AWS. ::

       (venv) $ toil launch-cluster <cluster-name> \
	--keyPairName <AWS-key-pair-name> \
       --nodeType t2.medium \
	--zone us-west-2a


#. Copy ``helloWorld.py`` to the leader node. ::

      	(venv) $ toil rsync-cluster --zone us-west-2a <cluster-name> helloWorld.py :/tmp

#. Login to the cluster leader node. ::

      	(venv) $ toil ssh-cluster --zone us-west-2a <cluster-name>

#. Run the Toil script in the cluster ::

      	$ python /tmp/helloWorld.py file:my-job-store

   Along with some other ``INFO`` log messages, you should get the following output in your
   terminal window: ``Hello, world!, here's a message: You did it!``


#. Exit from the SSH connection. ::

      	$ exit

#. Destroy the cluster. ::

      	(venv) $ toil destroy-cluster --zone us-west-2a <cluster-name>

.. _awscwl:

Run a CWL Workflow on AWS
-------------------------
After having installed the ``aws`` and ``cwl`` extras for Toil during the :ref:`installation-ref` and set up AWS (see :ref:`prepare_aws-ref`),
the user can run a CWL workflow with Toil on AWS.

#. First launch a node in AWS using the :ref:`launchCluster` command. ::

    	(venv) $ toil launch-cluster <cluster-name> \
    	--keyPairName <AWS-key-pair-name> \
    	--nodeType t2.micro \
    	--zone us-west-2a

#. Copy ``example.cwl`` and ``example-job.cwl`` from the :ref:`CWL example <cwlquickstart>` to the node using the :ref:`rsyncCluster` command. ::

     	(venv) $ toil rsync-cluster --zone us-west-2a <cluster-name> \
	example.cwl example-job.cwl :/tmp

#. Launch the CWL workflow using the :ref:`sshCluster` utility. ::

      	(venv) $ toil ssh-cluster --zone us-west-2a <cluster-name> \
      	cwltoil \
      	/tmp/example.cwl \
      	/tmp/example-job.yml

   ..  tip::

      When running a CWL workflow on AWS, input files can be provided either on the
      local file system or in S3 buckets using ``s3://`` URI references. Final output
      files will be copied to the local file system of the leader node.

#. Destroy the cluster. ::

      	(venv) $ toil destroy-cluster --zone us-west-2a <cluster-name>


.. todo:: Autoscaling example

.. todo:: Spark example
