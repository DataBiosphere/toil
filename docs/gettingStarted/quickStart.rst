.. _running:

Quickstart Examples
===================

.. _quickstart:

Running a basic workflow
------------------------

A Toil workflow can be run with just three steps:

1. Install Toil (see :ref:`installation-ref`)

2. Copy and paste the following code block into a new file called ``helloWorld.py``:

   .. code-block:: python

      from toil.common import Toil
      from toil.job import Job

      def helloWorld(message, memory="1G", cores=1, disk="1G"):
          return "Hello, world!, here's a message: %s" % message

      if __name__ == "__main__":
          parser = Job.Runner.getDefaultArgumentParser()
          options = parser.parse_args()
          with Toil(options) as toil:
              output = toil.start(Job.wrapFn(helloWorld, "You did it!"))
          print output


3. Specify the name of the :ref:`job store <jobStoreOverview>` and run the workflow::

       (venv) $ python helloWorld.py file:my-job-store

.. note::

   Don't actually type ``(venv) $`` in at the beginning of each command. This is intended only to remind the user that
   they should have their :ref:`virtual environment <venvPrep>` running.

Congratulations! You've run your first Toil workflow using the default :ref:`Batch System <batchsysteminterface>`, ``singleMachine``.
using the ``file`` job store.

Toil uses batch systems to manage the jobs it creates.

The ``singleMachine`` batch system is primarily used to prepare and debug workflows on a
local machine. Once validated, try running them on a full-fledged batch system (see :ref:`batchsysteminterface`).
Toil supports many different batch systems such as `Apache Mesos`_ and Grid Engine; its versatility makes it
easy to run your workflow in all kinds of places.

Toil is totally customizable! Run ``python helloWorld.py --help`` to see a complete list of available options.

For something beyond a "Hello, world!" example, refer to :ref:`runningDetail`.

.. _Apache Mesos: https://mesos.apache.org/getting-started/

.. _cwlquickstart:

Running a basic CWL workflow
----------------------------

The `Common Workflow Language`_ (CWL) is an emerging standard for writing
workflows that are portable across multiple workflow engines and platforms.
Running CWL workflows using Toil is easy.

#. First ensure that Toil is installed with the
   ``cwl`` extra (see :ref:`extras`).  ::

       (venv) $ pip install 'toil[cwl]'

   This installs the ``toil-cwl-runner`` and ``cwltoil`` executables.

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

        (venv) $ toil-cwl-runner example.cwl example-job.yaml

   Your output will be in ``output.txt`` ::

        (venv) $ cat output.txt
        Hello world!

To learn more about CWL, see the `CWL User Guide`_ (from where this example was
shamelessly borrowed).

To run this workflow on an AWS cluster have a look at :ref:`awscwl`.

For information on using CWL with Toil see the section :ref:`cwl`

.. _CWL User Guide: http://www.commonwl.org/v1.0/UserGuide.html

Running a basic WDL workflow
----------------------------

The `Workflow Description Language`_ (WDL) is another emerging language for writing workflows that are portable across multiple workflow engines and platforms.
Running WDL workflows using Toil is still in alpha, and currently experimental.  Toil currently supports basic workflow syntax (see :ref:`wdlSupport` for more details and examples).  Here we go over running a basic WDL helloworld workflow.

#. First ensure that Toil is installed with the
   ``wdl`` extra (see :ref:`extras`).  ::

        (venv) $ pip install 'toil[wdl]'

   This installs the ``toil-wdl-runner`` executable.

#. Copy and paste the following code block into ``wdl-helloworld.wdl``::

        workflow write_simple_file {
          call write_file
        }
        task write_file {
          String message
          command { echo ${message} > wdl-helloworld-output.txt }
          output { File test = "wdl-helloworld-output.txt" }
        }

#. and this code into ``wdl-helloworld.json``::

        {
          "write_simple_file.write_file.message": "Hello world!"
        }

#. To run the workflow simply enter ::

        (venv) $ toil-wdl-runner wdl-helloworld.wdl wdl-helloworld.json

   Your output will be in ``wdl-helloworld-output.txt`` ::

        (venv) $ cat wdl-helloworld-output.txt
        Hello world!

To learn more about WDL, see the main `WDL website`_ .

.. _WDL website: https://software.broadinstitute.org/wdl/
.. _Workflow Description Language: https://software.broadinstitute.org/wdl/

.. _runningDetail:

A (more) real-world example
---------------------------

For a more detailed example and explanation, we've developed a sample pipeline
that merge-sorts a temporary file. This is not supposed to be an efficient
sorting program, rather a more fully worked example of what Toil is capable of.

.. _sortExample:

Running the example
~~~~~~~~~~~~~~~~~~~

#. Download :download:`the example code <../../src/toil/test/sort/sort.py>`.


#. Run it with the default settings::

      (venv) $ python sort.py file:jobStore

   The workflow created a file called ``sortedFile.txt`` in your current directory.
   Have a look at it and notice that it contains a whole lot of sorted lines!

   This workflow does a smart merge sort on a file it generates. A file called ``fileToSort.txt``. The sort is *smart*
   because each step of the process---splitting the file into separate chunks, sorting these chunks, and merging them
   back together---is compartmentalized into a **job**. Each job can specify it's own resource requirements and will
   only be run after the jobs it depends upon have run. Jobs without dependencies will be run in parallel.

#. Run with custom options::

      (venv) $ python sort.py file:jobStore --numLines=5000 --lineLength=10 --workDir=/tmp/ --overwriteOutput=True

   Here we see that we can add our own options to a Toil script. The first two
   options determine the number of lines and how many characters are in each line.
   The last option is a built-in Toil option where temporary files unique to a
   job are kept.

Describing the source code
~~~~~~~~~~~~~~~~~~~~~~~~~~

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

``setup`` really only does two things. First it writes to the logs using :func:`Job.log` and then
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

At the end of the script the lines:

.. code-block:: python

    if __name__ == '__main__'
        main()

are included to ensure that the main function is only run once in the '__main__' process
invoked by you, the user.
In Toil terms, by invoking the script you created the *leader process*
in which the ``main()``
function is run. A *worker process* is a separate process whose sole purpose
is to host the execution of one or more jobs defined in that script. In any Toil
workflow there is always one leader process, and potentially many worker processes.

When using the single-machine batch system (the default), the worker processes will be running
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

   (venv) $ python sort.py file:jobStore --logLevel=critical --overwriteOutput=True

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

   (venv) $ python sort.py file:jobStore
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

   (venv) $ python sort.py file:jobStore --restart --overwriteOutput=True

We can also change the number of times Toil will attempt to retry a failed job::

   (venv) $ python sort.py --retryCount 2 --restart --overwriteOutput=True

You'll now see Toil attempt to rerun the failed job until it runs out of tries.
``--retryCount`` is useful for non-systemic errors, like downloading a file that
may experience a sporadic interruption, or some other non-deterministic failure.

To successfully restart our pipeline, we can edit our script to comment out
line 30, or remove it, and then run

::

    (venv) $ python sort.py --restart --overwriteOutput=True

The pipeline will run successfully, and the job store will be removed on the
pipeline's completion.


Collecting Statistics
~~~~~~~~~~~~~~~~~~~~~

Please see the :ref:`cli_status` section for more on gathering runtime and resource info on jobs.


Launching a Toil Workflow in AWS
--------------------------------
After having installed the ``aws`` extra for Toil during the :ref:`installation-ref` and set up AWS
(see :ref:`prepareAWS`), the user can run the basic ``helloWorld.py`` script (:ref:`quickstart`)
on a VM in AWS just by modifying the run command.

Note that when running in AWS, users can either run the workflow on a single instance or run it on a
cluster (which is running across multiple containers on multiple AWS instances).  For more information
on running Toil workflows on a cluster, see :ref:`runningAWS`.

Also!  Remember to use the :ref:`destroyCluster` command when finished to destroy the cluster!  Otherwise things may not be cleaned up properly.

#. Launch a cluster in AWS using the :ref:`launchCluster` command. The arguments ``keyPairName``,
   ``leaderNodeType``, and ``zone`` are required to launch a cluster. ::

        (venv) $ toil launch-cluster <cluster-name> --keyPairName <AWS-key-pair-name> --leaderNodeType t2.medium --zone us-west-2a

#. Copy ``helloWorld.py`` to the ``/tmp`` directory on the leader node using the :ref:`rsyncCluster` command.
   Note that the command requires defining the file to copy as well as the target location on the cluster leader node.::

        (venv) $ toil rsync-cluster --zone us-west-2a <cluster-name> helloWorld.py :/tmp

#. Login to the cluster leader node using the :ref:`sshCluster` command. Note this command will log you in as the
   ``root`` user ::

        (venv) $ toil ssh-cluster --zone us-west-2a <cluster-name>

#. Run the Toil script in the cluster.  In this particular case, we create an S3 bucket called ``my-S3-bucket`` in
   the ``us-west-2`` availability zone to store intermediate job results. ::

        $ python /tmp/helloWorld.py aws:us-west-2:my-S3-bucket

   Along with some other ``INFO`` log messages, you should get the following output in your terminal window:
   ``Hello, world!, here's a message: You did it!``


#. Exit from the SSH connection. ::

        $ exit

#. Use the :ref:`destroyCluster` command to destroy the cluster. Note this command will destroy the cluster leader
   node and any resources created to run the job, including the S3 bucket. ::

        (venv) $ toil destroy-cluster --zone us-west-2a <cluster-name>


.. _awscwl:

Running a CWL Workflow on AWS
-----------------------------
After having installed the ``aws`` and ``cwl`` extras for Toil during the :ref:`installation-ref` and set up AWS
(see :ref:`prepareAWS`), the user can run a CWL workflow with Toil on AWS.

Also!  Remember to use the :ref:`destroyCluster` command when finished to destroy the cluster!  Otherwise things may not be cleaned up properly.


#. First launch a node in AWS using the :ref:`launchCluster` command. ::

      (venv) $ toil launch-cluster <cluster-name> --keyPairName <AWS-key-pair-name> --leaderNodeType t2.medium --zone us-west-2a

#. Copy ``example.cwl`` and ``example-job.yaml`` from the :ref:`CWL example <cwlquickstart>` to the node using
   the :ref:`rsyncCluster` command. ::

      (venv) $ toil rsync-cluster --zone us-west-2a <cluster-name> example.cwl :/tmp
      (venv) $ toil rsync-cluster --zone us-west-2a <cluster-name> example-job.yaml :/tmp

#. SSH into the cluster's leader node using the :ref:`sshCluster` utility. ::

      (venv) $ toil ssh-cluster --zone us-west-2a <cluster-name>

#. Once on the leader node, it's a good idea to update and install the following::

    sudo apt-get update
    sudo apt-get -y upgrade
    sudo apt-get -y dist-upgrade
    sudo apt-get -y install git
    sudo pip install mesos.cli

#. Now create a new ``virtualenv`` with the ``--system-site-packages`` option and activate::

    virtualenv --system-site-packages venv
    source venv/bin/activate

#. Now run the CWL workflow::

      (venv) $ toil-cwl-runner /tmp/example.cwl /tmp/example-job.yaml

   ..  tip::

      When running a CWL workflow on AWS, input files can be provided either on the
      local file system or in S3 buckets using ``s3://`` URI references. Final output
      files will be copied to the local file system of the leader node.

#. Finally, log out of the leader node and from your local computer, destroy the cluster. ::

      (venv) $ toil destroy-cluster --zone us-west-2a <cluster-name>


.. _awscactus:

Running a Workflow with Autoscaling - Cactus
---------------------------------------------------

`Cactus <https://github.com/ComparativeGenomicsToolkit/cactus>`__ is a reference-free, whole-genome multiple alignment
program that can be run on any of the cloud platforms Toil supports.

.. note::

      **Cloud Independence**:

      This example provides a "cloud agnostic" view of running Cactus with Toil. Most options will not change between cloud providers.
      However, each provisioner has unique inputs for  ``--leaderNodeType``, ``--nodeType`` and ``--zone``.
      We recommend the following:

        +----------------------+----------------+------------+---------------+
        |        Option        | Used in        |  AWS       |     Google    |
        +----------------------+----------------+------------+---------------+
        | ``--leaderNodeType`` | launch-cluster | t2.medium  | n1-standard-1 |
        +----------------------+----------------+------------+---------------+
        | ``--zone``           | launch-cluster | us-west-2a |               |
        +----------------------+----------------+------------+   us-west1-a  +
        | ``--zone``           | cactus         | us-west-2  |               |
        +----------------------+----------------+------------+---------------+
        | ``--nodeType``       | cactus         | c3.4xlarge | n1-standard-8 |
        +----------------------+----------------+------------+---------------+

      When executing ``toil launch-cluster`` with ``gce`` specified for ``--provisioner``, the option ``--boto`` must
      be specified and given a path to your .boto file. See :ref:`Running in Google Compute Engine (GCE)` for more information about the ``--boto`` option.

Also!  Remember to use the :ref:`destroyCluster` command when finished to destroy the cluster!  Otherwise things may not be cleaned up properly.

#. Download :download:`pestis.tar.gz <../../src/toil/test/cactus/pestis.tar.gz>`.

#. Launch a leader node using the :ref:`launchCluster` command. ::

        (venv) $ toil launch-cluster <cluster-name> --provisioner <aws, gce, azure> --keyPairName <key-pair-name> --leaderNodeType <type> --zone <zone>


   .. note::

        **A Helpful Tip**

        When using AWS, setting the environment variable eliminates having to specify the ``--zone`` option for each command.
        This will be supported for GCE and Azure in the future. ::

            (venv) $ export TOIL_AWS_ZONE=us-west-2c

#. Create appropriate directory for uploading files. ::

        (venv) $ toil ssh-cluster --provisioner <aws, gce, azure> <cluster-name>
        $ mkdir /root/cact_ex
        $ exit

#. Copy the required files, i.e., seqFile.txt (a text file containing the locations of the input sequences as
   well as their phylogenetic tree, see
   `here <https://github.com/ComparativeGenomicsToolkit/cactus#seqfile-the-input-file>`__), organisms' genome sequence
   files in FASTA format, and configuration files (e.g. blockTrim1.xml, if desired), up to the leader node. ::

      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> pestis-short-aws-seqFile.txt :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> GCF_000169655.1_ASM16965v1_genomic.fna :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> GCF_000006645.1_ASM664v1_genomic.fna :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> GCF_000182485.1_ASM18248v1_genomic.fna :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> GCF_000013805.1_ASM1380v1_genomic.fna :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> setup_leaderNode.sh :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> blockTrim1.xml :/root/cact_ex
      (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name> blockTrim3.xml :/root/cact_ex

#. Log into the leader node. ::

        (venv) $ toil ssh-cluster --provisioner <aws, gce, azure> <cluster-name>

#. Set up the environment of the leader node to run Cactus. ::

        $ bash /root/cact_ex/setup_leaderNode.sh
        $ source cact_venv/bin/activate
        (cact_venv) $ cd cactus
        (cact_venv) $ pip install --upgrade .

#. Run `Cactus <https://github.com/ComparativeGenomicsToolkit/cactus>`__ as an autoscaling workflow. ::

       (cact_venv) $ TOIL_APPLIANCE_SELF=quay.io/ucsc_cgl/toil:3.14.0 cactus --provisioner <aws, gce, azure> --nodeType <type> --maxNodes 2 --minNodes 0 --retry 10 --batchSystem mesos --disableCaching --logDebug --logFile /logFile_pestis3 --configFile /root/cact_ex/blockTrim3.xml <aws, google, azure>:<zone>:cactus-pestis /root/cact_ex/pestis-short-aws-seqFile.txt /root/cact_ex/pestis_output3.hal

   .. note::

      **Pieces of the Puzzle**:

      ``TOIL_APPLIANCE_SELF=quay.io/ucsc_cgl/toil:3.14.0`` - specfies the version of Toil being used, 3.14.0;
      if the latest one is desired, please eliminate.

      ``--nodeType`` - determines the instance type used for worker nodes. The instance type specified here must be on
      the same cloud provider as the one specified with ``--leaderNodeType``

      ``--maxNodes 2`` - creates up to two instances of the type specified with ``--nodeType`` and
      launches Mesos worker containers inside them.

      ``--logDebug`` - equivalent to ``--logLevel DEBUG``.

      ``--logFile /logFile_pestis3`` - writes logs in a file named `logFile_pestis3` under ``/`` folder.

      ``--configFile`` - this is not required depending on whether a specific configuration file is intended to run
      the alignment.

      ``<aws, google, azure>:<zone>:cactus-pestis`` - creates a bucket, named ``cactus-pestis``, with the specified cloud provider to store intermediate job files and metadata.
      **NOTE**: If you want to use a GCE-based jobstore, specify ``google`` here, not ``gce``.

      The result file, named ``pestis_output3.hal``, is stored under ``/root/cact_ex`` folder of the leader node.

      Use ``cactus --help`` to see all the Cactus and Toil flags available.

#. Log out of the leader node. ::

        (cact_venv) $ exit

#. Download the resulted output to local machine. ::

        (venv) $ toil rsync-cluster --provisioner <aws, gce, azure> <cluster-name>  :/root/cact_ex/pestis_output3.hal <path-of-folder-on-local-machine>

#. Destroy the cluster. ::

        (venv) $ toil destroy-cluster --provisioner <aws, gce, azure> <cluster-name>

