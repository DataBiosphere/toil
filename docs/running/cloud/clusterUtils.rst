.. _clusterRef:

Cluster Utilities
-----------------

There are several utilities used for starting and managing a Toil cluster using the AWS provisioner. They are installed
via the ``[aws]`` or ``[google]`` extra. For installation details see :ref:`installProvisioner`. The cluster utilities
are used for :ref:`runningAWS` and are comprised of ``toil launch-cluster``, ``toil rsync-cluster``,
``toil ssh-cluster``, and ``toil destroy-cluster`` entry points.

Cluster commands specific to ``toil`` are:

    ``status`` --- Reports runtime and resource usage for all jobs in a specified jobstore (workflow must have originally been run using the -\\-stats option).

    ``stats`` --- Inspects a job store to see which jobs have failed, run successfully, etc.

    ``destroy-cluster`` --- For autoscaling.  Terminates the specified cluster and associated resources.

    ``launch-cluster`` --- For autoscaling.  This is used to launch a toil leader instance with the specified provisioner.

    ``rsync-cluster`` --- For autoscaling.  Used to transfer files to a cluster launched with ``toil launch-cluster``.

    ``ssh-cluster`` --- SSHs into the toil appliance container running on the leader of the cluster.

    ``clean`` --- Delete the job store used by a previous Toil workflow invocation.

    ``kill`` --- Kills any running jobs in a rogue toil.

For information on a specific utility run::

    toil launch-cluster --help

for a full list of its options and functionality.

The cluster utilities can be used for :ref:`runningGCE` and :ref:`runningAWS`.

.. tip::

   By default, all of the cluster utilities expect to be running on AWS. To run with Google
   you will need to specify the ``--provisioner gce`` option for each utility.

.. note::

   Boto must be `configured`_ with AWS credentials before using cluster utilities.

   :ref:`runningGCE` contains instructions for

.. _configured: http://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration

.. _cli_status:

Stats Command
--------------

To use the stats command, a workflow must first be run using the ``--stats`` option.  Using this command makes certain
that toil does not delete the job store, no matter what other options are specified (i.e. normally the option
``--clean=always`` would delete the job, but ``--stats`` will override this).

An example of this would be running the following::

    python discoverfiles.py file:my-jobstore --stats

Where ``discoverfiles.py`` is the following:

.. literalinclude:: ../../../src/toil/test/docs/scripts/tutorial_discoverfiles.py

Notice the ``displayName`` key, which can rename a job, giving it an alias when it is finally displayed in stats.
Running this workflow file should record three job names: ``sysFiles`` (job1), ``userFiles`` (job2), and ``discoverFiles`` (job3).
To see the runtime and resources used for each job when it was run, type ::

    toil stats file:my-jobstore

This should output the following:

.. code-block:: python

    Batch System: singleMachine
    Default Cores: 1  Default Memory: 2097152K
    Max Cores: 9.22337e+18
    Total Clock: 0.56  Total Runtime: 1.01
    Worker
        Count |                                    Time* |                                    Clock |                                     Wait |                                   Memory
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total
            1 |     0.14    0.14    0.14    0.14    0.14 |     0.13    0.13    0.13    0.13    0.13 |     0.01    0.01    0.01    0.01    0.01 |      76K     76K     76K     76K     76K
    Job
     Worker Jobs  |     min    med    ave    max
                  |       3      3      3      3
        Count |                                    Time* |                                    Clock |                                     Wait |                                   Memory
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total
            3 |     0.01    0.06    0.05    0.07    0.14 |     0.00    0.06    0.04    0.07    0.12 |     0.00    0.01    0.00    0.01    0.01 |      76K     76K     76K     76K    229K
     sysFiles
        Count |                                    Time* |                                    Clock |                                     Wait |                                   Memory
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total
            1 |     0.01    0.01    0.01    0.01    0.01 |     0.00    0.00    0.00    0.00    0.00 |     0.01    0.01    0.01    0.01    0.01 |      76K     76K     76K     76K     76K
     userFiles
        Count |                                    Time* |                                    Clock |                                     Wait |                                   Memory
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total
            1 |     0.06    0.06    0.06    0.06    0.06 |     0.06    0.06    0.06    0.06    0.06 |     0.01    0.01    0.01    0.01    0.01 |      76K     76K     76K     76K     76K
     discoverFiles
        Count |                                    Time* |                                    Clock |                                     Wait |                                   Memory
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total
            1 |     0.07    0.07    0.07    0.07    0.07 |     0.07    0.07    0.07    0.07    0.07 |     0.00    0.00    0.00    0.00    0.00 |      76K     76K     76K     76K     76K

Once we're done, we can clean up the job store by running

::

   toil clean file:my-jobstore

Status Command
--------------

Continuing the example from the stats section above, if we ran our workflow with the command ::

    python discoverfiles.py file:my-jobstore --stats

We could interrogate our jobstore with the status command, for example::

    toil status file:my-jobstore

If the run was successful, this would not return much valuable information, something like ::

    2018-01-11 19:31:29,739 - toil.lib.bioio - INFO - Root logger is at level 'INFO', 'toil' logger at level 'INFO'.
    2018-01-11 19:31:29,740 - toil.utils.toilStatus - INFO - Parsed arguments
    2018-01-11 19:31:29,740 - toil.utils.toilStatus - INFO - Checking if we have files for Toil
    The root job of the job store is absent, the workflow completed successfully.

Otherwise, the ``status`` command should return the following:

    There are ``x`` unfinished jobs, ``y`` parent jobs with children, ``z`` jobs with services, ``a`` services, and ``b`` totally failed jobs currently in  ``c``.

Clean Command
-------------

If a Toil pipeline didn't finish successfully, or was run using ``--clean=always`` or ``--stats``, the job store will exist
until it is deleted. ``toil clean <jobStore>`` ensures that all artifacts associated with a job store are removed.
This is particularly useful for deleting AWS job stores, which reserves an SDB domain as well as an S3 bucket.

The deletion of the job store can be modified by the ``--clean`` argument, and may be set to ``always``, ``onError``,
``never``, or ``onSuccess`` (default).

Temporary directories where jobs are running can also be saved from deletion using the ``--cleanWorkDir``, which has
the same options as ``--clean``.  This option should only be run when debugging, as intermediate jobs will fill up
disk space.

.. _launchCluster:

Launch-Cluster Command
----------------------

Running ``toil launch-cluster`` starts up a leader for a cluster. Workers can be
added to the initial cluster by specifying the ``-w`` option.  An example would be ::

    $ toil launch-cluster my-cluster --leaderNodeType t2.small -z us-west-2a --keyPairName your-AWS-key-pair-name --nodeTypes m3.large,t2.micro -w 1,4

Options are listed below.  These can also be displayed by running ::

    $ toil launch-cluster --help

launch-cluster's main positional argument is the clusterName.  This is simply the name of your cluster.  If it does not
exist yet, Toil will create it for you.

**Launch-Cluster Options**

  --help                -h also accepted.  Displays this help menu.
  --tempDirRoot TEMPDIRROOT
                        Path to the temporary directory where all temp
                        files are created, by default uses the current working
                        directory as the base.
  --version             Display version.
  --provisioner CLOUDPROVIDER
                        -p CLOUDPROVIDER also accepted.  The provisioner for
                        cluster auto-scaling.  Both AWS and GCE are
                        currently supported.
  --zone ZONE           -z ZONE also accepted.  The availability zone of the leader. This
                        parameter can also be set via the TOIL_AWS_ZONE or TOIL_GCE_ZONE
                        environment variables, or by the ec2_region_name
                        parameter in your .boto file if using AWS, or derived from the
                        instance metadata if using this utility on an existing
                        EC2 instance.
  --leaderNodeType LEADERNODETYPE
                        Non-preemptable node type to use for the cluster
                        leader.
  --keyPairName KEYPAIRNAME
                        The name of the AWS or ssh key pair to include on the
                        instance.
  --boto BOTOPATH       The path to the boto credentials directory. This is
                        transferred to all nodes in order to access the AWS
                        jobStore from non-AWS instances.
  --tag KEYVALUE
                        KEYVALUE is specified as KEY=VALUE. -t KEY=VALUE also
                        accepted.  Tags are added to the AWS cluster for this
                        node and all of its children.
                        Tags are of the form: -t key1=value1 --tag key2=value2.
                        Multiple tags are allowed and each tag needs its own
                        flag. By default the cluster is tagged with:
                        { "Name": clusterName, "Owner": IAM username }.
  --vpcSubnet VPCSUBNET
                        VPC subnet ID to launch cluster in. Uses default
                        subnet if not specified. This subnet needs to have
                        auto assign IPs turned on.
  --nodeTypes NODETYPES
                        Comma-separated list of node types to create while
                        launching the leader. The syntax for each node type
                        depends on the provisioner used. For the AWS
                        provisioner this is the name of an EC2 instance type
                        followed by a colon and the price in dollars to bid for
                        a spot instance, for example 'c3.8xlarge:0.42'. Must
                        also provide the --workers argument to specify how
                        many workers of each node type to create.
  --workers WORKERS
                        -w WORKERS also accepted.  Comma-separated list of the
                        number of workers of each node type to launch alongside
                        the leader when the cluster is created. This can be
                        useful if running toil without auto-scaling but with
                        need of more hardware support.
  --leaderStorage LEADERSTORAGE
                        Specify the size (in gigabytes) of the root volume for
                        the leader instance. This is an EBS volume.
  --nodeStorage NODESTORAGE
                        Specify the size (in gigabytes) of the root volume for
                        any worker instances created when using the -w flag.
                        This is an EBS volume.

**Logging Options**

  --logOff              Same as -\\-logCritical.
  --logCritical         Turn on logging at level CRITICAL and above. (default
                        is INFO)
  --logError            Turn on logging at level ERROR and above. (default is
                        INFO)
  --logWarning          Turn on logging at level WARNING and above. (default
                        is INFO)
  --logInfo             Turn on logging at level INFO and above. (default is
                        INFO)
  --logDebug            Turn on logging at level DEBUG and above. (default is
                        INFO)
  --logLevel LOGLEVEL   Log at given level (may be either OFF (or CRITICAL),
                        ERROR, WARN (or WARNING), INFO or DEBUG). (default is
                        INFO)
  --logFile LOGFILE     File to log in.
  --rotatingLogging     Turn on rotating logging, which prevents log files
                        getting too big.

.. _sshCluster:

Ssh-Cluster Command
-------------------

Toil provides the ability to ssh into the leader of the cluster. This
can be done as follows::

    $ toil ssh-cluster CLUSTER-NAME-HERE

This will open a shell on the Toil leader and is used to start an
:ref:`Autoscaling` run. Issues with docker prevent using ``screen`` and ``tmux``
when sshing the cluster (The shell doesn't know that it is a TTY which prevents
it from allocating a new screen session). This can be worked around via ::

    $ script
    $ screen

Simply running ``screen`` within ``script`` will get things working properly again.

Finally, you can execute remote commands with the following syntax::

    $ toil ssh-cluster CLUSTER-NAME-HERE remoteCommand

It is not advised that you run your Toil workflow using remote execution like this
unless a tool like `nohup <https://linux.die.net/man/1/nohup>`_ is used to ensure the
process does not die if the SSH connection is interrupted.

For an example usage, see :ref:`Autoscaling`.

.. _rsyncCluster:

Rsync-Cluster Command
---------------------

The most frequent use case for the ``rsync-cluster`` utility is deploying your
Toil script to the Toil leader. Note that the syntax is the same as traditional
`rsync <https://linux.die.net/man/1/rsync>`_ with the exception of the hostname before
the colon. This is not needed in ``toil rsync-cluster`` since the hostname is automatically
determined by Toil.

Here is an example of its usage::

    $ toil rsync-cluster CLUSTER-NAME-HERE \
       ~/localFile :/remoteDestination

.. _destroyCluster:

Destroy-Cluster Command
-----------------------

The ``destroy-cluster`` command is the advised way to get rid of any Toil cluster
launched using the :ref:`launchCluster` command. It ensures that all attached nodes, volumes,
security groups, etc. are deleted. If a node or cluster is shut down using Amazon's online portal
residual resources may still be in use in the background. To delete a cluster run ::

    $ toil destroy-cluster CLUSTER-NAME-HERE


Kill Command
------------

To kill all currently running jobs for a given jobstore, use the command ::

    toil kill file:my-jobstore
