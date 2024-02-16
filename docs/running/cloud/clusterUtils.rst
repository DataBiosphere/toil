.. _clusterUtils:

Toil Cluster Utilities
----------------------

In addition to the generic :ref:`utils`, there are several utilities used for starting and managing a Toil cluster using the AWS or GCE provisioners. They are installed
via the ``[aws]`` or ``[google]`` extra. For installation details see :ref:`installProvisioner`.

The ``toil`` cluster subcommands are:

    ``destroy-cluster`` --- For autoscaling.  Terminates the specified cluster and associated resources.

    ``launch-cluster`` --- For autoscaling.  This is used to launch a toil leader instance with the specified provisioner.

    ``rsync-cluster`` --- For autoscaling.  Used to transfer files to a cluster launched with ``toil launch-cluster``.

    ``ssh-cluster`` --- SSHs into the toil appliance container running on the leader of the cluster.

For information on a specific utility, run it with the ``--help`` option::

    toil launch-cluster --help

The cluster utilities can be used for :ref:`runningGCE` and :ref:`runningAWS`.

.. tip::

   By default, all of the cluster utilities expect to be running on AWS. To run with Google
   you will need to specify the ``--provisioner gce`` option for each utility.

.. note::

   Boto must be `configured`_ with AWS credentials before using cluster utilities.

   :ref:`runningGCE` contains instructions for

.. _configured: http://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration

.. _launchCluster:

Launch-Cluster Command
~~~~~~~~~~~~~~~~~~~~~~

Running ``toil launch-cluster`` starts up a leader for a cluster. Workers can be
added to the initial cluster by specifying the ``-w`` option.  An example would be ::

    $ toil launch-cluster my-cluster \
          --leaderNodeType t2.small -z us-west-2a \
          --keyPairName your-AWS-key-pair-name \
          --nodeTypes m3.large,t2.micro -w 1,4

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
  --owner OWNER
                        The owner tag for all instances. If not given, the value in
                        TOIL_OWNER_TAG will be used, or else the value of
                        ``--keyPairName``.
  --boto BOTOPATH       The path to the boto credentials directory. This is
                        transferred to all nodes in order to access the AWS
                        jobStore from non-AWS instances.
  --tag KEYVALUE
                        KEYVALUE is specified as KEY=VALUE. -t KEY=VALUE also
                        accepted.  Tags are added to the AWS cluster for this
                        node and all of its children.
                        Tags are of the form: ``-t key1=value1`` ``--tag key2=value2``.
                        Multiple tags are allowed and each tag needs its own
                        flag. By default the cluster is tagged with:
                        { "Name": clusterName, "Owner": IAM username }.
  --vpcSubnet VPCSUBNET
                        VPC subnet ID to launch cluster leader in. Uses default
                        subnet if not specified. This subnet needs to have auto
                        assign IPs turned on.
  --nodeTypes NODETYPES
                        Comma-separated list of node types to create while
                        launching the leader. The syntax for each node type
                        depends on the provisioner used. For the AWS
                        provisioner this is the name of an EC2 instance type
                        followed by a colon and the price in dollars to bid for
                        a spot instance, for example 'c3.8xlarge:0.42'. Must
                        also provide the ``--workers`` argument to specify how
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
  --nodeStorageOverrides NODESTORAGEOVERRIDES
                        Comma-separated list of nodeType:nodeStorage that are used
                        to override the default value from ``--nodeStorage`` for the
                        specified nodeType(s). This is useful for heterogeneous jobs
                        where some tasks require much more disk than others.

**Logging Options**

  --logOff              Same as ``--logCritical``.
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
~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

The most frequent use case for the ``rsync-cluster`` utility is deploying your
workflow code to the Toil leader. Note that the syntax is the same as traditional
`rsync <https://linux.die.net/man/1/rsync>`_ with the exception of the hostname before
the colon. This is not needed in ``toil rsync-cluster`` since the hostname is automatically
determined by Toil.

Here is an example of its usage::

    $ toil rsync-cluster CLUSTER-NAME-HERE \
       ~/localFile :/remoteDestination

.. _destroyCluster:

Destroy-Cluster Command
~~~~~~~~~~~~~~~~~~~~~~~

The ``destroy-cluster`` command is the advised way to get rid of any Toil cluster
launched using the :ref:`launchCluster` command. It ensures that all attached nodes, volumes,
security groups, etc. are deleted. If a node or cluster is shut down using Amazon's online portal
residual resources may still be in use in the background. To delete a cluster run ::

    $ toil destroy-cluster CLUSTER-NAME-HERE

