.. _clusterRef:

Cluster Utilities
-----------------
There are several utilities used for starting and managing a Toil cluster using
the AWS provisioner. They are installed via the ``[aws]`` extra. For installation
details see :ref:`installProvisioner`. The cluster utilities are used for :ref:`runningAWS` and are comprised of
``toil launch-cluster``, ``toil rsync-cluster``, ``toil ssh-cluster``, and
``toil destroy-cluster`` entry points. For a detailed explanation of the cluster
utilities run::

    toil --help

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

.. _launchCluster:

launch-cluster
^^^^^^^^^^^^^^

Running ``toil launch-cluster`` starts up a leader for a cluster. Workers can be
added to the initial cluster by specifying the ``-w`` option. For an example usage see
:ref:`launchCluster`. More information can be found using the ``--help`` option.

.. _sshCluster:

ssh-cluster
^^^^^^^^^^^

Toil provides the ability to ssh into the leader of the cluster. This
can be done as follows::

    $ toil ssh-cluster CLUSTER-NAME-HERE

This will open a shell on the Toil leader and is used to start an
:ref:`Autoscaling` run. Issues with docker prevent using ``screen`` and ``tmux``
when sshing the cluster (The shell doesn't know that it is a TTY which prevents
it from allocating a new screen session). This can be worked around via::

    $ script
    $ screen

Simply running ``screen`` within ``script`` will get things working properly again.

Finally, you can execute remote commands with the following syntax::

    $ toil ssh-cluster CLUSTER-NAME-HERE remoteCommand

It is not advised that you run your Toil workflow using remote execution like this
unless a tool like `nohup <https://linux.die.net/man/1/nohup>`_ is used to insure the
process does not die if the SSH connection is interrupted.

For an example usage, see :ref:`Autoscaling`.

.. _rsyncCluster:

rsync-cluster
^^^^^^^^^^^^^

The most frequent use case for the ``rsync-cluster`` utility is deploying your
Toil script to the Toil leader. Note that the syntax is the same as traditional
`rsync <https://linux.die.net/man/1/rsync>`_ with the exception of the hostname before
the colon. This is not needed in ``toil rsync-cluster`` since the hostname is automatically
determined by Toil.

Here is an example of its usage::

    $ toil rsync-cluster CLUSTER-NAME-HERE \
       ~/localFile :/remoteDestination

.. _destroyCluster:

destroy-cluster
^^^^^^^^^^^^^^^

The ``destroy-cluster`` command is the advised way to get rid of any Toil cluster
launched using the :ref:`launchCluster` command. It ensures that all attached node, volumes, and
security groups etc. are deleted. If a node or cluster in shut down using Amazon's online portal
residual resources may still be in use in the background. To delete a cluster run ::

    $ toil destroy-cluster CLUSTER-NAME-HERE