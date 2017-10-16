
.. _runningAWS:

Running in AWS
==============

Toil jobs can be run on a variety of cloud platforms. Of these, Amazon Web
Services (AWS) is currently the best-supported solution. Toil provides the
:ref:`clusterRef` to conveniently create AWS clusters, connect to the leader
of the cluster, and then launch a workflow. The leader handles distributing
the jobs over the worker nodes and autoscaling to optimize costs.

The fastest way to get started with Toil in a cloud environment is by using
Toil's autoscaling capabilities to handle node provisioning. Autoscaling is a
powerful and efficient tool for running your cluster in the cloud. It manages
your cluster for you and scales up or down depending on the workflow's demands.

The :ref:`Autoscaling` section details how to create a cluster and run a workflow
that will dynamically scale depending on the workflow's needs.

The :ref:`StaticProvisioning` section explains how a static cluster (one that
won't automatically change in size) can be created and provisioned (grown, shrunk, destroyed, etc.).


.. _EC2 instance type: https://aws.amazon.com/ec2/instance-types/


To setup AWS, see :ref:`prepare_aws-ref`.


.. _installProvisioner:

Toil Provisioner
----------------

The Toil provisioner is included in Toil alongside the ``[aws]`` extra and
allows us to spin up a cluster.

Getting started with the provisioner is simple:

#. Make sure you have Toil installed with the AWS extras. For detailed instructions see :ref:`extras`.

#. You will need an AWS account and you will need to save your AWS credentials on your local
   machine. For help setting up an AWS account see
   `here <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html>`__. For
   setting up your aws credentials follow instructions
   `here <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files>`__.

The Toil provisioner is built around the Toil Appliance, a Docker image that bundles
Toil and all its requirements (e.g. Mesos). This makes deployment simple across
platforms, and you can even simulate a cluster locally (see :ref:`appliance_dev` for details).

.. admonition:: Choosing Toil Appliance Image

    When using the Toil provisioner, the appliance image will be automatically chosen
    based on the pip installed version of Toil on your system. That choice can be
    overriden by setting the environment variables ``TOIL_DOCKER_REGISTRY`` and ``TOIL_DOCKER_NAME`` or
    ``TOIL_APPLIANCE_SELF``. See :ref:`envars` for more information on these variables. If
    you are developing with autoscaling and want to test and build your own
    appliance have a look at :ref:`appliance_dev`.

For information on using the Toil Provisioner have a look at :ref:`Autoscaling`.

Details about Launching a Cluster in AWS
----------------------------------------

Using the provisioner to launch a Toil leader instance is simple using the ``launch-cluster`` command. For example, to launch a cluster named "my-cluster" with a t2.medium leader in the us-west-2a zone, run:
::

    	(venv) $ toil launch-cluster my-cluster \
	--leaderNodeType t2.medium \
       	--zone us-west-2a \
	--keyPairName <your-AWS-key-pair-name>

The cluster name is used to uniquely identify your cluster and will be used to
populate the instance's ``Name`` tag. In addition, the Toil provisioner will
automatically tag your cluster with an ``Owner`` tag that corresponds to your
keypair name to facilitate cost tracking.

The leaderNodeType is an `EC2 instance type`_. This only affects the leader node.

.. _EC2 instance type: https://aws.amazon.com/ec2/instance-types/

The ``--zone`` parameter specifies which EC2 availability
zone to launch the cluster in. Alternatively, you can specify this option
via the ``TOIL_AWS_ZONE`` environment variable. Note: the zone is different from an EC2 region. A region corresponds to a geographical area like ``us-west-2 (Oregon)``, and availability zones are partitions of this area like ``us-west-2a``.

For more information on options try::

    	(venv) $ toil launch-cluster --help


.. _StaticProvisioning:

Static Provisioning
^^^^^^^^^^^^^^^^^^^
Toil can be used to manage a cluster in the cloud by using the :ref:`clusterRef`.
The cluster utilities also make it easy to run a toil workflow directly on this
cluster. We call this static provisioning because the size of the cluster does not
change. This is in contrast with :ref:`Autoscaling`.

To launch worker nodes alongside the leader we use the ``-w`` option.::

	(venv) $ toil launch-cluster my-cluster --leaderNodeType t2.small \
	-z us-west-2a --keyPairName your-AWS-key-pair-name --nodeTypes m3.large,t2.micro -w 1,4

This will spin up a leader node of type t2.small with five additional workers - one m3.large instance and four t2.micro.

Now we can follow the instructions under :ref:`runningAWS` to start the workflow
on the cluster.

Currently static provisioning is only possible during the cluster's creation.
The ability to add new nodes and remove existing nodes via the native provisioner is
in development, but can also be achieved through CGCloud_. Of course the cluster can
always be deleted with the :ref:`destroyCluster` utility.

.. note::

    CGCloud_ also can do static provisioning for an AWS cluster, however it is being phased out in favor of the Toil provisioner.

.. _CGCloud: https://github.com/BD2KGenomics/cgcloud

Uploading Workflows
^^^^^^^^^^^^^^^^^^^

Now that our cluster is launched, we use the :ref:`rsyncCluster` utility to copy
the workflow to the leader. For a simple workflow in a single file this might
look like::

    	(venv) $ toil rsync-cluster -z us-west-2a my-cluster toil-workflow.py :/

.. note::

    If your toil workflow has dependencies have a look at the :ref:`remoteDeploying`
    section for a detailed explanation on how to include them.


.. _Autoscaling:

Running a Workflow with Autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Autoscaling is a feature of running Toil in a cloud whereby additional cloud instances are launched to run the workflow.  Autoscaling leverages Mesos containers to provide an execution environment for these workflows.  



#. Download :download:`sort.py <../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in AWS using the :ref:`launchCluster` command. ::

        (venv) $ toil launch-cluster <cluster-name> \
        --keyPairName <AWS-key-pair-name> \
        --leaderNodeType t2.micro \
        --zone us-west-2a

#. Copy the `sort.py` script up to the leader node. ::

	(venv) $ toil rsync-cluster <cluster-name> sort.py :/tmp

#. Login to the leader node. ::

	(venv) $ toil ssh-cluster <cluster-name>

#. Run the script as an autoscaling workflow. ::

	$ python /tmp/sort.py  \
	aws:us-west-2:autoscaling-sort-jobstore \
	--provisioner aws --nodeTypes c3.large --maxNodes 2\
	--batchSystem mesos --mesosMaster <private-IP>:5050 
	--logLevel DEBUG

   .. note::

    In this example, the autoscaling Toil code creates up to two instances of type `c3.large` and launches Mesos slave containers inside them. The containers are then available to run jobs defined by the `sort.py` script.  Toil also creates a bucket in S3 called `aws:us-west-2:autoscaling-sort-jobstore` to store intermediate job results. The Toil autoscaler can also provision multiple different node types, which is useful for workflows that have jobs with varying resource requirements. For example, one could execute the script with ``--nodeTypes c3.large,r3.xlarge --maxNodes 5,1``, which would allow the provisioner to create up to five c3.large nodes and one r3.xlarge node for memory-intensive jobs. In this situation, the autoscaler would avoid creating the more expensive r3.xlarge node until needed, running most jobs on the c3.large nodes.

#. View the generated file to sort. ::

	$ head fileToSort.txt

#. View the sorted file. ::

	$ head sortedFile.txt

For more information on other autoscaling (and other) options have a look at :ref:`workflowOptions` and/or run::

    	$ python my-toil-script.py --help

.. important::

    Some important caveats about starting a toil run through an ssh session are
    explained in the :ref:`sshCluster` section.

Preemptability
^^^^^^^^^^^^^^

Toil can run on a heterogeneous cluster of both preemptable and non-preemptable nodes.
A node type can be specified as preemptable by adding a spot bid to its entry in the list of node types provided with the ``--nodeTypes`` flag. While individual jobs can each explicitly specify whether or not they should be run on preemptable nodes
via the boolean ``preemptable`` resource requirement, the ``--defaultPreemptable`` flag will allow jobs without a ``preemptable`` requirement to run on preemptable machines.


.. admonition:: Specify Preemptability Carefully

	Ensure that your choices for ``--nodeTypes`` and ``--maxNodes <>`` make
	sense for your workflow and won't cause it to hang. You should make sure the
	provisioner is able to create nodes large enough to run the largest job
	in the workflow, and that non-preemptable node types are allowed if there are
	non-preemptable jobs in the workflow.

Finally, the ``--preemptableCompensation`` flag can be used to handle cases where preemptable nodes may not be available but are required for your workflow. With this flag enabled, the autoscaler will attempt to compensate
for a shortage of preemptable nodes of a certain type by creating non-preemptable nodes of that type, if
non-preemptable nodes of that type were specified in ``--nodeTypes``.

.. admonition:: Using Mesos with Toil on AWS

   The mesos master and agent processes bind to the private IP addresses of their
   EC2 instance, so be sure to use the master's private IP when specifying
   ``--mesosMaster``. Using the public IP will prevent the nodes from properly
   discovering each other.

Dashboard
---------
Toil provides a dashboard for viewing the RAM and CPU usage of each node, the number of
issued jobs of each type, the number of failed jobs, and the size of the jobs queue. To launch this dashboard
for a toil workflow, include the ``--metrics`` flag in the toil script command. The dashboard can then be viewed
in your browser at localhost:3000 while connected to the leader node through ``toil ssh-cluster``.
On AWS, the dashboard keeps track of every node in the cluster to monitor CPU and RAM usage, but it
can also be used while running a workflow on a single machine. The dashboard uses Grafana as the
front end for displaying real-time plots, and Prometheus for tracking metrics exported by toil. In order to use the
dashboard for a non-released toil version, you will have to build the containers locally with ``make docker``, since
the prometheus, grafana, and mtail containers used in the dashboard are tied to a specific toil version.


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

.. note::

   Boto must be `configured`_ with AWS credentials before using cluster utilities.

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
