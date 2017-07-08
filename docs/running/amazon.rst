.. _runningAWS:

Running in AWS
==============

Prepare your AWS environment
----------------------------
1. If necessary, create and activate an `AWS account`_

.. _AWS account: https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/ 


2. Create a `key pair`_ in the ``us-west-2a`` availability zone. 

.. _key pair: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair 
.. important::

   This will automatically download a PEM file from your Web browser to your local machine.  Put this file in your home directory or somewhere memorable.

3. Add the AWS private key to the authentication agent.  
::

   $ (venv) ssh-add <path-to-aws-pem-file>

4. Create an `AWS access key`_

.. _AWS access key: http://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html 

5. Export AWS access key environment variables in shell.  
::

   $ (venv) export AWS_ACCESS_KEY_ID=<access-key-string>
   $ (venv) export AWS_SECRET_ACCESS_KEY=<secret-access-key-string>

.. note::

   Instead of typing the above ``ssh-add`` and ``export`` commands every time you wish to access AWS, you could instead put them in your shell initialization file (e.g. ``~/.bash_profile``).


Launch a Toil workflow in AWS
-----------------------------
The user can run the same ``HelloWorld.py`` script on a distributed cluster just by modifying the run command. Since our cluster is distributed, we'll use the ``aws`` job store which uses a combination of one S3 bucket and a couple of SimpleDB domains.  This allows all nodes in the cluster access to the job store which would not be possible if we were to use the ``file`` job store with a locally mounted file system on the leader.

1. Launch a mesos cluster in AWS.
::

   $ (venv) toil launch-cluster <cluster-name> \
   --keyPairName <AWS-key-pair-name> \
   --nodeType t2.micro \
   --zone us-west-2a 

2. Copy ``HelloWorld.py`` to the leader node.  
:: 

$ (venv) toil rsync-cluster <cluster-name> HelloWorld.py :/tmp

3. Run the Toil script in the cluster.  
::

   $ python HelloWorld.py \
   --batchSystem mesos \
   --mesosMaster master-private-ip:5050 \
   aws:us-west-2:my-aws-jobstore

Run a CWL workflow
------------------

::

   $ cwltoil --batchSystem=mesos  \
   --mesosMaster master-private-ip:5050 \
   --jobStore aws:us-west-2:my-aws-jobstore \
   example.cwl \
   example-job.yml

When running a CWL workflow on AWS, input files can be provided either on the
local file system or in S3 buckets using ``s3://`` URL references. Final output
files will be copied to the local file system of the leader node.

Details about Launching a Cluster in AWS
----------------------------------------

Using the provisioner to launch a Toil leader instance is simple using the launch-cluster command.
::

    $ toil launch-cluster my-cluster --nodeType=t2.micro \
       --zone us-west-2a --keyPairName=your-AWS-key-pair-name

The cluster name is used to uniquely identify your cluster and will be used to
populate the instance's ``Name`` tag. In addition, the Toil provisioner will
automatically tag your cluster with an ``Owner`` tag that corresponds to your
keypair name to facilitate cost tracking.

The nodeType is an `EC2 instance type`_. This only affects any nodes launched now.

.. _EC2 instance type: https://aws.amazon.com/ec2/instance-types/

The ``-z`` parameter specifies which EC2 availability
zone to launch the cluster in. Alternatively, you can specify this option
via the ``TOIL_AWS_ZONE`` environment variable. We will assume this environment variable is set for the
rest of the tutorial. Note: the zone is different from an EC2 region. A
region corresponds to a geographical area like ``us-west-2 (Oregon)``, and
availability zones are partitions of this area like ``us-west-2a``.

For more information on options try::

    $ toil launch-cluster --help

Uploading Workflows
^^^^^^^^^^^^^^^^^^^

Now that our cluster is launched we use the :ref:`rsyncCluster` utility to copy
the workflow to the leader. For a simple workflow in a single file this might
look like::

    $ toil rysnc-cluster my-cluster ~/toil-workflow.py :/

.. note::

    If your toil workflow has dependencies have a look at the :ref:`hotDeploying`
    section for a detailed explanation on how to include them.

.. _runningAutoscaling:

Running a Workflow with Autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The only remaining step is to kick off our Toil run with special autoscaling options.

First we use the :ref:`sshCluster` utility to log on to the leader. ::

    $ toil ssh-cluster my-cluster

In order for your script to make use of autoscaling you will need to specify the options
``--provisioner=aws`` and ``--nodeType=<>`` where nodeType is the name of an `EC2 instance type`_.
These options, respectively, tell Toil that we are running on AWS (currently the
only supported autoscaling environment) and which instance type to use for the
Toil worker instances. Here is an example: ::

    $ python my-toil-script.py --provisioner=aws --nodeType=m3.large

For more information on other autoscaling (and other) options
have a look at :ref:`workflowOptions` and/or run::

    $ python my-toil-script.py --help

.. important::

    Some important caveats about starting a toil run through an ssh session are
    explained in the :ref:`sshCluster` section.

Preemptability
^^^^^^^^^^^^^^

Toil can run on a heterogeneous cluster of both preemptable and non-preemptable nodes.
Our preemptable node type can be set by using the ``--preemptableNodeType=<>`` flag. While individual jobs can
each explicitly specify whether or not they should be run on preemptable nodes
via the boolean ``preemptable`` resource requirement, the
``--defaultPreemptable`` flag will allow jobs without a ``preemptable``
requirement to run on preemptable machines.

We can set the maximum number of preemptable and non-preemptable nodes via the flags ``--maxNodes=<>``
and ``--maxPreemptableNodes=<>``.

.. admonition:: Specify Preemptability Carefully

    Ensure that your choices for ``--maxNodes=<>`` and ``--maxPreemptableNodes=<>`` make
    sense for your workflow and won't cause it to hang - if the workflow requires preemptable nodes set
    ``--maxPreemptableNodes`` to some non-zero value and if any job requires
    non-preemptable nodes set ``--maxNodes`` to some non-zero value.

Finally, the ``--preemptableCompensation`` flag can be used to handle
cases where preemptable nodes may not be available but are required for your
workflow.

.. admonition:: Using Mesos with Toil on AWS

   The mesos master and agent processes bind to the private IP addresses of their
   EC2 instance, so be sure to use the master's private IP when specifying
   ``--mesosMaster``. Using the public IP will prevent the nodes from properly
   discovering each other.

.. _StaticProvisioning:

Static Provisioning
^^^^^^^^^^^^^^^^^^^
Toil can be used to manage a cluster in the cloud by using the :ref:`clusterRef`.
The cluster utilities also make it easy to run a toil workflow directly on this
cluster. We call this static provisioning because the size of the cluster does not
change. This is in contrast with :ref:`Autoscaling`.

To launch a cluster with a specific number of worker nodes we use the ``-w`` option.::

    $ toil launch-cluster my-cluster --nodeType=t2.micro \
       -z us-west-2a --keyPairName=your-AWS-key-pair-name -w 3

This will spin up a leader node with three additional workers all with the same type.

Now we can follow the instructions under :ref:`runningAWS` to start the workflow
on the cluster.

Currently static provisioning is only possible during the cluster's creation.
The ability to add new nodes and remove existing nodes via the native provisioner is
in development, but can also be achieved through CGCloud_. Of course the cluster can
always be deleted with the :ref:`destroyCluster` utility.

.. note::

    CGCloud_ also can do static provisioning for an AWS cluster, however it is being phased out in favor on the native provisioner.

.. _CGCloud: https://github.com/BD2KGenomics/cgcloud
