.. highlight:: console

.. _Cloud_Running:

Running in the cloud
====================

Toil jobs can be run on a variety of cloud platforms. Of these, Amazon Web
Services (AWS) is currently the best-supported solution. Toil provides the
:ref:`clusterRef` to conveniently create AWS clusters, connect to the leader
of the cluster, and then launch a workflow that runs distributedly on the
entire cluster.

The :ref:`StaticProvisioning` section explains how a static cluster (one that
won't change in size) can be created and provisioned (grown, shrunk, destroyed, etc.).

The :ref:`Autoscaling` section details how to create a cluster and run a workflow
that will dynamically scale depending on the workflows needs.

On all cloud providers, it is recommended that you run long-running jobs on
remote systems using a terminal multiplexer such as `screen`_ or `tmux`_.

For details on including dependencies in your distributed workflows have a
look at :ref:`hotDeploying`.

Screen
------

Screen allows you to run toil workflows in the cloud without the risk of a bad
connection forcing the workflow to fail.

Simply type ``screen`` to open a new ``screen``
session. Later, type ``ctrl-a`` and then ``d`` to disconnect from it, and run
``screen -r`` to reconnect to it. Commands running under ``screen`` will
continue running even when you are disconnected, allowing you to unplug your
laptop and take it home without ending your Toil jobs. See :ref:`sshCluster`
for complications that can occur when using screen within the Toil Appliance.

.. _screen: https://www.gnu.org/software/screen/
.. _tmux: https://tmux.github.io/

.. _Autoscaling:

Autoscaling
-----------

The fastest way to get started with Toil in a cloud environment is by using
Toil's autoscaling capabilities to handle node provisioning. Autoscaling is a
powerful and efficient tool for running your cluster in the cloud. It manages
your cluster for you and scales up or down depending on the workflow's demands.

If you haven't already, take a look at :ref:`installProvisioner` for information
on getting autoscaling set up before continuing on.

Autoscaling uses the cluster utilities. For more information see :ref:`clusterRef`.

.. _EC2 instance type: https://aws.amazon.com/ec2/instance-types/

.. _launchingCluster:

Launching a Cluster
^^^^^^^^^^^^^^^^^^^

Using the provisioner to launch a Toil leader instance is simple using the
:ref:`launchCluster` command::

    $ toil launch-cluster CLUSTER-NAME-HERE --nodeType=t2.micro \
       -z us-west-2a --keyPairName=your-AWS-key-pair-name

The cluster name is used to uniquely identify your cluster and will be used to
populate the instance's ``Name`` tag. In addition, the Toil provisioner will
automatically tag your cluster with an ``Owner`` tag that corresponds to your
keypair name to facilitate cost tracking.

The nodeType is an `EC2 instance type`_. This only affects any nodes launched now.

The ``-z`` parameter specifies which EC2 availability
zone to launch the cluster in. Alternatively, you can specify this option
via the ``TOIL_AWS_ZONE`` environment variable. We will assume this environment variable is set for the
rest of the tutorial. Note: the zone is different from an EC2 region. A
region corresponds to a geographical area like ``us-west-2 (Oregon)``, and
availability zones are partitions of this area like ``us-west-2a``.

For more information on options try::

    $ toil launch-cluster --help

Uploading Workflow
^^^^^^^^^^^^^^^^^^

Now that our cluster is launched we use the :ref:`rsyncCluster` utility to copy
the workflow to the leader. For a simple workflow in a single file this might
look like::

    $ toil rysnc-cluster MY-CLUSTER ~/toil-workflow.py :/

.. note::

    If your toil workflow has dependencies have a look at the :ref:`hotDeploying`
    section for a detailed explanation on how to include them.

.. _runningAutoscaling:

Running a Workflow with Autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The only remaining step is to kick off our Toil run with special autoscaling options.

First we use the :ref:`sshCluster` utility to log on to the leader. ::

    $ toil ssh-cluster MY-CLUSTER

In order for your script to make use of autoscaling you will need to specify the options
``--provisioner=aws`` and ``--nodeType=<>`` where nodeType is the name of an `EC2 instance type`_.
These options, respectively, tell Toil that we are running on AWS (currently the
only supported autoscaling environment) and which instance type to use for the
Toil worker instances. Here is an example: ::

    $ python my-toi-script.py --provisioner=aws --nodeType=m3.large

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
-------------------

Toil can be used to manage a cluster in the cloud by using the :ref:`clusterRef`.
The cluster utilities also make it easy to run a toil workflow directly on this
cluster. We call this static provisioning because the size of the cluster does not
change. This is in contrast with :ref:`Autoscaling`.

To launch a cluster with a specific number of worker nodes we use the ``-w`` option.::

    $ toil launch-cluster CLUSTER-NAME-HERE --nodeType=t2.micro \
       -z us-west-2a --keyPairName=your-AWS-key-pair-name -w 3

This will spin up a leader node with three additional workers all with the same type.

Now we can follow the instructions under :ref:`runningAWS` to start the workflow
on the cluster.

Currently static provisioning is only possible during the cluster's creation.
The ability to add new nodes and remove existing nodes via the native provisioner is
in development, but can also be achieved through CGCloud_. Of course the cluster can
always be deleted with the :ref:`destroyCluster` utility.

.. note::

    CGCloud_ also can do static provisioning for an AWS cluster, however it is being
    phased out in favor on the native provisioner.

.. _runningAWS:

Running on AWS
--------------

See :ref:`installationAWS` to get setup for running on AWS.

Having followed the :ref:`quickstart` guide, the user can run their
``HelloWorld.py`` script on a distributed cluster just by modifying the run
command. Since our cluster is distributed, we'll use the ``aws`` job store
which uses a combination of one S3 bucket and a couple of SimpleDB domains.
This allows all nodes in the cluster access to the job store which would not be
possible if we were to use the ``file`` job store with a locally mounted file
system on the leader.

Copy ``HelloWorld.py`` to the leader node using the :ref:`rsyncCluster` command, and run::

   $ python HelloWorld.py \
          --batchSystem=mesos \
          --mesosMaster=master-private-ip:5050 \
          aws:us-west-2:my-aws-jobstore

Alternatively, to run a CWL workflow::

   $ cwltoil --batchSystem=mesos  \
           --mesosMaster=master-private-ip:5050 \
           --jobStore=aws:us-west-2:my-aws-jobstore \
           example.cwl \
           example-job.yml

When running a CWL workflow on AWS, input files can be provided either on the
local file system or in S3 buckets using ``s3://`` URL references. Final output
files will be copied to the local file system of the leader node.

.. _runningAzure:

Running on Azure
----------------

See :ref:`installationAzure` to get setup for running on Azure. This section
assumes that you are SSHed into your cluster's leader node.

The Azure templates do not create a shared filesystem; you need to use the
``azure`` job store for which you need to create an *Azure storage account*.
You can store multiple job stores in a single storage account.

To create a new storage account, if you do not already have one:

1. `Click here <https://portal.azure.com/#create/Microsoft.StorageAccount>`_,
   or navigate to ``https://portal.azure.com/#create/Microsoft.StorageAccount``
   in your browser.

2. If necessary, log into the Microsoft Account that you use for Azure.

3. Fill out the presented form. The *Name* for the account, notably, must be
   a 3-to-24-character string of letters and lowercase numbers that is globally
   unique. For *Deployment model*, choose *Resource manager*. For *Resource
   group*, choose or create a resource group **different than** the one in
   which you created your cluster. For *Location*, choose the **same** region
   that you used for your cluster.

4. Press the *Create* button. Wait for your storage account to be created; you
   should get a notification in the notifications area at the upper right when
   that is done.

Once you have a storage account, you need to authorize the cluster to access
the storage account, by giving it the access key. To do find your storage
account's access key:

1. When your storage account has been created, open it up and click the
   "Settings" icon.

2. In the *Settings* panel, select *Access keys*.

3. Select the text in the *Key1* box and copy it to the clipboard, or use the
   copy-to-clipboard icon.

You then need to share the key with the cluster. To do this temporarily, for
the duration of an SSH or screen session:

1. On the leader node, run ``export AZURE_ACCOUNT_KEY="<KEY>"``, replacing
   ``<KEY>`` with the access key you copied from the Azure portal.

To do this permanently:

1. On the leader node, run ``nano ~/.toilAzureCredentials``.

2. In the editor that opens, navigate with the arrow keys, and give the file
   the following contents

   .. code-block:: ini

      [AzureStorageCredentials]
      <accountname>=<accountkey>

   Be sure to replace ``<accountname>`` with the name that you used for your
   Azure storage account, and ``<accountkey>`` with the key you obtained above.
   (If you want, you can have multiple accounts with different keys in this
   file, by adding multipe lines. If you do this, be sure to leave the
   ``AZURE_ACCOUNT_KEY`` environment variable unset.)

3. Press ``ctrl-o`` to save the file, and ``ctrl-x`` to exit the editor.

Once that's done, you are now ready to actually execute a job, storing your job
store in that Azure storage account. Assuming you followed the
:ref:`quickstart` guide above, you have an Azure storage account created, and
you have placed the storage account's access key on the cluster, you can run
the ``HelloWorld.py`` script by doing the following:

1. Place your script on the leader node, either by downloading it from the
   command line or typing or copying it into a command-line editor.

2. Run the command::

      $ python HelloWorld.py \
             --batchSystem=mesos \
             --mesosMaster=10.0.0.5:5050 \
             azure:<accountname>:hello-world-001

   To run a CWL workflow::

      $ cwltoil --batchSystem=mesos \
              --mesosMaster=10.0.0.5:5050 \
              --jobStore=azure:<accountname>:hello-world-001 \
              example.cwl \
              example-job.yml

   Be sure to replace ``<accountname>`` with the name of your Azure storage
   account.

Note that once you run a job with a particular job store name (the part after
the account name) in a particular storage account, you cannot re-use that name
in that account unless one of the following happens:

1. You are restarting the same job with the ``--restart`` option.

2. You clean the job store with ``toil clean azure:<accountname>:<jobstore>``.

3. You delete all the items created by that job, and the main job store table
   used by Toil, from the account (destroying all other job stores using the
   account).

4. The job finishes successfully and cleans itself up.


.. _runningOpenStack:

Running on Open Stack
---------------------

After setting up Toil on :ref:`installationOpenStack`, Toil scripts can be run
by designating a job store location as shown in :ref:`quickstart`.
Be sure to specify a temporary directory that Toil can use to run jobs in with
the ``--workDir`` argument::

    $ python HelloWorld.py --workDir=/tmp file:jobStore


.. _runningGoogleComputeEngine:

Running on Google Compute Engine
--------------------------------

After setting up Toil on :ref:`installationGoogleComputeEngine`, Toil scripts
can be run just by designating a job store location as shown in
:ref:`quickstart`.

If you wish to use the Google Storage job store, install Toil with the
``google`` extra (:ref:`extras`). Then, create a file named ``.boto`` with your
credentials and some configuration:

.. code-block:: ini

    [Credentials]
    gs_access_key_id = KEY_ID
    gs_secret_access_key = SECRET_KEY

    [Boto]
    https_validate_certificates = True

    [GSUtil]
    content_language = en
    default_api_version = 2

``gs_access_key_id`` and ``gs_secret_access_key`` can be generated by navigating
to your Google Cloud Storage console and clicking on *Settings*. On
the *Settings* page, navigate to the *Interoperability* tab and click *Enable
interoperability access*. On this page you can now click *Create a new key* to
generate an access key and a matching secret. Insert these into their
respective places in the ``.boto`` file and you will be able to use a Google
job store when invoking a Toil script, as in the following example::

    $ python HelloWorld.py google:projectID:jobStore

The ``projectID`` component of the job store argument above refers your Google
Cloud Project ID in the Google Cloud Console, and will be visible in the
console's banner at the top of the screen. The ``jobStore`` component is a name
of your choosing that you will use to refer to this job store.


