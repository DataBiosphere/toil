.. highlight:: console

Running in the cloud
====================

Toil jobs can be run on a variety of cloud platforms. Of these, Amazon Web
Services is currently the best-supported solution.

On all cloud providers, it is recommended that you run long-running jobs on
remote systems using a terminal multiplexer such as `screen`_ or `tmux`_.

under ``screen``. Simply type ``screen`` to open a new ``screen``
session. Later, type ``ctrl-a`` and then ``d`` to disconnect from it, and run
``screen -r`` to reconnect to it. Commands running under ``screen`` will
continue running even when you are disconnected, allowing you to unplug your
laptop and take it home without ending your Toil jobs.

.. _screen: https://www.gnu.org/software/screen/
.. _tmux: https://tmux.github.io/

.. _Autoscaling:


Autoscaling
-----------

The fastest way to get started with Toil in a cloud environment is by using
Toil's autoscaling capabilities to handle node provisioning. You can do this by
using Toil's Docker-based provisioner, which operates using the Amazon Web
Services cloud platform.

The AWS provisioner is included in Toil alongside the ``[aws]`` extra and
allows us to spin up a cluster without any external dependencies using the Toil
Appliance, a Docker image that bundles Toil and all its requirements, e.g.
Mesos. Toil will automatically choose an appliance image that matches the
current Toil version but that choice can be overriden by setting the
environment variables ``TOIL_DOCKER_REGISTRY`` and ``TOIL_DOCKER_NAME`` or
``TOIL_APPLIANCE_SELF`` (see :func:`toil.applianceSelf` and
:ref:`appliance_dev` for details)::

   $ toil launch-cluster -p aws CLUSTER-NAME-HERE --nodeType=t2.micro \
       --keyPairName=your-AWS-key-pair-name

to launch a t2.micro leader instance -- adjust this instance type accordingly
to do real work. See `here <https://aws.amazon.com/ec2/instance-types/>`_ for a
full selection of EC2 instance types. For more information on cluster
management using Toil's AWS provisioner, see :ref:`clusterRef`.

Once we have our leader instance launched, the steps for both provisioners
converge. As with all distributed AWS workflows, we start our Toil run using an
AWS job store and being sure to pass ``--batchSystem=mesos``. Additionally, we
have to pass the following autoscaling specific options. You can read the help
strings for all of the possible Toil flags by passing ``--help`` to your toil
script invocation. Indicate your provisioner choice via the
``--provisioner=<>`` flag and node type for your worker nodes via
``--nodeType=<>``. Additionally, both provisioners support `preemptable nodes
<https://aws.amazon.com/ec2/spot/>`_. Toil can run on a heterogenous cluster of
both preemptable and non-preemptable nodes. Our preemptable node type can be
set by using the ``--preemptableNodeType=<>`` flag. While individual jobs can
each explicitly specify whether or not they should be run on preemptable nodes
via the boolean `preemptable` resource requirement, the
``--defaultPreemptable`` flag will allow jobs without a `preemptable`
requirement to run on preemptable machines. Finally, we can set the maximum
number of preemptable and non-preemptable nodes via the flags ``--maxNodes=<>``
and ``--maxPreemptableNodes=<>``. Insure that these choices won't cause a hang
in your workflow - if the workflow requires preemptable nodes set
``--maxPreemptableNodes`` to some non-zero value and if any job requires
non-preemptable nodes set ``--maxNodes`` to some non-zero value. If the
provisioner can't provision the correct type of node for the workflow's jobs,
the workflow will hang. Use the ``--preemptableCompensation`` flag to handle
cases where preemptable nodes may not be available but are required for your
workflow.

.. admonition:: Using mesos with Toil on AWS

   The mesos master and agent processes bind to the private IP addresses of their
   EC2 instance, so be sure to use the master's private IP when specifying
   `--mesosMaster`. Using the public IP will prevent the nodes from properly
   discovering each other.

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

Copy ``HelloWorld.py`` to the leader node, and run::

   $ python HelloWorld.py \
          --batchSystem=mesos \
          --mesosMaster=mesos-master:5050 \
          aws:us-west-2:my-aws-jobstore

Alternatively, to run a CWL workflow::

   $ cwltoil --batchSystem=mesos  \
           --mesosMaster=mesos-master:5050 \
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


