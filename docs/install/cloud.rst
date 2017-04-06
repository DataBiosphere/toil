.. highlight:: console

.. _cloudInstallation:

Cloud installation
==================

This section details how to properly set up Toil and its dependencies in various cloud environments.

.. _installationAWS:

Amazon Web Services
-------------------
Toil includes a native AWS provisioner that can be used to start :ref:`Autoscaling`
clusters. To provision static, non-autoscaling clusters we recommend using
CGCloud_.

.. _Toil_Provisioner:

Toil Provisioner
~~~~~~~~~~~~~~~~
The native Toil provisioner is included in Toil alongside the ``[aws]`` extra and
allows us to spin up a cluster without any external dependencies. It is built around the
Toil Appliance, a Docker image that bundles Toil and all its requirements,
e.g. Mesos. This makes deployment simple across platforms, and you can even
simulate a cluster locally (see :ref:`appliance_dev` for details).

When using the Toil provisioner, the appliance image will be automatically chosen
based on the pip installed version of Toil on your system. That choice can be
overriden by setting the environment variables ``TOIL_DOCKER_REGISTRY`` and ``TOIL_DOCKER_NAME`` or
``TOIL_APPLIANCE_SELF``. See :ref:`envars` for more information on these variables.

Using the provisioner to launch a Toil leader instance is simple::

    $ toil launch-cluster CLUSTER-NAME-HERE --nodeType=t2.micro \
       -z us-west-2a --keyPairName=your-AWS-key-pair-name

The cluster name is used to uniquely identify your cluster and will be used to
populate the instance's ``Name`` tag. In addition, the Toil provisioner will
automatically tag your cluster with an ``Owner`` tag that corresponds to your
keypair name to facilitate cost tracking.

The ``-z`` parameter is important since it specifies which EC2 availability
zone to launch the cluster in. Alternatively, you can specify this option
via the ``TOIL_AWS_ZONE`` environment variable. This is generally preferable
since it lets us avoid repeating the ``-z`` option for every subsequent
cluster command. We will assume this environment variable is set for the
rest of the tutorial. Note: the zone is different from an EC2 region. A
region corresponds to a geographical area like ``us-west-2 (Oregon)``, and
availability zones are partitions of this area like ``us-west-2a``.

Once the leader is running, the ``ssh-cluster`` and ``rsync-cluster`` utilities can be
used to interact with the instance::

    $ toil rsync-cluster CLUSTER-NAME-HERE \
       ~/localFile :/remoteDestination

The most frequent use case for the ``rsync-cluster`` utility is deploying your
Toil script to the Toil leader. Note that the syntax is the same as traditional
`rsync <https://linux.die.net/man/1/rsync>`_ with the exception of the hostname before
the colon. This is not needed in ``toil rsync-cluster`` since the hostname is automatically
determined by Toil.

The last utility provided by the Toil Provisioner is ``ssh-cluster`` and it
can be used as follows::

    $ toil ssh-cluster CLUSTER-NAME-HERE

This will give you a shell on the Toil leader, where you proceed to start off your
:ref:Autoscaling run. This shell actually originates from within the Toil leader container,
and as such has a couple restrictions involving the use of the ``screen`` and ``tmux`` commands.
The shell doesn't know that it is a TTY, which prevents it from properly allocating
a new screen session. This can be worked around via::

    $ script
    $ screen

Simply running ``screen`` within ``script`` will get things working properly again.

Finally, you can execute remote commands with the following syntax::

    $ toil ssh-cluster CLUSTER-NAME-HERE remoteCommand

It is not advised that you run your Toil workflow using remote execution like this
unless a tool like `nohup <https://linux.die.net/man/1/nohup>`_ is used to insure the
process does not die if the SSH connection is interrupted.

CGCloud Quickstart
~~~~~~~~~~~~~~~~~~
Setting up clusters with CGCloud_ has the benefit of coming pre-packaged with
Toil and Mesos, our preferred batch system for running on AWS.

.. admonition:: CGCloud documentation

    Users of CGCloud_ may want to refer to the documentation for CGCloud-core_ and
    CGCloud-toil_.

1. Create and activate a virtualenv::

      $ virtualenv ~/cgcloud
      $ source ~/cgcloud/bin/activate

2. Install CGCloud and the CGCloud Toil plugin::

      $ pip install cgcloud-toil

3. Add the following to your ``~/.profile``, using the appropriate region for
   your account:

   .. code-block:: bash

      export CGCLOUD_ZONE=us-west-2a
      export CGCLOUD_PLUGINS="cgcloud.toil:$CGCLOUD_PLUGINS"

4. Setup credentials for your AWS account in ``~/.aws/credentials``::

      [default]
      aws_access_key_id=PASTE_YOUR_FOO_ACCESS_KEY_ID_HERE
      aws_secret_access_key=PASTE_YOUR_FOO_SECRET_KEY_ID_HERE
      region=us-west-2

5. Register your SSH key. If you don't have one, create it with ``ssh-keygen``::

      $ cgcloud register-key ~/.ssh/id_rsa.pub

6. Create a template *toil-box* which will contain necessary prerequisites::

      $ cgcloud create -IT toil-box

7. Create a small leader/worker cluster::

      $ cgcloud create-cluster toil -s 2 -t m3.large

8. SSH into the leader::

      $ cgcloud ssh toil-leader

At this point, any Toil script can be run on the distributed AWS cluster by
following instructions in :ref:`runningAWS`.

Finally, if you wish to tear down the cluster and remove all its data permanently,
CGCloud allows you to do so without logging into the AWS web interface::

   $ cgcloud terminate-cluster toil

.. _CGCloud-core: https://github.com/BD2KGenomics/cgcloud/blob/master/core/README.rst
.. _CGCloud-toil: https://github.com/BD2KGenomics/cgcloud/blob/master/toil/README.rst

.. _installationAzure:

Azure
-----

.. image:: https://azuredeploy.net/deploybutton.png
   :target: https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBD2KGenomics%2Ftoil%2Fmaster%2Fcontrib%2Fazure%2Fazuredeploy.json
   :alt: Microsoft Azure deploy button

Toil comes with a `cluster template`_ to facilitate easy deployment of clusters
running Toil on Microsoft Azure. The template allows these clusters to be
created and managed through the Azure portal. To use the template to set up a
Toil Mesos cluster on Azure, use the deploy button above, or open the
`deploy link`_ in your browser.

For more information, see the `cluster template`_'s documentation, or read our
walkthrough on :ref:`azure-walkthrough`.

.. _cluster template: https://github.com/BD2KGenomics/toil/blob/master/contrib/azure/README.md
.. _deploy link: https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBD2KGenomics%2Ftoil%2Fmaster%2Fcontrib%2Fazure%2Fazuredeploy.json

.. _installationOpenStack:

OpenStack
---------

Our group is working to expand distributed cluster support to OpenStack by
providing convenient Docker containers to launch Mesos from. Currently,
OpenStack nodes can be set up to run Toil in single machine mode by following
the :ref:`installation-ref`.

.. _installationGoogleComputeEngine:

Google Compute Engine
---------------------

Support for running on Google Cloud is currently experimental. Our group is
working to expand distributed cluster support to Google Compute with a cluster
provisioning tool based around a Dockerized Mesos setup. Currently, Google
Compute Engine nodes can be configured to run Toil in single machine mode by
following the :ref:`installation-ref`.
