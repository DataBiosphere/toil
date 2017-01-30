.. _cloudInstallation:

.. highlight:: console

Cloud installation
==================

.. _installationAWS:

Amazon Web Services
-------------------
Toil includes a native AWS provisioner that can be used to start autoscaling
clusters. For more information on this provisioner, see the :ref:`Autoscaling`
section. To provision static, non-autoscaling clusters we recommend using
CGCloud_.

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
