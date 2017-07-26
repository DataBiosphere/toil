.. highlight:: console

.. _cloudInstallation:

Cloud installation
==================

This section details how to properly set up Toil and its dependencies in various cloud environments.

.. _installationAWS:

Amazon Web Services
-------------------
Toil includes a native AWS provisioner that can be used to start :ref:`Autoscaling`
clusters. CGCloud_ can be used to provision static, non-autoscaling clusters, but
this functionality is currently being replaced by the Toil Provisioner.

.. _installProvisioner:

Toil Provisioner
~~~~~~~~~~~~~~~~
The native Toil provisioner is included in Toil alongside the ``[aws]`` extra and
allows us to spin up a cluster without any external dependencies.

Getting started with the native provisioner is simple:

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

.. _installationAzure:

Azure
-----

.. include:: azure.rst

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
