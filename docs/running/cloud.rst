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

.. _installProvisioner:

Toil Provisioner
----------------
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
