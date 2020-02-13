
.. _cloudOverview:

Running in the Cloud
====================

Toil supports Amazon Web Services (AWS) and Google Compute Engine (GCE) in the cloud and has autoscaling capabilities
that can adapt to the size of your workflow, whether your workflow requires 10 instances or 20,000.

Toil does this by creating a virtual cluster with `Apache Mesos`_.  `Apache Mesos`_ requires a leader node to coordinate
the workflow, and worker nodes to execute the various tasks within the workflow.  As the workflow runs, Toil will
"autoscale", creating and terminating workers as needed to meet the demands of the workflow.

Once a user is familiar with the basics of running toil locally (specifying a :ref:`jobStore <jobStoreOverview>`, and
how to write a toil script), they can move on to the guides below to learn how to translate these workflows into cloud
ready workflows.

.. _cloudProvisioning:

Managing a Cluster of Virtual Machines (Provisioning)
-----------------------------------------------------

Toil can launch and manage a cluster of virtual machines to run using the *provisioner* to run a workflow
distributed over several nodes. The provisioner also has the ability to automatically scale up or down the size of
the cluster to handle dynamic changes in computational demand (autoscaling).  Currently we have working provisioners
with AWS and GCE (Azure support has been deprecated).

Toil uses `Apache Mesos`_ as the :ref:`batchSystemOverview`.

See here for instructions for :ref:`runningAWS`.

See here for instructions for :ref:`runningGCE`.

.. _Apache Mesos: https://mesos.apache.org/gettingstarted/

.. _cloudJobStore:

Storage (Toil jobStore)
-----------------------

Toil can make use of cloud storage such as AWS or Google buckets to take care of storage needs.

This is useful when running Toil in single machine mode on any cloud platform since it allows you to
make use of their integrated storage systems.

.. FIXME add link to batch system overview for single machine mode

For an overview of the job store see :ref:`jobStoreOverview`.

For instructions configuring a particular job store see:

- :ref:`awsJobStore`

- :ref:`googleJobStore`

Cloud Platforms
===============

.. toctree::
    amazon
    gce
    clusterUtils
