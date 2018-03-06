
.. _cloudOverview:

Running Toil in the Cloud
=========================

Toil can use cloud platforms in several different ways.

.. _cloudJobStore:

Storage (job store)
-------------------

Toil can make use of cloud storage such as Azure, AWS, or Google buckets to take of care of storage needs.

This is useful when running Toil in single machine mode on any cloud platform since it allows you to
make use of their integrated storage systems.

.. FIXME add link to batch system overview for single machine mode

For an overview of the job store see :ref:`jobStoreOverview`.

For instructions configuring a particular job store see:

- :ref:`awsJobStore`

- :ref:`azureJobStore`

- :ref:`googleJobStore`

.. _cloudProvisioning:

Managing a Cluster of Virtual Machines (provisioning)
-----------------------------------------------------

Toil can launch and manage a cluster of virtual machines to run using the *provisioner* to run a workflow
distributed over several nodes. The provisioner also has the ability to automatically scale up or down the size of
the cluster to handle dynamic changes in computational demand (autoscaling).

Currently we have working provisioners with AWS and GCE with Azure coming soon.

Toil uses `Apache Mesos`_ as the :ref:`batchSystemOverview`.

See here for instructions for :ref:`runningAWS`.

See here for instructions for :ref:`runningGCE`.

See `here <https://github.com/BD2KGenomics/toil/pull/1912>`_ for latest details on development of the Azure Provisioner.

.. _Apache Mesos: https://mesos.apache.org/gettingstarted/

Cloud Platforms
===============

.. toctree::
   amazon
   azure
   gce
   openstack
   clusterUtils
