.. _runningOverview:

Introduction
============

Toil runs in various environments, including :ref:`locally <fileJobStore>` and :ref:`in the cloud <cloudOverview>`
(Amazon Web Services and Google Compute Engine).  Toil also supports two DSLs: :ref:`CWL <cwl>` and
(Amazon Web Services and Google Compute Engine).  Toil also supports two DSLs: :ref:`CWL <cwl>` and
:ref:`WDL <wdl>` (experimental).

Toil is built in a modular way so that it can be used on lots of different systems, and with different configurations.
The three configurable pieces are the

 - :ref:`jobStoreInterface`: A filepath or url that can host and centralize all files for a workflow (e.g. a local folder, or an AWS s3 bucket url).
 - :ref:`batchSystemInterface`: Specifies either a local single-machine or a currently supported HPC environment (lsf, parasol, mesos, slurm, torque, htcondor, or gridengine).  Mesos is a special case, and is launched for cloud environments.
 - :ref:`provisionerOverview`: For running in the cloud only.  This specifies which cloud provider provides instances to do the "work" of your workflow.

.. _jobStoreOverview:

Job Store
---------

The job store is a storage abstraction which contains all of the information used in a Toil run. This centralizes all
of the files used by jobs in the workflow and also the details of the progress of the run. If a workflow crashes
or fails, the job store contains all of the information necessary to resume with minimal repetition of work.

Several different job stores are supported, including the file job store and cloud job stores.

.. _fileJobStore:

File Job Store
~~~~~~~~~~~~~~

The file job store is for use locally, and keeps the workflow information in a directory on the machine where the
workflow is launched.  This is the simplest and most convenient job store for testing or for small runs.

For an example that uses the file job store, see :ref:`quickstart`.

Cloud Job Stores
~~~~~~~~~~~~~~~~

Toil currently supports the following cloud storage systems as job stores:

 - :ref:`awsJobStore`: An AWS S3 bucket formatted as "aws:<zone>:<bucketname>" where only numbers, letters, and dashes are allowed in the bucket name.  Example: `aws:us-west-2:my-aws-jobstore-name`.
 - :ref:`googleJobStore`: A Google Cloud Storage bucket formatted as "gce:<zone>:<bucketname>" where only numbers, letters, and dashes are allowed in the bucket name.  Example: `gce:us-west2-a:my-google-jobstore-name`.

These use cloud buckets to house all of the files. This is useful if there are several different
worker machines all running jobs that need to access the job store.

.. _batchSystemOverview:

Batch System
------------

A Toil batch system is either a local single-machine (one computer) or a currently supported
HPC cluster of computers (lsf, parasol, mesos, slurm, torque, htcondor, or gridengine).  Mesos
is a special case, and is launched for cloud environments.  These environments manage individual
worker nodes under a leader node to process the work required in a workflow.  The leader and its
workers all coordinate their tasks and files through a centralized job store location.

See :ref:`batchSystemInterface` for a more detailed description of different batch systems.

.. _provisionerOverview:

Provisioner
-----------

The Toil provisioner provides a tool set for running a Toil workflow on a particular cloud platform.

The :ref:`clusterRef` are command line tools used to provision nodes in your desired cloud platform.
They allows you to launch nodes, ssh to the leader, and rsync files back and forth.

For detailed instructions for using the provisioner see :ref:`runningAWS` or :ref:`runningGCE`.
