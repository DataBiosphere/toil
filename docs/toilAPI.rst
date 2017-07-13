Toil API
********

This section describes the API for writing Toil workflows in Python.

Job methods
-----------
Jobs are the units of work in Toil which are composed into workflows.

.. autoclass:: toil.job.Job
   :members:

Job.FileStore
-------------
The FileStore is an abstraction of a Toil run's shared storage.

.. autoclass:: toil.fileStore::FileStore
   :members:

Job.Runner
----------
The Runner contains the methods needed to configure and start a Toil run.

.. autoclass:: toil.job::Job.Runner
   :members:

Toil
----
The Toil class provides for a more general way to configure and start a Toil run.

.. autoclass:: toil.common::Toil
   :members:

Job.Service
-----------
The Service class allows databases and servers to be spawned within a Toil workflow.

.. autoclass:: toil.job::Job.Service
   :members:

FunctionWrappingJob
-------------------
The subclass of Job for wrapping user functions.


.. autoclass:: toil.job::FunctionWrappingJob
   :members:

JobFunctionWrappingJob
----------------------
The subclass of FunctionWrappingJob for wrapping user job functions.

.. autoclass:: toil.job::JobFunctionWrappingJob
   :members:

EncapsulatedJob
---------------
The subclass of Job for *encapsulating* a job, allowing a subgraph of jobs to be treated as a single job.

.. autoclass:: toil.job::EncapsulatedJob
   :members:

Promise
-------
The class used to reference return values of jobs/services not yet run/started.

.. autoclass:: toil.job::Promise
   :members:

.. autoclass:: toil.job::PromisedRequirement
   :members:

Exceptions
----------
Toil specific exceptions.

.. autoexception:: toil.job::JobException
   :members:

.. autoexception:: toil.job::JobGraphDeadlockException
   :members:

.. autoexception:: toil.jobStores.abstractJobStore::ConcurrentFileModificationException
   :members:

.. autoexception:: toil.jobStores.abstractJobStore::JobStoreExistsException
   :members:

.. autoexception:: toil.jobStores.abstractJobStore::NoSuchFileException
   :members:

.. autoexception:: toil.jobStores.abstractJobStore::NoSuchJobException
   :members:

.. autoexception:: toil.jobStores.abstractJobStore::NoSuchJobStoreException
   :members:
