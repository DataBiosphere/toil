.. _api-jobfunctions:

Toil Job API
************

Functions to wrap jobs and return values (promises).

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
