The job store interface
=======================

Implementing the job store interface tutorial
*********************************************

The job store interface is used by Toil to abstract over different file stores, 
for example standard file systems, S3, etc. This tutorial will guide you through
the job store (:class:`toil.jobStores.abstractJobStore.AbstractJobStore`) functions
and how to implement them to support a new file store.

TODO

Toil Abstract Job Store API
***************************

The :class:`toil.jobStores.abstractJobStore.AbstractJobStore` API is implemented to
support a give file store, e.g. S3.

.. autoclass:: toil.batchSystems.abstractJobStore::AbstractJobStore
   :members:  