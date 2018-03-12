.. _jobStoreInterface:

Job Store API
=============

The job store interface is an abstraction layer that that hides the specific details of file storage,
for example standard file systems, S3, etc. The :class:`~toil.jobStores.abstractJobStore.AbstractJobStore`
API is implemented to support a give file store, e.g. S3. Implement this API to support a new file store.

.. autoclass:: toil.jobStores.abstractJobStore::AbstractJobStore
   :members:
