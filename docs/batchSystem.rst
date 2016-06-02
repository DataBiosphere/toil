.. _batchsysteminterface:

The batch system interface
==========================

The batch system interface is used by Toil to abstract over different ways of running
batches of jobs, for example GridEngine, Mesos, Parasol and a single node. The 
:class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem` API is implemented to
run jobs using a given job management system, e.g. Mesos.

.. autoclass:: toil.batchSystems.abstractBatchSystem::AbstractBatchSystem
   :members:  