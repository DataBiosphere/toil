The batch system interface
==========================

Implementing the batch system interface tutorial
************************************************

The batch system interface is used by Toil to abstract over different ways of running
batches of jobs, for example GridEngine, Mesos, Parasol and a single node.

This tutorial will guide you through
the batch system class (:class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem`) functions
and how to implement them to support a new batch system.

TODO

Toil Abstract Batch System API
******************************

The :class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem` API is implemented to
run jobs using a given job management system, e.g. Mesos.

.. autoclass:: toil.batchSystems.abstractBatchSystem::AbstractBatchSystem
   :members:  