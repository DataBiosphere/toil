.. Toil documentation master file, created by
   sphinx-quickstart on Tue Aug 25 12:37:16 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Toil
====

Toil is a workflow engine entirely written in Python. It features:

* Easy installation, e.g. ``pip install toil``.

* `Common Workflow Language`_ (`CWL`_) support

  Complete support for the draft-3 CWL specification, allowing it to execute
  CWL workflows.

* `Workflow Description Language`_ (`WDL`_) support

  Draft support for the WDL specification, allowing it to execute
  WDL workflows.

* Cross platform support

  Develop and test on your laptop then deploy on any of the following:

  - Commercial clouds:
    - `Amazon Web Services`_ (including the `spot market`_)
    - `Microsoft Azure`_
  - Private clouds:
    - `OpenStack`_
  - High Performance Computing Environments:
    - `GridEngine`_
    - `Apache Mesos`_
    - `Parasol`_
    - Individual multi-core machines

* A small API

  Easily mastered, the Python user API for defining and running workflows is built upon one core class.

* Complete file and stream management:

  Temporary and persistent file management that abstracts the details of the
  underlying file system, providing a uniform interface regardless of
  environment. Supports both atomic file transfer and streaming interfaces, and
  provides encryption of user data.

* Scalability:

  Toil can easily handle workflows concurrently using hundreds of nodes and
  thousands of cores.

* Robustness:

  Toil workflows support arbitrary worker and leader failure, with strong
  check-pointing that always allows resumption.

* Efficiency:

  Caching, fine grained, per task, resource requirement specifications, and
  support for the AWS spot market mean workflows can be executed with little
  waste.

* Declarative and dynamic workflow creation:

  Workflows can be declared statically, but new jobs can be added dynamically
  during execution within any existing job, allowing arbitrarily complex
  workflow graphs with millions of jobs within them.

* Support for databases and services:

  For example, Apache Spark clusters can be created quickly and easily
  integrated within a toil workflow as a service, with precisely defined time
  start and end times that fits with the flow of other jobs in the workflow.

* Open Source: An Apache license allows unrestricted use, incorporation and modification.

.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Parasol: https://users.soe.ucsc.edu/~donnak/eng/parasol.htm
.. _Apache Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/
.. _Microsoft Azure: https://azure.microsoft.com
.. _Amazon Web Services: https://aws.amazon.com/
.. _OpenStack: https://www.openstack.org/
.. _CWL: http://commonwl.org
.. _Common Workflow Language: http://commonwl.org
.. _WDL: https://github.com/broadinstitute/wdl
.. _Workflow Description Language: https://github.com/broadinstitute/wdl

Contents:

.. toctree::
   :maxdepth: 3

   installation
   running
   cli
   developing
   toilAPI
   architecture
   batchSystem
   jobStore
   generated_rst/modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
