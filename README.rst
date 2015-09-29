Toil is a massively scalable pipeline management system, written entirely in
Python, and designed around the principles of functional programming.

Toil runs as easily on a laptop as it does on a bare-metal cluster or in the
cloud, thanks to support for many batch systems, including `GridEngine`_,
Parasol_, and a custom Mesos_ framework.

Toil is robust, and designed to run in unreliable computing environments like
Amazon's `spot market`_. Towards this goal, Toil does not rely on a shared file
system. Instead, Toil abstracts a pipeline's global storage as a job store that
can reside on a locally attached file system or within an object store like
Amazon S3. The result of this abstraction is a robust system that can be
resumed even after an unexpected shutdown of every node in the cluster, even if
that event resulted in the loss of all locally stored data.

Writing a Toil script requires only a knowledge of basic Python, with Toil
*jobs* as the unit of work in a Toil workflow. A job can dynamically spawn
other jobs as needed, leading to an intuitive and powerful control over the
pipeline. File management is through an immutable interface that makes it
simple and easy to reason about the state of the workflow.

.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Parasol: https://users.soe.ucsc.edu/~donnak/eng/parasol.htm
.. _Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/

Prerequisites
=============

* Python 2.7.x

* pip_ > 7.x

.. _pip: https://pip.readthedocs.org/en/latest/installing.html

Installation
============

Toil uses setuptools' extras_ mechanism for dependencies of optional features
like support for Mesos or AWS. To install Toil with all bells and whistles use

::

   pip install toil[aws,mesos,azure,encryption]

.. _extras: https://pythonhosted.org/setuptools/setuptools.html#declaring-extras-optional-features-with-their-own-dependencies

Here's what each extra provides:

* The ``aws`` extra provides support for storing workflow state in Amazon AWS.

* The ``azure`` extra provides support for storing workflow state in Microsoft
  Azure Storage.

* The ``mesos`` extra provides support for running Toil on an `Apache Mesos`_
  cluster. Note that running Toil on SGE (GridEngine), Parasol or a single
  machine is enabled by default and does not require an extra.

* The ``encryption`` extra provides client-side encryption for files stored in
  the job store. Currently, client-side encryption is only used by the Azure
  job store. The AWS job store uses server side encryption and is not affected
  by this extra. Note that if you install Toil without the encryption extra,
  files in an Azure job store will **not** be encrypted, even if you provide
  encryption keys (see issue #407).

.. _Apache Mesos: http://mesos.apache.org/gettingstarted/

Building & Testing
==================

Simply running

::

   make

from the project root will print a description of the available Makefile
targets.

If cloning from GitHub, running

::

   make develop

will install Toil in *editable* mode, also known as `development mode`_. Just
like with a regular install, you may specify extras to use in development mode

::

   make develop extras=[aws,mesos,azure,encryption]

.. _development mode: https://pythonhosted.org/setuptools/setuptools.html#development-mode

To invoke the tests (unit and integration) use

::

   make test

Run an individual test with

::

   make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The default value for ``tests`` is ``"src"`` which includes all tests in the
``src`` subdirectory of the project root. Tests that require a particular
feature will be skipped implicitly. If you want to explicitly skip tests that
depend on a currently installed feature, use

::

   make test tests="-m 'not azure' src"

This will run only the tests that don't depend on the ``azure`` extra, even if
that extra is currently installed. Note the distinction between the terms
*feature* and *extra*. Every extra is a feature but there are features that are
not extras, e.g. ``gridengine`` and ``parasol``. One last example: In order to
skip tests involving Parasol and Azure, the following can be used::

   make test tests="-m 'not azure and not parasol' src"
