
.. highlight:: console

.. _installation-ref:

Installation
============

This document describes how to prepare for and install Toil. Note that Toil requires that the user run all commands
inside of a Python `virtualenv`_. Instructions for installing and creating a Python virtual environment are provided
below.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/

.. _venvPrep:

Preparing Your Python Runtime Environment
-----------------------------------------

It is recommended to install Toil into a virtual environment. This is useful
for automatically deploying Python workflows, and is the only supported way to
install Toil for Toil development.

If not already present, please install the latest Python ``virtualenv`` using pip_::

    $ pip install --user virtualenv

And create a virtual environment called ``venv`` somewhere convenient, usch as under ``~``::

    $ python3 -m virtualenv ~/venv

.. _pip: https://pip.readthedocs.io/en/latest/installing/

Now that you've created your virtualenv, activate your virtual environment::

    $ source ~/venv/bin/activate

Basic Installation
------------------

If all you want to do is run workflows, you can install Toil with frontends for CWL and WDL like this::

    $ pip install toil[cwl,wdl]

If you need only the basic version of Toil for Python workflows, it can be installed like this::

    $ pip install toil

Now you're ready to run :ref:`your first Toil workflow <quickstart>`!

(If you need any more extra features, such as AWS support, don't do this yet and instead see :ref:`extras`.)

.. _extras:

Installing Toil with Extra Features
-----------------------------------

Some optional features, called *extras*, are not included in the basic
installation of Toil. To install Toil with all its bells and whistles, first
install any necessary headers and libraries (`python-dev`_, `libffi-dev`_). Then run ::

    $ pip install toil[aws,google,mesos,encryption,cwl,wdl,kubernetes,server]

or ::

    $ pip install toil[all]

Here's what each extra provides:

+----------------+------------------------------------------------------------+
| Extra          | Description                                                |
+================+============================================================+
| ``all``        | Installs all extras (though htcondor is linux-only and     |
|                | will be skipped if not on a linux computer).               |
+----------------+------------------------------------------------------------+
| ``aws``        | Provides support for managing a cluster on Amazon Web      |
|                | Service (`AWS`_) using Toil's built in :ref:`clusterUtils`.|
|                | Clusters can scale up and down automatically.              |
|                | It also supports storing workflow state.                   |
+----------------+------------------------------------------------------------+
| ``google``     | Experimental. Stores workflow state in `Google Cloud       |
|                | Storage`_.                                                 |
+----------------+------------------------------------------------------------+
| ``mesos``      | Provides support for running Toil on an `Apache Mesos`_    |
|                | cluster. Note that running Toil on other batch systems     |
|                | does not require an extra. The ``mesos`` extra requires    |
|                | the following native dependencies:                         |
|                |                                                            |
|                | * `Apache Mesos`_ (Tested with Mesos v1.0.0)               |
|                | * :ref:`Python headers and static libraries <python-dev>`  |
|                |                                                            |
|                | .. important::                                             |
|                |    If launching toil remotely on a mesos instance,         |
|                |    to install Toil with the ``mesos`` extra in a           |
|                |    virtualenv, be sure to create that virtualenv with the  |
|                |    ``--system-site-packages`` flag (only use remotely!)::  |
|                |                                                            |
|                |       $ virtualenv ~/venv --system-site-packages           |
|                |                                                            |
|                |    Otherwise, you'll see something like this:              |
|                |                                                            |
|                |    .. code-block:: python                                  |
|                |                                                            |
|                |        ImportError: No module named mesos.native           |
|                |                                                            |
+----------------+------------------------------------------------------------+
| ``htcondor``   | Support for the htcondor batch system.  This currently is  |
|                | a linux only extra.                                        |
+----------------+------------------------------------------------------------+
| ``encryption`` | Provides client-side encryption for files stored in the    |
|                | AWS job store. This extra requires the                     |
|                | following native dependencies:                             |
|                |                                                            |
|                | * :ref:`Python headers and static libraries <python-dev>`  |
|                | * :ref:`libffi headers and library <libffi-dev>`           |
+----------------+------------------------------------------------------------+
| ``cwl``        | Provides support for running workflows written using the   |
|                | `Common Workflow Language`_.                               |
+----------------+------------------------------------------------------------+
| ``wdl``        | Provides support for running workflows written using the   |
|                | `Workflow Description Language`_. This extra has no native |
|                | dependencies.                                              |
+----------------+------------------------------------------------------------+
| ``kubernetes`` | Provides support for running workflows written using a     |
|                | `Kubernetes`_ cluster.                                     |
+----------------+------------------------------------------------------------+
| ``server``     | Provides support for Toil server mode, including support   |
|                | for the GA4GH `Workflow Execution Service`_ API.           |
+----------------+------------------------------------------------------------+

Some extras can't install without additional dependencies. If you need any of these extras, make sure to install their dependencies first!

.. _python-dev:
.. topic:: Python headers and static libraries

   Needed for the ``mesos``, ``aws``, ``google``, and ``encryption`` extras.

   On Ubuntu::

      $ sudo apt-get install build-essential python-dev

   On macOS::

      $ xcode-select --install

.. _libffi-dev:
.. topic:: Encryption specific headers and library

   Needed for the ``encryption`` extra.

   On Ubuntu::

      $ sudo apt-get install libssl-dev libffi-dev

   On macOS::

      $ brew install libssl libffi

   Or see `Cryptography`_ for other systems.


.. _AWS: https://aws.amazon.com/
.. _Apache Mesos: https://mesos.apache.org/gettingstarted/
.. _Google Cloud Storage: https://cloud.google.com/storage/
.. _Workflow Description Language: https://software.broadinstitute.org/wdl/
.. _Workflow Execution Service: https://ga4gh.github.io/workflow-execution-service-schemas/docs/
.. _Cryptography: https://cryptography.io/en/latest/installation/
.. _Homebrew: http://brew.sh/
.. _Kubernetes: https://kubernetes.io/docs/concepts/overview/

.. _buildFromSource:

Building from Source
--------------------

If developing with Toil, you will need to build from source. This allows changes you
make to Toil to be reflected immediately in your runtime environment.

First, clone the source::

   $ git clone https://github.com/DataBiosphere/toil.git
   $ cd toil

Then, create and activate a virtualenv::

   $ virtualenv venv
   $ . venv/bin/activate

From there, you can list all available Make targets by running ``make``.
First and foremost, we want to install Toil's build requirements (these are
additional packages that Toil needs to be tested and built but not to be run)::

    $ make prepare

Now, we can install Toil in development mode (such that changes to the
source code will immediately affect the virtualenv)::

    $ make develop

Or, to install with support for all optional :ref:`extras`::

    $ make develop extras=[aws,mesos,google,encryption,cwl]

Or::

    $ make develop extras=[all]

To build the docs, run ``make develop`` with all extras followed by ::

    $ make docs

To run a quick batch of tests (this should take less than 30 minutes)
run ::

    $ export TOIL_TEST_QUICK=True; make test

For more information on testing see :ref:`runningTests`.
