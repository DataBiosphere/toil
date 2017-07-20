.. highlight:: console

.. _installation-ref:

Installation
============

This document describes how to prepare for and install the Toil software. Note that we recommend running all the Toil commands inside a Python `virtualenv`_. Instructions for installing and creating a Python virtual environment are provided below.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/

.. _venvPrep:

Preparation
-----------

Toil supports only Python 2.7.  If you don't satisfy this requirement, consider using anaconda_ to create an alternate Python 2.7 installation.

.. _anaconda: https://conda.io/docs/py2or3.html 

Install Python ``virtualenv`` using pip_.
::

    $ sudo pip install virtualenv

.. _pip: https://pip.readthedocs.io/en/latest/installing/

Create a virtual environment called ``venv`` in your home directory.
::

    $ virtualenv ~/venv

Or, if using an `Apache Mesos`_ cluster (see ``mesos`` in the Extras section below).
::

    $ virtualenv ~/venv --system-site-packages

Activate your virtual environment.
::

    $ source ~/venv/bin/activate
   

Basic installation
------------------

Toil can be easily installed using pip::

    $ pip install toil


.. _extras:

Extras
------

Some optional features, called *extras*, are not included in the basic
installation of Toil. To install Toil with all its bells and whistles, first
install any necessary headers and libraries if using the mesos or encryption
extras (`python-dev`_, `libffi-dev`_). Then run

::

    $ pip install toil[aws,mesos,azure,google,encryption,cwl]

Here's what each extra provides:

+----------------+------------------------------------------------------------+
| Extra          | Description                                                |
+================+============================================================+
| ``aws``        | Provides support for managing a cluster on Amazon Web      |
|                | Service (`AWS`_) using Toil's built in :ref:`clusterRef`.  |
|                | Clusters can scale up and down automatically.              |
|                | It also supports storing workflow state.                   |
|                | This extra has no native dependencies.                     |
+----------------+------------------------------------------------------------+
| ``google``     | Experimental. Stores workflow state in `Google Cloud       |
|                | Storage`_. This extra has no native dependencies.          |
+----------------+------------------------------------------------------------+
| ``azure``      | Stores workflow state in `Microsoft Azure`_. This          |
|                | extra has no native dependencies.                          |
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
|                |    If you want to install Toil with the ``mesos`` extra    |
|                |    in a virtualenv, be sure to create that virtualenv with |
|                |    the ``--system-site-packages`` flag::                   |
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
| ``encryption`` | Provides client-side encryption for files stored in the    |
|                | Azure and AWS job stores. This extra requires the          |
|                | following native dependencies:                             |
|                |                                                            |
|                | * :ref:`Python headers and static libraries <python-dev>`  |
|                | * :ref:`libffi headers and library <libffi-dev>`           |
+----------------+------------------------------------------------------------+
| ``cwl``        | Provides support for running workflows written using the   |
|                | `Common Workflow Language`_. This extra has no native      |
|                | dependencies.                                              |
+----------------+------------------------------------------------------------+

.. _AWS: https://aws.amazon.com/
.. _Apache Mesos: https://mesos.apache.org/gettingstarted/
.. _Google Cloud Storage: https://cloud.google.com/storage/
.. _Microsoft Azure: https://azure.microsoft.com/

.. _python-dev:
.. topic:: Python headers and static libraries

   Only needed for the ``mesos`` and ``encryption`` extras.

   On Ubuntu::

      $ sudo apt-get install build-essential python-dev

   On macOS::

      $ xcode-select --install

.. _libffi-dev:
.. topic:: Encryption specific headers and library

   Only needed for the ``encryption`` extra.

   On Ubuntu::

      $ sudo apt-get install libssl-dev libffi-dev

   On macOS::

      $ brew install libssl libffi

   Or see `Cryptography`_ for other systems.

.. _Cryptography: https://cryptography.io/en/latest/installation/

.. _Homebrew: http://brew.sh/
