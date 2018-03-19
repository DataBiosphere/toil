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

Toil currently supports only Python 2.7 and requires a virtualenv to be active to install.

If not already present, please install the latest Python ``virtualenv`` using pip_.::

    $ sudo pip install virtualenv

And create a virtual environment called ``venv`` in your home directory.::

    $ virtualenv ~/venv

.. _pip: https://pip.readthedocs.io/en/latest/installing/

If the user does not have root privileges, there are a few more steps, but one can download a specific virtualenv
package directly, untar the file, create, and source the virtualenv (version 15.1.0 as an example) using::

    $ curl -O https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz
    $ tar xvfz virtualenv-15.1.0.tar.gz
    $ cd virtualenv-15.1.0
    $ python virtualenv.py ~/venv

Now that you've created your virtualenv, activate your virtual environment.::

    $ source ~/venv/bin/activate

Basic Installation
------------------

If you need only the basic version of Toil, it can be easily installed using pip::

    $ pip install toil

Now you're ready to run :ref:`your first Toil workflow <quickstart>`!

(If you need any of the extra features don't do this yet and instead skip to the next section.)

.. _extras:

Installing Toil with extra features
-----------------------------------

.. _python-dev:
.. topic:: Python headers and static libraries

   Needed for the ``mesos``, ``aws``, ``google``, ``azure``, and ``encryption`` extras.

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

Some optional features, called *extras*, are not included in the basic
installation of Toil. To install Toil with all its bells and whistles, first
install any necessary headers and libraries (`python-dev`_, `libffi-dev`_). Then run::

    $ pip install toil[aws,mesos,azure,google,encryption,cwl]

or::

    $ pip install toil[all]

Here's what each extra provides:

+----------------+------------------------------------------------------------+
| Extra          | Description                                                |
+================+============================================================+
| ``all``        | Installs all extras (though htcondor is linux-only and     |
|                | will be skipped if not on a linux computer).               |
+----------------+------------------------------------------------------------+
| ``aws``        | Provides support for managing a cluster on Amazon Web      |
|                | Service (`AWS`_) using Toil's built in :ref:`clusterRef`.  |
|                | Clusters can scale up and down automatically.              |
|                | It also supports storing workflow state.                   |
+----------------+------------------------------------------------------------+
| ``google``     | Experimental. Stores workflow state in `Google Cloud       |
|                | Storage`_.                                                 |
+----------------+------------------------------------------------------------+
| ``azure``      | Stores workflow state in `Microsoft Azure`_.               |
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
|                | Azure and AWS job stores. This extra requires the          |
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

.. _AWS: https://aws.amazon.com/
.. _Apache Mesos: https://mesos.apache.org/gettingstarted/
.. _Google Cloud Storage: https://cloud.google.com/storage/
.. _Microsoft Azure: https://azure.microsoft.com/
.. _Workflow Description Language: https://software.broadinstitute.org/wdl/
.. _Cryptography: https://cryptography.io/en/latest/installation/
.. _Homebrew: http://brew.sh/

.. _prepareAzure:

Preparing your Azure environment
--------------------------------

Follow the steps below to prepare your Azure environment for running a Toil workflow.

#. Create an `Azure account`_ and to use the job store make an `Azure storage account`_.

#. Locate your Azure storage account key and then store it in one of the following locations:
    - ``AZURE_ACCOUNT_KEY_<account>`` environment variable
    - ``AZURE_ACCOUNT_KEY`` environment variable
    - or finally in ``~/.toilAzureCredentials.`` with the format ::

         [AzureStorageCredentials]
         accountName1=ACCOUNTKEY1==
         accountName2=ACCOUNTKEY2==

   These locations are searched in the order above, which can be useful if you work with multiple
   accounts.

#. Create an SSH keypair if one doesn't exist (see :ref:`sshKey`).

#. Create an `Azure active directory and service principal`_ to create the following credentials:
    - Client ID (also known as an Application ID)
    - Subscription ID
    - secret (also known as the authentication key)
    - Tenant ID (also known as a Directory ID)

   These Azure credentials need to be stored a location where the Ansible scripts can read them.
   There are multiple options for doing so as described here_, one of which is to create a
   file called `~/.azure/credentials` with the format ::

      [default]
      subscription_id=22222222-2222-2222-2222-222222222222
      client_id=1111111-111-1111-111-111111111
      secret=abc123
      tenant=33333333-3333-3333-3333-333333333333

.. _Azure account: https://azure.microsoft.com/en-us/free/
.. _here: http://docs.ansible.com/ansible/latest/guide_azure.html#providing-credentials-to-azure-modules.o/docs/py2or3.html
.. _Azure storage account: https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal
.. _Azure active directory and service principal: https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal

.. _building_from_source-ref:

Building from source
--------------------

If developing with Toil, you will need to build from source. This allows changes you
make to Toil to be reflected immediately in your runtime environment.

First, clone the source::

   $ git clone https://github.com/BD2KGenomics/toil
   $ cd toil

Then, create and activate a virtualenv::

   $ virtualenv venv
   $ . venv/bin/activate

From there, you can list all available Make targets by running ``make``.
First and foremost, we want to install Toil's build requirements. (These are
additional packages that Toil needs to be tested and built but not to be run.)::

    $ make prepare

Now, we can install Toil in development mode (such that changes to the
source code will immediately affect the virtualenv)::

    $ make develop

Or, to install with support for all optional :ref:`extras`::

    $ make develop extras=[aws,mesos,azure,google,encryption,cwl]

Or::

    $ make develop extras=[all]

To build the docs, run ``make develop`` with all extras followed by::

    $ make docs

To run a quick batch of tests (this should take less than 30 minutes)::

	$ export TOIL_TEST_QUICK=True; make test

For more information on testing see :ref:`runningTests`.
