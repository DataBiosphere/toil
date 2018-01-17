.. highlight:: console

.. _installation-ref:

Installation
============

This document describes how to prepare for and install the Toil software. Note that we recommend running all the Toil
commands inside a Python `virtualenv`_. Instructions for installing and creating a Python virtual environment are
provided below.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/

.. _venvPrep:

Preparing your Python runtime environment
-----------------------------------------

Toil currently  supports only Python 2.7.  If you don't satisfy this requirement, consider using anaconda_ to create
an alternate Python 2.7 installation.

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

Basic Installation
------------------

If you need only the basic version of Toil, it can be easily installed using pip::

    $ pip install toil

Now you're ready to run :ref:`your first Toil workflow <quickstart>`!

(If you need any of the extra features don't do this yet and instead skip to the next section.)

.. _extras:

Installing Toil with extra features
-----------------------------------

Some optional features, called *extras*, are not included in the basic
installation of Toil. To install Toil with all its bells and whistles, first
install any necessary headers and libraries (`python-dev`_, `libffi-dev`_). Then run

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


.. _prepare_aws-ref:

Preparing your AWS environment
------------------------------

To use Amazon Web Services (AWS) to run Toil or to just use S3 to host the files 
during the computation of a workflow, first set up and configure an account with AWS.

#. If necessary, create and activate an `AWS account`_

#. Create a `key pair`_ in the availability zone of your choice (our examples use ``us-west-2a``).

#. Follow `Amazon's instructions`_ to create an SSH key and import it into EC2.

#. Finally, you will need to `install`_ and `configure`_ the AWS Command Line Interface (CLI).


.. _AWS account: https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/
.. _key pair: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html
.. _Amazon's instructions : http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#how-to-generate-your-own-key-and-import-it-to-aws
.. _install: http://docs.aws.amazon.com/cli/latest/userguide/installing.html
.. _configure: http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html


.. _prepare_azure-ref:

Preparing your Azure environment
--------------------------------

Follow the steps below to prepare your Azure environment for running a Toil workflow.

#. Create an `Azure account`_.


#. Make sure you have an SSH RSA public key, usually stored in
   ``~/.ssh/id_rsa.pub``. If not, you can use ``ssh-keygen -t rsa`` to create
   one.

.. _Azure account: https://azure.microsoft.com/en-us/free/


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
additional packages that Toil needs to be tested and built but not to be run.)

::

    $ make prepare

Now, we can install Toil in development mode (such that changes to the
source code will immediately affect the virtualenv)::

    $ make develop

Or, to install with support for all optional :ref:`extras`::

    $ make develop extras=[aws,mesos,azure,google,encryption,cwl]

To build the docs, run ``make develop`` with all extras followed by

::

    $ make docs

    
To run a quick batch of tests (this should take less than 30 minutes)

::

	$ export TOIL_TEST_QUICK=True; make test
	
For more information on testing see :ref:`runningTests`.
