Installation
============

Prerequisites
-------------

* Python 2.7.x

* pip_ > 7.x

.. _pip: https://pip.readthedocs.org/en/latest/installing.html

.. _installation-ref:

Basic Installation
------------------

To setup a basic Toil installation use

::
    
    pip install toil

Toil uses setuptools' extras_ mechanism for dependencies of optional features
like support for Mesos or AWS. To install Toil with all bells and whistles use

::

   pip install toil[aws,mesos,azure,encryption]

.. _extras: https://pythonhosted.org/setuptools/setuptools.html#declaring-extras-optional-features-with-their-own-dependencies

Here's what each extra provides:

* The ``aws`` extra provides support for storing workflow state in Amazon AWS.
  This extra has no native dependencies.

* The ``azure`` extra stores workflow state in Microsoft Azure Storage. This
  extra has no native dependencies.

* The ``mesos`` extra provides support for running Toil on an `Apache Mesos`_
  cluster. Note that running Toil on SGE (GridEngine), Parasol or a single
  machine does not require an extra. The ``mesos`` extra requires the following
  native dependencies:

  * :ref:`Apache Mesos <mesos>`
  * :ref:`Python headers and static libraries <python-dev>`

* The ``encryption`` extra provides client-side encryption for files stored in
  the Azure and AWS job stores. This extra requires the following native
  dependencies:
  
  * :ref:`Python headers and static libraries <python-dev>`
  * :ref:`Libffi headers and library <libffi-dev>`
  
.. _mesos:
.. topic:: Apache Mesos

   Only needed for the ``mesos`` extra. Toil has been tested with version
   0.25.0. Mesos can be installed on Linux by following the instructions on
   https://open.mesosphere.com/getting-started/install/. The `Homebrew`_
   package manager has a formula for Mesos such that running ``brew install
   mesos`` is probably the easiest way to install Mesos on OS X. This assumes,
   of course, that you already have `Xcode`_ and `Homebrew`_.

   Please note that even though Toil depends on the Python bindings for Mesos,
   it does not explicitly declare that dependency and they will **not** be
   installed automatically when you run ``pip install toil[mesos]``. You need
   to install the bindings manually. The `Homebrew`_ formula for OS X installs
   them by default. On Ubuntu you will need to download the appropriate .egg
   from https://open.mesosphere.com/downloads/mesos/ and install it using
   ``easy_install -a <path_to_egg>``. Note that on Ubuntu Trusty you may need
   to upgrade ``protobuf`` via ``pip install --upgrade protobuf`` **before**
   running the above ``easy_install`` command.

.. _python-dev:
.. topic:: Python headers and static libraries

   Only needed for the ``mesos`` and ``encryption`` extras. The Python headers
   and static libraries can be installed on Ubuntu/Debian by running ``sudo
   apt-get install build-essential python-dev`` and accordingly on other Linux
   distributions. On Mac OS X, these headers and libraries are installed when
   you install the `Xcode`_ command line tools by running ``xcode-select
   --install``, assuming, again, that you have `Xcode`_ installed.

.. _libffi-dev:
.. topic:: Libffi headers and library

   `Libffi`_ is only needed for the ``encryption`` extra. To install `Libffi`_
   on Ubuntu, run ``sudo apt-get install libffi-dev``. On Mac OS X, run ``brew
   install libffi``. This assumes, of course, that you have `Xcode`_ and
   `Homebrew`_ installed.

.. _Apache Mesos: http://mesos.apache.org/

.. _Libffi: https://sourceware.org/libffi/

.. _Xcode: https://developer.apple.com/xcode/

.. _Homebrew: http://brew.sh/

Building & Testing
------------------

For developers and people interested in building the project from source the following
explains how to setup virtualenv to create an environment to use Toil in. 

After cloning the source and ``cd``-ing into the project root, create a virtualenv and activate it::

    virtualenv venv
    . venv/bin/activate

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
after installing any native dependencies listed in :ref:`installation-ref`.

::

   make develop extras=[aws,mesos,azure,encryption]

.. _development mode: https://pythonhosted.org/setuptools/setuptools.html#development-mode

To build the docs use

::

    make docs

To invoke the tests (unit and integration) use

::

   make test

Run an individual test with

::

   make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The default value for ``tests`` is ``"src"`` which includes all tests in the
``src`` subdirectory of the project root. Tests that require a particular
feature will be skipped implicitly. If you want to explicitly skip tests that
depend on a currently installed *feature*, use

::

   make test tests="-m 'not azure' src"

This will run only the tests that don't depend on the ``azure`` extra, even if
that extra is currently installed. Note the distinction between the terms
*feature* and *extra*. Every extra is a feature but there are features that are
not extras, the ``gridengine`` and ``parasol`` features fall into that
category. So in order to skip tests involving both the Parasol feature and the
Azure extra, the following can be used::

   make test tests="-m 'not azure and not parasol' src"

Running Mesos Tests
~~~~~~~~~~~~~~~~~~~

See :ref:`Apache Mesos <mesos>`. Be sure to create the virtualenv with
``--system-site-packages`` to include the Mesos Python bindings. Verify by
activating the virtualenv and running .. ``pip list | grep mesos``. On OS X,
this may come up empty. To fix it, run the following::

    for i in /usr/local/lib/python2.7/site-packages/*mesos*; do ln -snf $i venv/lib/python2.7/site-packages/ ; done
    
Cloud Installation
==================

.. _installationAWS:

Installation on AWS for Distributed Computing
---------------------------------------------
We use CGCloud_ to provision instances and clusters in AWS. Thorough documentation of CGCloud_ can be found
in the CGCloud-core_ and CGCloud-toil_ documentation. Brief steps will be provided to those
interested in using CGCloud_ for provisioning.

 .. _CGCloud: https://github.com/BD2KGenomics/cgcloud/
 .. _CGCloud-core: https://github.com/BD2KGenomics/cgcloud/blob/master/core/README.rst
.. _CGCloud-toil: https://github.com/BD2KGenomics/cgcloud/blob/master/toil/README.rst

CGCloud in a Nutshell
~~~~~~~~~~~~~~~~~~~~~
Setting up clusters with CGCloud_ has the benefit of coming pre-packaged with Toil and Mesos, our preferred
batch system for running on AWS. If you encounter any issues following these steps, check official documentation
which contains Troubleshooting sections.

1. ``virtualenv ~/cgcloud``
2. ``source ~/cgcloud/bin/activate``
3. ``pip install cgcloud-core``
4. ``pip install cgcloud-toil``
5. Add the following to your ``~/.profile``, use the appropriate region for your account.
    5a. ``export CGCLOUD_ZONE=us-west-2a``

    5b. ``export CGCLOUD_PLUGINS="cgcloud.toil:$CGCLOUD_PLUGINS"``
6. Setup credentials for your AWS account in ``~/.aws/credentials``::

    [default]
    aws_access_key_id=PASTE_YOUR_FOO_ACCESS_KEY_ID_HERE
    aws_secret_access_key=PASTE_YOUR_FOO_SECRET_KEY_ID_HERE
    region=us-west-2

7. Register your SSH key. You can create one with ``ssh-keygen``.
    7a. ``cgcloud register-key ~/.ssh/id_rsa.pub``
8. Create a template *toil-box* which will contain necessary prerequisites
    8a. ``cgcloud create -IT toil-box``
9. Create a small leader/worker cluster
    9a. ``cgcloud create-cluster toil -s 2 -t m3.large``
10. SSH into the leader: ``cgcloud ssh toil-leader``

At this point, any toil script can be run on the distributed AWS cluster following instructions in :ref:`runningAWS`.

Installation on Azure
---------------------

TODO

.. _installationOpenStack:

Installation on OpenStack
-------------------------

Our group is working to expand distributed cluster support to OpenStack by providing
convenient Docker containers to launch Mesos from. Currently, OpenStack nodes can be setup
to run Toil in **singleMachine** mode following the basic installation instructions: :ref:`installation-ref`


