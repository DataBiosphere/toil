Toil
====

Features
--------

Toil is a workflow engine written in 100% Python. It features:
    *  Easy installation 
        - e.g. "pip install toil".
    *  A small API 
        - Easily mastered, the user API is built upon one core class.
    *  Cross platform support 
        - Develop and test on your laptop then deploy on any of the following:
            - Commercial clouds
                + `Amazon Web Services`_ (including the `spot market`_)
                + `Microsoft Azure`_
            - Private clouds
                + `OpenStack`_
            - High Performance Computing Environments
                + `GridEngine`_
                + `Mesos`_
                + `Parasol`_
            - Individual multi-core machines
    *  Complete file and stream management 
        - Temporary and persistent file management that abstracts the details of the underlying file system, providing a uniform interface regardless of environment. Supports both atomic file transfer and streaming interfaces, and provides encryption of user data.
    *  Scalability 
        - Toil can easily handle workflows concurrently using hundreds of nodes and thousands of cores. 
    *  Robustness 
        - Toil workflows support arbitrary worker and leader failure, with strong check-pointing that always allows resumption.
    *  Efficiency
        - Caching and fine grained resource requirement specification and support for AWS spot market mean workflows can be executed with little waste. 
    *  Declarative and dynamic workflow creation
        - Workflows can be declared statically, but new jobs can be added dynamically during execution within any existing job, allowing arbitrarily complex workflow graphs with millions of jobs within them.
    *  Support for databases and services. 
        - For example, Apache Spark clusters can be created in seconds and easily integrated within a toil workflow as a service, with precisely defined time start and end times that fits with the flow of other jobs in the workflow.
    *  Draft Common Workflow Language (CWL) support
        - Complete support for the draft 2.0 CWL specification, allowing it to execute CWL workflows.
    *  Open source
        - An Apache license allows unrestricted use.
        
.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Parasol: https://users.soe.ucsc.edu/~donnak/eng/parasol.htm
.. _Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/
.. _Microsoft Azure: https://azure.microsoft.com
.. _Amazon Web Services: https://aws.amazon.com/
.. _OpenStack: https://www.openstack.org/

Prerequisites
-------------

* Python 2.7.x

* pip_ > 7.x

.. _pip: https://pip.readthedocs.org/en/latest/installing.html

.. _installation-ref:

Installation
------------

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

* The ``azure`` extra stores workflow state in Microsoft Azure Storage.

* The ``mesos`` extra provides support for running Toil on an `Apache Mesos`_
  cluster. Note that running Toil on SGE (GridEngine), Parasol or a single
  machine is enabled by default and does not require an extra.

* The ``encryption`` extra provides client-side encryption for files stored in
  the Azure and AWS job stores. Note that if you install Toil without the
  ``encryption`` extra, files in these job stores will **not** be encrypted,
  even if you provide encryption keys (see issue #407).

.. _Apache Mesos: http://mesos.apache.org/gettingstarted/

Scripting Quick Start
---------------------

Toil's Job class (:class:`toil.job.Job`) contains the Toil API, documented below.
To begin, consider this short toil script which illustrates defining a workflow:: 
    from toil.job import Job
         
    def helloWorld(message, memory="2G", cores=2, disk="3G"):
        return "Hello, world!, here's a message: %s" % message
            
    j = Job.wrapFn(helloWorld, "woot")
               
    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflow")
        print Job.Runner.startToil(j, options) #Prints Hello, world!, ...

The workflow consists of a single job, which calls the helloWorld function. The resource
requirements for that job are (optionally) specified by keyword arguments (memory, cores, disk).

The :class:`toil.job.Job.Runner` class handles the invocation of Toil workflows. 
It is fed an options object that configures the running of the workflow. 
This can be populated by an argument parser object using 
:func:`toil.job.Job.Runner.getDefaultArgumentParser`, allowing all these options to be specified 
via the command line to the script. See :ref:`tutorial-ref` for more details.

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

Install Mesos according to the official instructions. On OS X with Homebrew,
``brew install mesos`` should be sufficient.

Create the virtualenv with ``--system-site-packages`` to ensure that the Mesos
Python packages are included. Verify by activating the virtualenv and running
.. ``pip list | grep mesos``. On OS X, this may come up empty. To fix it, run the
following::

for i in /usr/local/lib/python2.7/site-packages/*mesos*; do ln -snf $i venv/lib/python2.7/site-packages/ ; done