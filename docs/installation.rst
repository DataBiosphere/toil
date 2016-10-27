.. _installation:

Installation
============

Prerequisites
-------------

* Python 2.7.x

* pip_ > 7.x

.. _pip: https://pip.readthedocs.org/en/latest/installing.html

.. _installation-ref:

Basic installation
------------------

To setup a basic Toil installation use

::

    pip install toil

Toil uses setuptools' extras_ mechanism for dependencies of optional features
like support for Mesos or AWS. To install Toil with all bells and whistles use

::

   pip install toil[aws,mesos,azure,google,encryption,cwl]

.. _extras: https://pythonhosted.org/setuptools/setuptools.html#declaring-extras-optional-features-with-their-own-dependencies

Here's what each extra provides:

* The ``aws`` extra provides support for storing workflow state in Amazon AWS.
  This extra has no native dependencies.

* The ``google`` extra is experimental and stores workflow state in
  Google Cloud Storage. This extra has no native dependencies.

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

* The ``cwl`` extra provides support for running workflows written using the
  `Common Workflow Language`_.

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
   
   If you intend to install Toil with the ``mesos`` extra into a virtualenv, be
   sure to create that virtualenv with

   ::

      virtualenv --system-site-packages

   Otherwise, Toil will not be able to import the ``mesos.native`` module.

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

.. _Common Workflow Language: http://commonwl.org

Building & testing
------------------

For developers and people interested in building the project from source the
following explains how to setup virtualenv to create an environment to use Toil
in.

After cloning the source and ``cd``-ing into the project root, create a
virtualenv and activate it::

    virtualenv venv
    . venv/bin/activate

Simply running

::

   make

from the project root will print a description of the available Makefile
targets.

Once you created and activated the virtualenv, the first step is to install the
build requirements. These are additional packages that Toil needs to be tested
and built, but not run::

   make prepare

Once the virtualenv has been prepared with the build requirements, running

::

   make develop

will create an editable installation of Toil and its runtime requirements in
the current virtualenv. The installation is called *editable* (also known as a
`development mode`_ installation) because changes to the Toil source code
immediately affect the virtualenv. Optionally, set the ``extras`` variable to
ensure that ``make develop`` installs support for optional extras. Consult
``setup.py`` for the list of supported extras. To install Toil in development
mode with all extras run

::

   make develop extras=[aws,mesos,azure,google,encryption,cwl]

.. _development mode: https://pythonhosted.org/setuptools/setuptools.html#development-mode

Note that some extras have native dependencies as listed in
:ref:`installation-ref`. Be sure to install them before running the above
command. If you get

::

   ImportError: No module named mesos.native

make sure you install Mesos and the Mesos egg as described in :ref:`Apache
Mesos <mesos>` and be sure to create the virtualenv with
``--system-site-packages``.

To build the docs, run ``make develop`` with all extras followed by

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

Running Mesos tests
~~~~~~~~~~~~~~~~~~~

See :ref:`Apache Mesos <mesos>`. Be sure to create the virtualenv with
``--system-site-packages`` to include the Mesos Python bindings. Verify by
activating the virtualenv and running .. ``pip list | grep mesos``. On OS X,
this may come up empty. To fix it, run the following::

    for i in /usr/local/lib/python2.7/site-packages/*mesos*; do ln -snf $i venv/lib/python2.7/site-packages/ ; done

Cloud installation
==================

.. _installationAWS:

Installation on AWS for distributed computing
---------------------------------------------
We use Toil's included AWS provisioner and CGCloud_ to provision instances and
clusters in AWS. More information on the AWS provisioner can be found in the
:ref:`Autoscaling` section. Thorough documentation of CGCloud_ can be found in
the CGCloud-core_ and CGCloud-toil_ documentation. Brief steps will be provided
to those interested in using CGCloud_ for provisioning.

.. _CGCloud: https://github.com/BD2KGenomics/cgcloud/
.. _CGCloud-core: https://github.com/BD2KGenomics/cgcloud/blob/master/core/README.rst
.. _CGCloud-toil: https://github.com/BD2KGenomics/cgcloud/blob/master/toil/README.rst

CGCloud in a nutshell
~~~~~~~~~~~~~~~~~~~~~
Setting up clusters with CGCloud_ has the benefit of coming pre-packaged with Toil and Mesos, our preferred
batch system for running on AWS. If you encounter any issues following these steps, check official documentation
which contains Troubleshooting sections.

#. Create and activate a virtualenv::

      virtualenv ~/cgcloud
      source ~/cgcloud/bin/activate

#. Install CGCloud and the CGCloud Toil plugin::

      pip install cgcloud-toil

#. Add the following to your ``~/.profile``, use the appropriate region for your account::

      export CGCLOUD_ZONE=us-west-2a
      export CGCLOUD_PLUGINS="cgcloud.toil:$CGCLOUD_PLUGINS"

#. Setup credentials for your AWS account in ``~/.aws/credentials``::

      [default]
      aws_access_key_id=PASTE_YOUR_FOO_ACCESS_KEY_ID_HERE
      aws_secret_access_key=PASTE_YOUR_FOO_SECRET_KEY_ID_HERE
      region=us-west-2

#. Register your SSH key. If you don't have one, create it with ``ssh-keygen``::

      cgcloud register-key ~/.ssh/id_rsa.pub

#. Create a template *toil-box* which will contain necessary prerequisites::

      cgcloud create -IT toil-box

#. Create a small leader/worker cluster::

      cgcloud create-cluster toil -s 2 -t m3.large

#. SSH into the leader::

      cgcloud ssh toil-leader

At this point, any Toil script can be run on the distributed AWS cluster following instructions in :ref:`runningAWS`.

.. _installationAzure:

Installation on Azure
---------------------

.. image:: http://azuredeploy.net/deploybutton.png
   :target: https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBD2KGenomics%2Ftoil%2Fmaster%2Fcontrib%2Fazure%2Fazuredeploy.json

While CGCloud does not currently support cloud providers other than Amazon, Toil comes with a cluster template to facilitate easy deployment of clusters running Toil on Microsoft Azure. The template allows these clusters to be created and managed through the Azure portal.

Detailed information about the template is available `here <https://github.com/BD2KGenomics/toil/blob/master/contrib/azure/README.md>`_.

To use the template to set up a Toil Mesos cluster on Azure, follow these steps.

1.  Make sure you have an SSH RSA public key, usually stored in ``~/.ssh/id_rsa.pub``. If not, you can use ``ssh-keygen -t rsa`` to create one.
2.  Click on the deploy button above, or navigate to ``https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBD2KGenomics%2Ftoil%2Fmaster%2Fcontrib%2Fazure%2Fazuredeploy.json`` in your browser.
3.  If necessary, sign into the Microsoft account that you use for Azure.
4.  You should be presented with a screen resembling the following:

    .. image:: azurescreenshot1.png

5.  Fill out the form on the far right (marked "1" in the image), giving the following information. Important fields for which you will want to override the defaults are in bold:

    1. **AdminUsername**: Enter a username for logging into the cluster. It is easiest to set this to match your username on your local machine.
    2. **AdminPassword**: Choose a strong root password. Since you will be configuring SSH keys, you will not actually need to use this password to log in in practice, so choose something long and complex and store it safely.
    3. **DnsNameForMastersPublicIp**: Enter a unique DNS name fragment to identify your cluster within your region. For example, if you are putting your cluster in ``westus``, and you choose ``awesomecluster``, your cluster's public IP would be assigned the name ``awesomecluster.westus.cloudapp.azure.com``.
    4. JumpboxConfiguration: If you would like, you can select to have either a Linux or Windows "jumpbox" with remote desktop software set up on the cluster's internal network. By default this is turned off, since it is unnecessary.
    5. DnsNameForJumpboxPublicIp: If you are using a jumpbox, enter another unique DNS name fragment here to set its DNS name. See ``DnsNameForMastersPublicIp`` above.
    6. **NewStorageAccountNamePrefix**: Enter a globally unique prefix to be used in the names of new storage accounts created to support the cluster. Storage account names must be 3 to 24 characters long, include only numbers and lower-case letters, and be globally unique. Since the template internally appends to this prefix, it must be shorter than the full 24 characters. Up to 20 should work.
    7. **AgentCount**: Choose how many agents (i.e. worker nodes) you want in the cluster. Be mindful of your Azure subscription limits on both VMs (20 per region by default) and total cores (also 20 per region by default); if you ask for more agents or more total cores than you are allowed, you will not get them all, errors will occur during template instantiation, and the resulting cluster will be smaller than you wanted it to be.
    8. **AgentVmSize**: Choose from the available VM instance sizes to determine how big each node will be. Again, be mindful of your Azure subscription's core limits. Also be mindful of how many cores and how much disk and memory your Toil jobs will need: if any requirement is greater than that provided by an entire node, a job may never be scheduled to run.
    9. MasterCount: Choose the number of "masters" or leader nodes for the cluster. By default only one is used, because although the underlying Mesos batch system supports master failover, currently Toil does not. You can increase this if multiple Toil jobs will be running and you want them to run from different leader nodes. Remember that the leader nodes also count against your VM and core limits.
    10. MasterVmSize: Select one of the available VM sizes to use for the leader nodes. Generally the leader node can be relatively small.
    11. MasterConfiguration: This is set to ``masters-are-not-agents`` by default, meaning that the leader nodes will not themselves run any jobs. If you are worried about wasting unused computing power on your leader nodes, you can set this to ``masters-are-agents`` to allow them to run jobs. However, this may slow them down for interactive use, making it harder to monitor and control your Toil workflows.
    12. JumpboxVmSize: If you are using a jumpbox, you can select a VM instance size for it to use here. Again, remember that it counts against your Azure subscription limits.
    13. ClusterPrefix: This prefix gets used to generate the internal hostnames of all the machines in the cluster. You can use it to give clusters friendly names to differentiate them. It has to be a valid part of a DNS name; you might consider setting it to match ``DnsNameForMastersPublicIp``. You can also leave it at the default.
    14. SwarmEnabled: You can set this to ``true`` to install Swarm, a system for scheduling Docker containers. Toil does not use Swarm, and Swarm has a tendency to allocate all the cluster's resources for itself, so you should probably leave this set to ``false`` unless you also find yourself needing a Swarm cluster.
    15. MarathonEnabled: You can set this to ``true`` to install Marathon, a scheduling system for persistent jobs run in Docker containers. It also has nothing to do with Toil, and should probably remains et to ``false``.
    16. ChronosEnabled: You can set this to ``true`` to install Chronos, which is a way to periodically run jobs on the cluster. Unless you find yourself needing this functionality, leave this set to ``false``. (All these extra frameworks are here because the Toil Azure template was derived from a Microsoft template for a generic Mesos cluster, offering these services.)
    17. ToilEnabled: You should leave this set to ``true``. If you set it to ``false``, Toil will not be installed on the cluster, which rather defeats the point.
    18. **SshRsaPublicKey**: Replace ``default`` with your SSH public key contents, beginning with ``ssh-rsa``. Paste in the whole line. Only one key is supported, and as the name suggests it must be an RSA key. This enables SSH key-based login on the cluster.
    19. GithubSource: If you would like to install Toil from a nonstandard fork on Github (for example, installing a version inclusing your own patches), set this to the Github fork (formatted as ``<username>/<reponame>``) from which Toil should be downloaded and installed. If not, leave it set to the default of ``BD2KGenomics/toil``.
    20. **GithubBranch**: To install Toil from a branch other than ``master``, enter the name of its branch here. For example, for the latest release of Toil 3.1, enter ``releases/3.1.x``. By default, you will get the latest and greatest Toil, but it may have bugs or breaking changes introduced since the last release.

6.  Click OK (marked "2" in the screenshot).
7.  Choose a subscription and select or create a Resource Group (marked "3" in the screenshot). If creating a Resource Group, select a region in which to place it. It is recommended to create a new Resource Group for every cluster; the template creates a large number of Azure entitites besides just the VMs (like virtual networks), and if they are organized into their own Resource Group they can all be cleaned up at once when you are done with the cluster, by deleting the Resource Group.
8.  Read the Azure terms of service (by clicking on the item marked "4" in the screenshot) and accept them by clicking the "Create" button on the right (not shown). This is the contract that you are accepting with Microsoft, under which you are purchasing the cluster.
9.  Click the main "Create" button (marked "5" in the screenshot). This will kick off the process of creating the cluster.
10. Eventually you will receive a notification (Bell icon on the top bar of the Azure UI) letting you know that your cluster has been created. At this point, you should be able to connect to it; however, note that it will not be ready to run any Toil jobs until it is finished setting itself up.
11. SSH into the first (and by default only) leader node. For this, you need to know the ``AdminUsername`` and ``DnsNameForMastersPublicIp`` you set above, and the name of the region you placed your cluster in. If you named your user ``phoebe`` and named your cluster ``toilisgreat``, and placed it in the ``centralus`` region, the hostname of the cluster would be ``toilisgreat.centralus.cloudapp.azure.com``, and you would want to connect as ``phoebe``. SSH is forwarded through the cluster's load balancer to the first leader node on port 2211, so you would run ``ssh phoebe@toilisgreat.centralus.cloudapp.azure.com -p 2211``.
12. Wait for the leader node to finish setting itself up. Run ``tail -f /var/log/azure/cluster-bootstrap.log`` and wait until the log reaches the line ``completed mesos cluster configuration``. At that point, kill ``tail`` with a ``ctrl-c``. Your leader node is now ready.
13. At this point, you can start running Toil jobs, using the Mesos batch system (by passing ``--batchSystem mesos --mesosMaster 10.0.0.5:5050``) and the Azure job store (for which you will need a separate Azure Storage account set up, ideally in the same region as your cluster but in a different Resource Group). The nodes of the cluster may take a few more minutes to finish installing, but when they do they will report in to Mesos and begin running any scheduled jobs.
14. Whan you are done running your jobs, go back to the Azure portal, find the Resource Group you created for your cluster, and delete it. This will destroy all the VMs and any data stored on them, and stop Microsoft charging you money for keeping the cluster around. As long as you used a separate Asure Storage account in a different Resource Group, any information kept in the job stores and file stores you were using will be retained.

For more information about how your new cluster is organized, for information on how to access the Mesos Web UI, or for troubleshooting advice, please see `the template documentation <https://github.com/BD2KGenomics/toil/blob/master/contrib/azure/README.md>`_.

.. _installationOpenStack:

Installation on OpenStack
-------------------------

Our group is working to expand distributed cluster support to OpenStack by providing
convenient Docker containers to launch Mesos from. Currently, OpenStack nodes can be setup
to run Toil in **singleMachine** mode following the basic installation instructions: :ref:`installation-ref`

.. _installationGoogleComputeEngine:

Installation on Google Compute Engine
-------------------------------------

Support for running on Google Cloud is experimental, and our group is working to expand
distributed cluster support to Google Compute by writing a cluster provisioning tool based around
a Dockerized Mesos setup. Currently, Google Compute Engine nodes can be configured to
run Toil in **singleMachine** mode following the basic installation instructions: :ref:`installation-ref`
