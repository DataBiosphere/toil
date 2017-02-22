.. highlight:: console

Building from Source
====================

For developers, tinkerers, and people otherwise interested in Toil's internals,
this section explains how to build Toil from source and run its test suite.

Building from master
--------------------

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

Now, we can install Toil in `development mode`_ (such that changes to the
source code will immediately affect the virtualenv)::

    $ make develop

Or, to install with support for all optional :ref:`extras`::

    $ make develop extras=[aws,mesos,azure,google,encryption,cwl]

To build the docs, run ``make develop`` with all extras followed by

::

    $ make docs

.. _development mode: https://pythonhosted.org/setuptools/setuptools.html#development-mode

Running tests
-------------

To invoke all tests (unit and integration) use

::

    $ make test

.. topic:: Installing Docker with Quay

   `Docker`_ is needed for some of the tests. Follow the appopriate
   installation instructions for your system on their website to get started.

   When running ``make test`` you might still get the following error::

      $ make test
      Please set TOIL_DOCKER_REGISTRY, e.g. to quay.io/USER.

   To solve, make an account with `Quay`_ and specify it like so::

      $ TOIL_DOCKER_REGISTRY=quay.io/USER make test

   where ``USER`` is your Quay username.

   For convenience you may want to add this variable to your bashrc by running

   ::

      $ echo 'export TOIL_DOCKER_REGISTRY=quay.io/USER' >> $HOME/.bashrc


Run an individual test with

::

    $ make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The default value for ``tests`` is ``"src"`` which includes all tests in the
``src/`` subdirectory of the project root. Tests that require a particular
feature will be skipped implicitly. If you want to explicitly skip tests that
depend on a currently installed *feature*, use

::

    $ make test tests="-m 'not azure' src"

This will run only the tests that don't depend on the ``azure`` extra, even if
that extra is currently installed. Note the distinction between the terms
*feature* and *extra*. Every extra is a feature but there are features that are
not extras, such as the ``gridengine`` and ``parasol`` features.  To skip tests
involving both the Parasol feature and the Azure extra, use the following::

    $ make test tests="-m 'not azure and not parasol' src"

Running Mesos tests
~~~~~~~~~~~~~~~~~~~

If you're running Toil's Mesos tests, be sure to create the virtualenv with
``--system-site-packages`` to include the Mesos Python bindings. Verify this by
activating the virtualenv and running ``pip list | grep mesos``. On macOS,
this may come up empty. To fix it, run the following:

.. code-block:: bash

    for i in /usr/local/lib/python2.7/site-packages/*mesos*; do ln -snf $i venv/lib/python2.7/site-packages/; done

.. _Docker: https://www.docker.com/products/docker
.. _Quay: https://quay.io/


Developing with the Toil Appliance
----------------------------------

To develop on features reliant on the Toil Appliance (i.e. autoscaling), you
should consider setting up a personal registry on `Quay`_ or `Docker Hub`_. Because
the Toil Appliance images are tagged with the Git commit they are based on and
because only commits on our master branch trigger an appliance build on Quay,
as soon as a developer makes a commit or dirties the working copy they will no
longer be able to rely on Toil to automatically detect the proper Toil Appliance
image. Instead, developers wishing to test any appliance changes in autoscaling
should build and push their own appliance image to a personal Docker registry.
See :ref:`Autoscaling` and :func:`toil.applianceSelf` for information on how to
configure Toil to pull the Toil Appliance image from your personal repo instead
of the our official Quay account.

The Toil Appliance container can also be useful as a test environment since it
can simulate a Toil cluster locally. An important caveat for this is autoscaling,
since autoscaling will only work on an EC2 instance and cannot (at this time) be
run on a local machine.

To spin up a local cluster, start by using the following Docker run command to launch
a Toil leader container::

    docker run --entrypoint=mesos-master --net=host -d --name=leader quay.io/ucsc_cgl/toil:3.6.0 --registry=in_memory --ip=127.0.0.1 --port=5050 --allocation_interval=500ms

A couple notes on this command: the ``-d`` flag tells Docker to run in daemon mode so
the container will run in the background. To verify that the container is running you
can run ``docker ps`` to see all containers. If you want to run your own container
rather than the official UCSC container you can simply replace the
``quay.io/ucsc_cgl/toil:3.6.0`` parameter with your own container name. Also
note that there are no data volumes being mounted into the container. If you
want your own files in the container, add the ``-v`` argument. For example::

    docker run -v ~/yourDirectory:/directory/inContainer/ ...

The next command will launch the Toil worker container with similar parameters::

    docker run --entrypoint=mesos-slave --net=host -d --name=worker quay.io/ucsc_cgl/toil:3.6.0 --work_dir=/var/lib/mesos --master=127.0.0.1:5050 --ip=127.0.0.1 —-attributes=preemptable:False --resources=cpus:2

Note here that we are specifying 2 CPUs and a non-preemptable worker. We can
easily change either or both of these in a logical way. To change the number
of cores we can change the 2 to whatever number you like, and to
change the worker to be preemptable we change ``preemptable:False`` to
``preemptable:True``. Now that your cluster is running, you can run::

    docker exec -it leader bash

to get a shell in your leader 'node'. You can also replace the ``leader`` parameter
with ``worker`` to get shell access in your worker.

.. _Quay: https://quay.io/

.. _Docker Hub: https://hub.docker.com/
