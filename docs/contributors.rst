
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

.. code-block:: console

    $ make

from the project root will print a description of the available Makefile
targets.

Once you created and activated the virtualenv, the first step is to install the
build requirements. These are additional packages that Toil needs to be tested
and built, but not run:

.. code-block:: console

    $ make prepare

Once the virtualenv has been prepared with the build requirements, running

.. code-block:: console

    $ make develop

will create an editable installation of Toil and its runtime requirements in
the current virtualenv. The installation is called *editable* (also known as a
`development mode`_ installation) because changes to the Toil source code
immediately affect the virtualenv. Optionally, set the ``extras`` variable to
ensure that ``make develop`` installs support for optional extras. Consult
``setup.py`` for the list of supported extras. To install Toil in development
mode with all extras run

.. code-block:: console

    $ make develop extras=[aws,mesos,azure,google,encryption,cwl]

.. _development mode: https://pythonhosted.org/setuptools/setuptools.html#development-mode

Note that some extras have native dependencies as listed in
:ref:`installation-ref`. Be sure to install them before running the above
command. If you get

.. code-block:: python

   ImportError: No module named mesos.native

make sure you install Mesos and the Mesos egg as described in :ref:`Apache
Mesos <mesos>` and be sure to create the virtualenv with
``--system-site-packages``.

To build the docs, run ``make develop`` with all extras followed by

.. code-block:: console

    $ make docs

To invoke all tests (unit and integration) use

.. code-block:: console

    $ make test

Note that :ref:`Docker and Quay <docker-quay-note>` are necessary for some tests.

Run an individual test with

.. code-block:: console

    $ make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The default value for ``tests`` is ``"src"`` which includes all tests in the
``src`` subdirectory of the project root. Tests that require a particular
feature will be skipped implicitly. If you want to explicitly skip tests that
depend on a currently installed *feature*, use

.. code-block:: console

    $ make test tests="-m 'not azure' src"

This will run only the tests that don't depend on the ``azure`` extra, even if
that extra is currently installed. Note the distinction between the terms
*feature* and *extra*. Every extra is a feature but there are features that are
not extras, the ``gridengine`` and ``parasol`` features fall into that
category. So in order to skip tests involving both the Parasol feature and the
Azure extra, the following can be used::

.. code-block:: console

    $ make test tests="-m 'not azure and not parasol' src"

Running Mesos tests
~~~~~~~~~~~~~~~~~~~

If you're running Toil's Mesos tests, be sure to create the virtualenv with
``--system-site-packages`` to include the Mesos Python bindings. Verify this by
activating the virtualenv and running ``pip list | grep mesos``. On macOS,
this may come up empty. To fix it, run the following:

.. code-block:: bash

    for i in /usr/local/lib/python2.7/site-packages/*mesos*; do ln -snf $i venv/lib/python2.7/site-packages/ ; done

.. _docker-quay-note:
.. topic:: Installing Docker with Quay

   `Docker`_ is needed for some of the tests. Follow the appopriate
   installation instructions for your system on their website to get started.

   When running ``make test`` you may still get the following error

   ::

       Please set TOIL_DOCKER_REGISTRY, e.g. to quay.io/USER.

   To solve, make an account with `Quay`_ and specify it like so:

   ::

       TOIL_DOCKER_REGISTRY=quay.io/USER make test

   where ``USER`` is your Quay username.

   For convenience you may want to add this variable to your bashrc by running

   .. code-block:: console

        $ echo 'export TOIL_DOCKER_REGISTRY=quay.io/USER' >> $HOME/.bashrc


.. _Docker: https://www.docker.com/products/docker
.. _Quay: https://quay.io/
