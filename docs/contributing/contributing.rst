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
.. _log into Quay: https://docs.quay.io/solution/getting-started.html


.. _appliance_dev:

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

Here is a general workflow: (similar instructions apply when using
Docker Hub)

1. Make some changes to the provisioner of your local version of Toil.

2. Go to the location where you installed the Toil source code and run::

        $ make docker

   to automatically build a docker image that can now be uploaded to
   your personal `Quay`_ account. If you have not installed Toil source
   code yet check out `Building from Source`_.

3. If it's not already you will need Docker installed and need
   to `log into Quay`_. Also you will want to make sure that your Quay
   account is public.

4. Set the environment variable ``TOIL_DOCKER_REGISTRY`` to your Quay
   account. If you find yourself doing this often you may want to add::

        export TOIL_DOCKER_REGISTRY=quay.io/<MY_QUAY_USERNAME>

   to your ``.bashrc`` or equivalent.

5. Now you can run::

        $ make push_docker

   which will upload the docker image to your Quay account. Take note of
   the image's tag for the next step.

6. Finally you will need to tell Toil from where to pull the Appliance
   image you've created (it uses the Toil release you have installed by
   default). To do this set the environment variable
   ``TOIL_APPLIANCE_SELF`` to the url of your image. For more info see
   :ref:`envars`.

7. Now you can launch your cluster! For more information see
   :ref:`Autoscaling`.

Running Cluster Locally
~~~~~~~~~~~~~~~~~~~~~~~

The Toil Appliance container can also be useful as a test environment since it
can simulate a Toil cluster locally. An important caveat for this is autoscaling,
since autoscaling will only work on an EC2 instance and cannot (at this time) be
run on a local machine.

To spin up a local cluster, start by using the following Docker run command to launch
a Toil leader container::

    docker run --entrypoint=mesos-master --net=host -d --name=leader --volume=/home/jobStoreParentDir:/jobStoreParentDir quay.io/ucsc_cgl/toil:3.6.0 --registry=in_memory --ip=127.0.0.1 --port=5050 --allocation_interval=500ms

A couple notes on this command: the ``-d`` flag tells Docker to run in daemon mode so
the container will run in the background. To verify that the container is running you
can run ``docker ps`` to see all containers. If you want to run your own container
rather than the official UCSC container you can simply replace the
``quay.io/ucsc_cgl/toil:3.6.0`` parameter with your own container name.

Also note that we are not mounting the job store directory itself, but rather the location
where the job store will be written. Due to complications with running Docker on MacOS, I
recommend only mounting directories within your home directory. The next command will
launch the Toil worker container with similar parameters::

    docker run --entrypoint=mesos-slave --net=host -d --name=worker --volume=/home/jobStoreParentDir:/jobStoreParentDir quay.io/ucsc_cgl/toil:3.6.0 --work_dir=/var/lib/mesos --master=127.0.0.1:5050 --ip=127.0.0.1 —-attributes=preemptable:False --resources=cpus:2

Note here that we are specifying 2 CPUs and a non-preemptable worker. We can
easily change either or both of these in a logical way. To change the number
of cores we can change the 2 to whatever number you like, and to
change the worker to be preemptable we change ``preemptable:False`` to
``preemptable:True``. Also note that the same volume is mounted into the
worker. This is needed since both the leader and worker write and read
from the job store. Now that your cluster is running, you can run::

    docker exec -it leader bash

to get a shell in your leader 'node'. You can also replace the ``leader`` parameter
with ``worker`` to get shell access in your worker.

.. admonition:: Docker-in-Docker issues

    If you want to run Docker inside this Docker cluster (Dockerized tools, perhaps),
    you should also mount in the Docker socket via ``-v /var/run/docker.sock:/var/run/docker.sock``.
    This will give the Docker client inside the Toil Appliance access to the Docker engine
    on the host. Client/engine version mismatches have been known to cause issues, so we
    recommend using Docker version 1.12.3 on the host to be compatible with the Docker
    client installed in the Appliance. Finally, be careful where you write files inside
    the Toil Appliance - 'child' Docker containers launched in the Appliance will actually
    be siblings to the Appliance since the Docker engine is located on the host. This
    means that the 'child' container can only mount in files from the Appliance if
    the files are located in a directory that was originally mounted into the Appliance
    from the host - that way the files are accessible to the sibling container. Note:
    if Docker can't find the file/directory on the host it will silently fail and mount
    in an empty directory.


.. _Quay: https://quay.io/

.. _Docker Hub: https://hub.docker.com/

Contributing
============

Maintainer's Guidelines
-----------------------

* We strive to never break the build on master.

* Pull requests should be used for any and all changes (except truly trivial
  ones).

* The commit message of direct commits to master must end in ``(resolves #``
  followed by the issue number followed by ``)``.

Naming conventions
------------------

* The **branch name** for a pull request starts with ``issues/`` followed by the
  issue number (or numbers, separated by a dash), followed by a short
  snake-case description of the change. (There can be many open pull requests
  with their associated branches at any given point in time and this convention
  ensures that we can easily identify branches.)

* The **commit message** of the first commit in a pull request needs to end in
  ``(resolves #`` followed by the issue number, followed by ``)``. See `here`_
  for details about writing properly-formatted and informative commit messages.

* The title of the **pull request** needs to have the same ``(resolves #...)``
  suffix as the commit message. This lets `Waffle`_ stack the pull request
  and the associated issue. (Fortunately, Github automatically prepopulates the
  title of the PR with the message of the first commit in the PR, so this isn't
  any additional work.)

Say there is an issue numbered #123 titled `Foo does not work`. The branch name
would be ``issues/123-fix-foo`` and the title of the commit would be `Fix foo in
case of bar (resolves #123).`

* Pull requests that address **multiple issues** use the
  ``(resolves #602, resolves #214)`` suffix in the request's title. These pull
  requests can and should contain multiple commits, with each commit message
  referencing the specific issue(s) it addresses. We may or may not squash the
  commits in those PRs.

.. _here: http://chris.beams.io/posts/git-commit/
.. _Waffle: https://waffle.io/BD2KGenomics/toil

Pull requests
-------------

* All pull requests must be reviewed by a person other than the request's
  author.

* Only the reviewer of a pull request can merge it.

* Until the pull request is merged, it should be continually rebased by the
  author on top of master.

* Pull requests are built automatically by Jenkins and won't be merged unless
  all tests pass.

* Ideally, a pull request should contain a single commit that addresses a
  single, specific issue. Rebasing and squashing can be used to achieve that
  goal (see :ref:`multi-author`).

.. _multi-author:

Multi-author pull requests
--------------------------

* A pull request starts off as single-author and can be changed to multi-author
  upon request via comment (typically by the reviewer) in the PR. The author of
  a single-author PR has to explicitly grant the request.

* Multi-author pull requests can have more than one commit. They must `not` be
  rebased as doing so would create havoc for other contributors.

* To keep a multi-author pull request up to date with master, merge from master
  instead of rebasing on top of master.

* Before the PR is merged, it may transition back to single-author mode, again
  via comment request in the PR. Every contributor to the PR has to acknowledge
  the request after making sure they don't have any unpushed changes they care
  about. This is necessary because a single-author PR can be reabsed and
  rebasing would make it hard to integrate these pushed commits.

