.. highlight:: console

.. _runningTests:

Running Tests
-------------

Test make targets, invoked as ``$ make <target>``, subject to which
environment variables are set (see :ref:`test_env_vars`).

+-------------------------+---------------------------------------------------+
|     TARGET              |        DESCRIPTION                                |
+-------------------------+---------------------------------------------------+
| test                    | Invokes all tests.                                |
+-------------------------+---------------------------------------------------+
| integration_test        | Invokes only the integration tests.               |
+-------------------------+---------------------------------------------------+
| test_offline            | Skips building the Docker appliance and only      |
|                         | invokes tests that have no docker dependencies.   |
+-------------------------+---------------------------------------------------+
| integration_test_local  | Makes integration tests easier to debug locally   |
|                         | by running the integration tests serially and     |
|                         | doesn't redirect output. This makes it appears on |
|                         | the terminal as expected.                         |
+-------------------------+---------------------------------------------------+

Before running tests for the first time, initialize your virtual environment
following the steps in :ref:`buildFromSource`.

Run all tests (including slow tests)::

    $ make test


Run only quick tests (as of Jul 25, 2018, this was ~ 20 minutes)::

    $ export TOIL_TEST_QUICK=True; make test

Run an individual test with::

    $ make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The default value for ``tests`` is ``"src"`` which includes all tests in the
``src/`` subdirectory of the project root. Tests that require a particular
feature will be skipped implicitly. If you want to explicitly skip tests that
depend on a currently installed *feature*, use

::

    $ make test tests="-m 'not aws' src"

This will run only the tests that don't depend on the ``aws`` extra, even if
that extra is currently installed. Note the distinction between the terms
*feature* and *extra*. Every extra is a feature but there are features that are
not extras, such as the ``gridengine`` and ``parasol`` features.  To skip tests
involving both the ``parasol`` feature and the ``aws`` extra, use the following::

    $ make test tests="-m 'not aws and not parasol' src"



Running Tests with pytest
~~~~~~~~~~~~~~~~~~~~~~~~~

Often it is simpler to use pytest directly, instead of calling the ``make`` wrapper.
This usually works as expected, but some tests need some manual preparation. To run a specific test with pytest,
use the following::

    python -m pytest src/toil/test/sort/sortTest.py::SortTest::testSort

For more information, see the `pytest documentation`_.

.. _pytest documentation: https://docs.pytest.org/en/latest/

.. _test_env_vars:

Running Integration Tests
~~~~~~~~~~~~~~~~~~~~~~~~~

These tests are generally only run using in our CI workflow due to their resource requirements and cost. However, they
can be made available for local testing:

 - Running tests that make use of Docker (e.g. autoscaling tests and Docker tests) require an appliance image to be
   hosted. First, make sure you have gone through the set up found in :ref:`quaySetup`.
   Then to build and host the appliance image run the ``make`` target ``push_docker``. ::

        $ make push_docker

 - Running integration tests require activation via an environment variable as well as exporting information relevant to
   the desired tests. Enable the integration tests::

        $ export TOIL_TEST_INTEGRATIVE=True

 - Finally, set the environment variables for keyname and desired zone::

        $ export TOIL_X_KEYNAME=[Your Keyname]
        $ export TOIL_X_ZONE=[Desired Zone]

   Where ``X`` is one of our currently supported cloud providers (``GCE``, ``AWS``).

 - See the above sections for guidance on running tests.

Test Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~

+------------------------+----------------------------------------------------+
| TOIL_TEST_TEMP         | An absolute path to a directory where Toil tests   |
|                        | will write their temporary files. Defaults to the  |
|                        | system's `standard temporary directory`_.          |
+------------------------+----------------------------------------------------+
| TOIL_TEST_INTEGRATIVE  | If ``True``, this allows the integration tests to  |
|                        | run. Only valid when running the tests from the    |
|                        | source directory via ``make test`` or              |
|                        | ``make test_parallel``.                            |
+------------------------+----------------------------------------------------+
| TOIL_AWS_KEYNAME       | An AWS keyname (see :ref:`prepareAWS`), which      |
|                        | is required to run the AWS tests.                  |
+------------------------+----------------------------------------------------+
| TOIL_GOOGLE_PROJECTID  | A Google Cloud account projectID                   |
|                        | (see :ref:`runningGCE`), which is required to      |
|                        | to run the Google Cloud tests.                     |
+------------------------+----------------------------------------------------+
| TOIL_TEST_QUICK        | If ``True``, long running tests are skipped.       |
+------------------------+----------------------------------------------------+

.. _standard temporary directory: https://docs.python.org/2/library/tempfile.html#tempfile.gettempdir

.. admonition:: Partial install and failing tests

    Some tests may fail with an ImportError if the required extras are not installed.
    Install Toil with all of the extras
    do prevent such errors.

.. _quaySetup:

Using Docker with Quay
~~~~~~~~~~~~~~~~~~~~~~

`Docker`_ is needed for some of the tests. Follow the appropriate
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

Running Mesos Tests
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

Developing with Docker
----------------------

To develop on features reliant on the Toil Appliance (the docker image toil uses for AWS autoscaling), you
should consider setting up a personal registry on `Quay`_ or `Docker Hub`_. Because
the Toil Appliance images are tagged with the Git commit they are based on and
because only commits on our master branch trigger an appliance build on Quay,
as soon as a developer makes a commit or dirties the working copy they will no
longer be able to rely on Toil to automatically detect the proper Toil Appliance
image. Instead, developers wishing to test any appliance changes in autoscaling
should build and push their own appliance image to a personal Docker registry.
This is described in the next section.

Making Your Own Toil Docker Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Note!**  Toil checks if the docker image specified by TOIL_APPLIANCE_SELF
exists prior to launching by using the docker v2 schema.  This should be
valid for any major docker repository, but there is an option to override
this if desired using the option: `-\\-forceDockerAppliance`.

Here is a general workflow (similar instructions apply when using Docker Hub):

#. Make some changes to the provisioner of your local version of Toil

#. Go to the location where you installed the Toil source code and run ::

        $ make docker

   to automatically build a docker image that can now be uploaded to
   your personal `Quay`_ account. If you have not installed Toil source
   code yet see :ref:`buildFromSource`.

#. If it's not already you will need Docker installed and need
   to `log into Quay`_. Also you will want to make sure that your Quay
   account is public.

#. Set the environment variable ``TOIL_DOCKER_REGISTRY`` to your Quay
   account. If you find yourself doing this often you may want to add ::

        export TOIL_DOCKER_REGISTRY=quay.io/<MY_QUAY_USERNAME>

   to your ``.bashrc`` or equivalent.

#. Now you can run ::

        $ make push_docker

   which will upload the docker image to your Quay account. Take note of
   the image's tag for the next step.

#. Finally you will need to tell Toil from where to pull the Appliance
   image you've created (it uses the Toil release you have installed by
   default). To do this set the environment variable
   ``TOIL_APPLIANCE_SELF`` to the url of your image. For more info see
   :ref:`envars`.

#. Now you can launch your cluster! For more information see
   :ref:`Autoscaling`.

Running a Cluster Locally
~~~~~~~~~~~~~~~~~~~~~~~~~

The Toil Appliance container can also be useful as a test environment since it
can simulate a Toil cluster locally. An important caveat for this is autoscaling,
since autoscaling will only work on an EC2 instance and cannot (at this time) be
run on a local machine.

To spin up a local cluster, start by using the following Docker run command to launch
a Toil leader container::

    docker run \
        --entrypoint=mesos-master \
        --net=host \
        -d \
        --name=leader \
        --volume=/home/jobStoreParentDir:/jobStoreParentDir \
        quay.io/ucsc_cgl/toil:3.6.0 \
        --registry=in_memory \
        --ip=127.0.0.1 \
        --port=5050 \
        --allocation_interval=500ms

A couple notes on this command: the ``-d`` flag tells Docker to run in daemon mode so
the container will run in the background. To verify that the container is running you
can run ``docker ps`` to see all containers. If you want to run your own container
rather than the official UCSC container you can simply replace the
``quay.io/ucsc_cgl/toil:3.6.0`` parameter with your own container name.

Also note that we are not mounting the job store directory itself, but rather the location
where the job store will be written. Due to complications with running Docker on MacOS, I
recommend only mounting directories within your home directory. The next command will
launch the Toil worker container with similar parameters::

    docker run \
        --entrypoint=mesos-slave \
        --net=host \
        -d \
        --name=worker \
        --volume=/home/jobStoreParentDir:/jobStoreParentDir \
        quay.io/ucsc_cgl/toil:3.6.0 \
        --work_dir=/var/lib/mesos \
        --master=127.0.0.1:5050 \
        --ip=127.0.0.1 \
        —-attributes=preemptable:False \
        --resources=cpus:2

Note here that we are specifying 2 CPUs and a non-preemptable worker. We can
easily change either or both of these in a logical way. To change the number
of cores we can change the 2 to whatever number you like, and to
change the worker to be preemptable we change ``preemptable:False`` to
``preemptable:True``. Also note that the same volume is mounted into the
worker. This is needed since both the leader and worker write and read
from the job store. Now that your cluster is running, you can run ::

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

Maintainer's Guidelines
-----------------------

In general, as developers and maintainers of the code, we adhere to the following guidelines:

* We strive to never break the build on master. All development should be done
  on branches, in either the main Toil repository or in developers' forks.

* Pull requests should be used for any and all changes (except truly trivial
  ones).

* Pull requests should be in response to issues. If you find yourself making a
  pull request without an issue, you should create the issue first.


Naming Conventions
~~~~~~~~~~~~~~~~~~

* **Commit messages** *should* be `great`_. Most importantly, they *must*:

  - Have a short subject line. If in need of more space, drop down **two** lines
    and write a body to explain what is changing and why it has to change.

  - Write the subject line as a command: `Destroy all humans`,
    not `All humans destroyed`.

  - Reference the issue being fixed in a Github-parseable format, such as
    `(resolves #1234)` at the end of the subject line, or `This will fix #1234.`
    somewhere in the body. If no single commit on its own fixes the issue, the
    cross-reference must appear in the pull request title or body instead.

* **Branches** in the main Toil repository *must* start with ``issues/``,
  followed by the issue number (or numbers, separated by a dash), followed by a
  short, lowercase, hyphenated description of the change. (There can be many open
  pull requests with their associated branches at any given point in time and
  this convention ensures that we can easily identify branches.)

  Say there is an issue numbered #123 titled `Foo does not work`. The branch name
  would be ``issues/123-fix-foo`` and the title of the commit would be
  `Fix foo in case of bar (resolves #123).`

.. _great: https://chris.beams.io/posts/git-commit/#seven-rules

Pull Requests
~~~~~~~~~~~~~
* All pull requests must be reviewed by a person other than the request's
  author.

* Modified pull requests must be re-reviewed before merging. **Note that Github
  does not enforce this!**

* When merging a pull request, make sure to update the `Draft Changelog`_ on
  the Github wiki, which we will use to produce the changelog for the next
  release. The PR template tells you to do this, so don't forget. New entries
  should go at the bottom.

  .. _Draft Changelog: https://github.com/DataBiosphere/toil/wiki/Draft-Changelog

* Pull requests will not be merged unless Travis and Gitlab CI tests pass.
  Gitlab tests are only run on code in the main Toil repository on some branch,
  so it is the responsibility of the approving reviewer to make sure that pull
  requests from outside repositories are copied to branches in the main
  repository. This can be accomplished with (from a Toil clone):

  .. code-block:: bash

      ./contrib/admin/test-pr theirusername their-branch issues/123-fix-description-here

  This must be repeated every time the PR submitter updates their PR, after
  checking to see that the update is not malicious.

  If there is no issue corresponding to the PR, after which the branch can be
  named, the reviewer of the PR should first create the issue.

  Developers who have push access to the main Toil repository are encouraged to
  make their pull requests from within the repository, to avoid this step.

* Prefer using "Squash and marge" when merging pull requests to master especially
  when the PR contains a "single unit" of work (i.e. if one were to rewrite the
  PR from scratch with all the fixes included, they would have one commit for
  the entire PR). This makes the commit history on master more readable
  and easier to debug in case of a breakage.

  When squashing a PR from multiple authors, please add
  `Co-authored-by`_ to give credit to all contributing authors.

  See `Issue #2816`_ for more details.

  .. _Co-authored-by: https://github.blog/2018-01-29-commit-together-with-co-authors/
  .. _Issue #2816: https://github.com/DataBiosphere/toil/issues/2816
  .. _toil.lib.retry: https://github.com/DataBiosphere/toil/blob/master/src/toil/lib/retry.py
  

Publishing a Release
~~~~~~~~~~~~~~~~~~~~

These are the steps to take to publish a Toil release:

* Determine the release version **X.Y.Z**. This should follow
  `semantic versioning`_; if user-workflow-breaking changes are made, **X**
  should be incremented, and **Y** and **Z** should be zero. If non-breaking
  changes are made but new functionality is added, **X** should remain the same
  as the last release, **Y** should be incremented, and **Z** should be zero.
  If only patches are released, **X** and **Y** should be the same as the last
  release and **Z** should be incremented.

  .. _semantic versioning: https://semver.org/

* If it does not exist already, create a release branch in the Toil repo
  named ``X.Y.x``, where **x** is a literal lower-case "x". For patch releases,
  find the existing branch and make sure it is up to date with the patch
  commits that are to be released. They may be `cherry-picked over`_ from
  master.

  .. _cherry-picked over: https://trunkbaseddevelopment.com/branch-for-release/

* On the release branch, edit ``version_template.py`` in the root of the
  repository. Find the line that looks like this (slightly different for patch
  releases):

  .. code-block:: python

      baseVersion = 'X.Y.0a1'

  Make it look like this instead:

  .. code-block:: python

      baseVersion = 'X.Y.Z'

  Commit your change to the branch.

* Tag the current state of the release branch as ``releases/X.Y.Z``.

* Make the Github release here_, referencing that tag. For a non-patch
  release, fill in the description with the changelog from `the wiki page`_,
  which you should clear. For a patch release, just describe the patch.

  .. _here: https://github.com/DataBiosphere/toil/releases/new
  .. _the wiki page: https://github.com/DataBiosphere/toil/wiki/Draft-Changelog

* For a non-patch release, set up the main branch so that development
  builds will declare themselves to be alpha versions of what the next release
  will probably be. Edit  ``version_template.py`` in the root of the repository
  on the main branch to set ``baseVersion`` like this:

  .. code-block:: python

      baseVersion = 'X.Y+1.0a1'

  Make sure to replace ``X`` and ``Y+1`` with actual numbers.

Adding Retries to a Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `toil.lib.retry`_ .

retry() can be used to decorate any function based on the list of errors one wishes to retry on.

This list of errors can contain normal Exception objects, and/or RetryCondition objects wrapping Exceptions to
include additional conditions.

For example, retrying on a one Exception (HTTPError)::

    from requests import get
    from requests.exceptions import HTTPError

    @retry(errors=[HTTPError])
    def update_my_wallpaper():
        return get('https://www.deviantart.com/')

Or::

    from requests import get
    from requests.exceptions import HTTPError

    @retry(errors=[HTTPError, ValueError])
    def update_my_wallpaper():
        return get('https://www.deviantart.com/')

The examples above will retry for the default interval on any errors specified the "errors=" arg list.

To retry on specifically 500/502/503/504 errors, you could specify an ErrorCondition object instead, for example::

    from requests import get
    from requests.exceptions import HTTPError

    @retry(errors=[
        ErrorCondition(
                   error=HTTPError,
                   error_codes=[500, 502, 503, 504]
               )])
    def update_my_wallpaper():
        return requests.get('https://www.deviantart.com/')

To retry on specifically errors containing the phrase "NotFound"::

    from requests import get
    from requests.exceptions import HTTPError

    @retry(errors=[
        ErrorCondition(
            error=HTTPError,
            error_message_must_include="NotFound"
        )])
    def update_my_wallpaper():
        return requests.get('https://www.deviantart.com/')

To retry on all HTTPError errors EXCEPT an HTTPError containing the phrase "NotFound"::

    from requests import get
    from requests.exceptions import HTTPError

    @retry(errors=[
        HTTPError,
        ErrorCondition(
                   error=HTTPError,
                   error_message_must_include="NotFound",
                   retry_on_this_condition=False
               )])
    def update_my_wallpaper():
        return requests.get('https://www.deviantart.com/')

To retry on boto3's specific status errors, an example of the implementation is::

    import boto3
    from botocore.exceptions import ClientError

    @retry(errors=[
        ErrorCondition(
                   error=ClientError,
                   boto_error_codes=["BucketNotFound"]
               )])
    def boto_bucket(bucket_name):
        boto_session = boto3.session.Session()
        s3_resource = boto_session.resource('s3')
        return s3_resource.Bucket(bucket_name)

Any combination of these will also work, provided the codes are matched to the correct exceptions.  A ValueError will
not return a 404, for example.

The retry function as a decorator should make retrying functions easier and clearer.  It also encourages
smaller independent functions, as opposed to lumping many different things that may need to be retried on
different conditions in the same function.

The ErrorCondition object tries to take some of the heavy lifting of writing specific retry conditions
and boil it down to an API that covers all common use-cases without the user having to write
any new bespoke functions.

Use-cases covered currently:

1. Retrying on a normal error, like a KeyError.
2. Retrying on HTTP error codes (use ErrorCondition).
3. Retrying on boto's specific status errors, like "BucketNotFound" (use ErrorCondition).
4. Retrying when an error message contains a certain phrase (use ErrorCondition).
5. Explicitly NOT retrying on a condition (use ErrorCondition).

If new functionality is needed, it's currently best practice in Toil to add
functionality to the ErrorCondition itself rather than making a new custom retry method.
