Deploying a workflow
====================

If a Toil workflow is run on a single machine (that is, single machine mode),
there is nothing special you need to do. You change into the directory
containing your user script and invoke it like any Python script:

.. code-block:: console

   $ cd my_project
   $ ls
   userScript.py …
   $ ./userScript.py …

This assumes that your script has the executable permission bit set and
contains a *shebang*, i.e. a line of the form

::

   #!/usr/bin/env python

Alternatively, the shebang can be omitted and the script invoked as a module
via

::

   $ python -m userScript

in which case the executable permission is not required either. Both are common
methods for invoking Python scripts.

The script can have dependencies, as long as those are installed on the machine,
either globally, in a user-specific location or in a virtualenv. In the latter
case, the virtualenv must of course be active when you run the user script.

If, however, you want to run your workflow in a distributed environment, on
multiple worker machines, either in the cloud or on a bare-metal cluster, your
script needs to be made available to those other machines. If your script
imports other modules, those modules also need to be made available on the
workers. Toil can automatically do that for you, with a little help on your
part. We call this feature *hot-deployment* of a workflow.

Let's first examine various scenarios of hot-deploying a workflow and then take
a look at :ref:`deploying Toil <deploying_toil>`, which, as we'll see shortly
cannot be hot-deployed. Lastly we'll deal with the issue of declaring
:ref:`Toil as a dependency <depending_on_toil>` of a workflow that is packaged
as a setuptools distribution.


Hot-deployment without dependencies
-----------------------------------

If your script has no additional dependencies, i.e. imports only modules that
are shipped with Python or Toil, only your script needs to be hot-deployed.
Both Python and Toil are assumed to be present on all workers. Toil takes your
script, stores it in the job store and just before the jobs in your script are
about to be run on a worker machine, your script will be saved to a temporary
directory on the worker and loaded into the Python interpreter from there.

In this scenario, the script is invoked as follows:

.. code-block:: console

   $ cd my_project
   $ ls
   userScript.py
   $ ./userScript.py --batchSystem=mesos …


This is very similar to the single-machine scenario but note that we selected a
distributed batch system, ``mesos`` in this case. And just like in single-machine
mode, we can also use ``-m`` to invoke the workflow::

   $ python -m userScript --batchSystem=mesos …


Hot-deployment with sibling modules
-----------------------------------

This scenario applies if the user script imports modules that are its siblings::

   $ cd my_project
   $ ls
   userScript.py utilities.py
   $ ./userScript.py --batchSystem=mesos …

Here ``userScript.py`` imports additional functionality from ``utilities.py``.
Toil detects that ``userScript.py`` has sibling modules and copies them to the
workers, alongside the user script. Note that sibling modules will be
hot-deployed regardless of whether they are actually imported by the user
script–all .py files residing in the same directory as the user script will
automatically be hot-deployed.

Sibling modules are a suitable method of organizing the source code of
reasonably complicated workflows.


Hot-deploying a package hierarchy
---------------------------------

Recall that in Python, a `package`_ is a directory containing one or more
``.py`` files—one of which must be called ``__init__.py``—and optionally other
packages. For more involved workflows that contain a significant amount of
code, this is the recommended way of organizing the source code. Because we use
a package hierarchy, we can't really refer to the user script as such, we call
it the user *module* instead. It is merely one of the modules in the package
hierarchy. We need to inform Toil that we want to use a package hierarchy by
invoking Python's ``-m`` option. That enables Toil to identify the entire set
of modules belonging to the workflow and copy all of them to each worker. Note
that while using the ``-m`` option is optional in the scenarios above, it is
mandatory in this one.

The following shell session illustrates this::

   $ cd my_project
   $ tree
   .
   ├── utils
   │   ├── __init__.py
   │   └── sort
   │       ├── __init__.py
   │       └── quick.py
   └── workflow
       ├── __init__.py
       └── main.py

   3 directories, 5 files
   $ python -m workflow.main --batchSystem=mesos …

.. _package: https://docs.python.org/2/tutorial/modules.html#packages

Here the user module ``main.py`` does not reside in the current directory, but
is part of a package called ``util``, in a subdirectory of the current
directory. Additional functionality is in a separate module called
``util.sort.quick`` which corresponds to ``util/sort/quick.py``. Because we
invoke the user module via ``python -m workflow.main``, Toil can determine the
root directory of the hierarchy–``my_project`` in this case–and copy all Python
modules underneath it to each worker. The ``-m`` option is documented `here`_

.. _here: https://docs.python.org/2/using/cmdline.html#cmdoption-m

When ``-m`` is passed, Python adds the current working directory to
``sys.path``, the list of root directories to be considered when resolving a
module name like ``workflow.main``. Without that added convenience we'd have to
run the workflow as ``PYTHONPATH="$PWD" python -m workflow.main``. This also
means that Toil can detect the root directory of the user module's package
hierarchy even if it isn't the current working directory. In other words we
could do this::

   $ cd my_project
   $ export PYTHONPATH="$PWD"
   $ cd /some/other/dir
   $ python -m workflow.main --batchSystem=mesos …

Also note that the root directory itself must not be package, i.e. must not
contain an ``__init__.py``.

Hot-deploying a virtualenv
--------------------------

So far we've looked at running an isolated user script, a user script in
conjunction with sibling modules and a user module that is part of an entire
package tree. But what if our workflow requires external dependencies that can
be downloaded from PyPI and installed via pip or easy_install? Toil supports
this common scenario, too. The solution is to install the user module and its
dependencies into a virtualenv::

   $ cd my_project
   $ tree
   .
   ├── util
   │   ├── __init__.py
   │   └── sort
   │       ├── __init__.py
   │       └── quick.py
   └── workflow
       ├── __init__.py
       └── main.py

   3 directories, 5 files
   $ virtualenv --system-site-packages .env
   $ . .env/bin/activate
   $ pip install fairydust
   $ cp -R workflow util .env/lib/python2.7/site-packages
   $ python -m workflow.main --batchSystem=mesos …

Here we created a virtualenv in the ``.env`` subdirectory of our project, we
installed the ``fairydust`` distribution from PyPI and finally we installed the
two packages that our project consists of.

The main caveat to this solution is that the workflow's external dependencies
may not contain native code, i.e. they must be pure Python. If you have
dependencies that rely on native code, you must manually install them on each
worker.

The ``--system-site-packages`` option to ``virtualenv`` makes globally
installed packages visible inside the virtualenv. It is essential because, as
we'll see later, Toil and its dependencies must be installed globally and would
be inaccessible without that option.

If you create a ``setup.py`` for your project (see `setuptools`_), the ``cp``
step can be replaced with ``pip install .``. Your ``setup.py`` should declare
the ``fairydust`` dependency, also making redundant the manual installation of
that package in the steps above. Note that it is not possible to use ``python
setup.py develop`` or ``pip install -e .`` instead of ``pip install .`` because
the former two do not copy the source files but create an ``.egg-link`` file
instead, which Toil is not able to hot-deploy. Similarly, ``python setup.py
install`` does not work either because it installs the project as a Python Egg
(a ``.egg`` file), which is not supported by Toil although that may `change`_
in the future. You might be tempted to prevent the installation of the ``.egg``
by passing ``--single-version-externally-managed`` to ``setup.py install`` but
that would also disable the automatic installation of your project's
dependencies.

.. _setuptools: http://setuptools.readthedocs.io/en/latest/index.html
.. _change: https://github.com/BD2KGenomics/toil/issues/1367

If you publish your project to PyPI, others will be able to install it on their
leader using pip, provided they 1) already installed Toil on the leader and
workers nodes and 2) use a virtualenv created with ``--system-site-packages``::

   $ virtualenv --system-site-packages my-project
   $ . my-project/bin/activate
   $ pip install my-project
   $ python -m workflow.main --batchSystem=mesos …

Relying on shared filesystems
-----------------------------

Bare-metal clusters typically mount a shared file system like NFS on each node.
If every node has that file system mounted at the same path, you can place your
project on that shared filesystem and run your user script from there.
Additionally, you can clone the Toil source tree into a directory on that
shared file system and you won't even need to install Toil on every worker. Be
sure to add both your project directory and the Toil clone to ``PYTHONPATH``. Toil
replicates ``PYTHONPATH`` from the leader to every worker.

.. _deploying_toil:

Deploying Toil
--------------

We've looked at various ways of installing your workflow on the leader such
that Toil can replicate it to the workers and load the job definitions there.
But what about Toil itself? Unless you are running your workflow in single
machine mode (the default) or on a cluster where every node mounts a shared
file system at the same path, Toil somehow needs to be made available on each
worker. Unfortunately, hot-deployment only works for the user script/module and
its dependencies, not for Toil itself. Generally speaking, you or your admin
will need to manually :ref:`install <installation>` Toil on every cluster node
you intend to run Toil jobs on.

The Toil team is eagerly working to ameliorate this. Toil 3.5.0 will contain
the Toil Appliance, a Docker image that contains Mesos and Toil. You can use
this image to run the Toil Appliance locally without the need to install
anything. Only Docker is required. Inside the appliance you can then run a
workflow in single machine mode. From the appliance, you will also be able to
provision clusters of VMs in the cloud. Initially this will support Amazon EC2
only, but Google Cloud and Microsoft Azure will soon follow.

For the current stable release (3.3.x), you can use `CGCloud`_ to provision a
cluster of Amazon EC2 instances with Toil and Mesos on them. The ``contrib``
directory of the Toil contains Adam Novak's Azure resource template with which
you can deploy a Toil cluster in Azure. With CGCloud you would typically
provision a static cluster of either spot or on-demand instances, or a mix.
This is explained in more detail in section :ref:`installation`.

.. _depending_on_toil:

Depending on Toil
-----------------

If you are packing your workflow(s) as a pip-installable distribution on PyPI,
you might be tempted to declare Toil as a dependency in your ``setup.py``, via
the ``install_requires`` keyword argument to ``setup()``. Unfortunately, this
does not work, for two reasons: For one, Toil uses Setuptools' *extra*
mechanism to manage its own optional dependencies. If you explicitly declared a
dependency on Toil, you would have to hard-code a particular combination of
extras (or no extras at all), robbing the user of the choice what Toil extras
to install. Secondly, and more importantly, declaring a dependency on Toil
would only lead to Toil being installed on the leader node of a cluster, but
not the worker nodes. Hot-deployment does not work here because Toil cannot
hot-deploy itself, the classic "Which came first, chicken or egg?" problem.

In other words, you shouldn't explicitly depend on Toil. Document the
dependency instead (as in "This workflow needs Toil version X.Y.Z to be
installed") and optionally add a version check to your ``setup.py``. Refer to
the ``check_version()`` function in the ``toil-lib`` project's `setup.py`_ for
an example. Alternatively, you can also just depend on ``toil-lib`` and you'll
get that check for free.

.. _setup.py: https://github.com/BD2KGenomics/toil-lib/blob/master/setup.py

If your workflow depends on a dependency of Toil, e.g. ``bd2k-python-lib``,
consider not making that dependency explicit either. If you do, you risk a
version conflict between your project and Toil. The ``pip`` utility may
silently ignore that conflict, breaking either Toil or your workflow. It is
safest to simply assume that Toil installs that dependency for you. The only
downside is that you are locked into the exact version of that dependency that
Toil declares. But such is life with Python, which, unlike Java, has no means
of dependencies belonging to different software components within the same
process, and whose favored software distribution utility is `incapable`_ of
properly resolving overlapping dependencies and detecting conflicts.

.. _incapable: https://github.com/pypa/pip/issues/988


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

.. _Quay: https://quay.io/

.. _Docker Hub: https://hub.docker.com/

