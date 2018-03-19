.. highlight:: console

.. _autoDeploying:

Auto-Deployment
===============

If you want to run your workflow in a distributed environment, on multiple worker machines, either in the cloud or on a
bare-metal cluster, your script needs to be made available to those other machines. If your script imports other
modules, those modules also need to be made available on the workers. Toil can automatically do that for you, with a
little help on your part. We call this feature *auto-deployment* of a workflow.

Let's first examine various scenarios of auto-deploying a workflow, which, as we'll see shortly cannot be
auto-deployed. Lastly, we'll deal with the issue of declaring :ref:`Toil as a dependency <depending_on_toil>` of a
workflow that is packaged as a setuptools distribution.

Toil can be easily deployed to a remote host. First, assuming you've followed our :ref:`prepareAWS` section to install Toil
and use it to create a remote leader node on (in this example) AWS, you can now log into this into using
:ref:`sshCluster` and once on the remote host, create and activate a virtualenv (noting to make sure to use the
``--system-site-packages`` option!)::

   $ virtualenv --system-site-packages venv
   $ . venv/bin/activate

Note the ``--system-site-packages`` option, which ensures that globally-installed packages are accessible inside the
virtualenv.  Do not (re)install Toil after this!  The ``--system-site-packages`` option has already transferred Toil and
the dependencies from your local installation of Toil for you.

From here, you can install a project and its dependencies::

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
   $ pip install matplotlib
   $ cp -R workflow util venv/lib/python2.7/site-packages

Ideally, your project would have a ``setup.py`` file (see `setuptools`_) which streamlines the installation process::

   $ tree
   .
   ├── util
   │   ├── __init__.py
   │   └── sort
   │       ├── __init__.py
   │       └── quick.py
   ├── workflow
   │   ├── __init__.py
   │   └── main.py
   └── setup.py

   3 directories, 6 files
   $ pip install .

Or, if your project has been published to PyPI::

   $ pip install my-project

In each case, we have created a virtualenv with the ``--system-site-packages`` flag in the ``venv`` subdirectory then
installed the ``matplotlib`` distribution from PyPI along with the two packages that our project consists of. (Again,
both Python and Toil are assumed to be present on the leader and all worker nodes.)

We can now run our workflow::

   $ python main.py --batchSystem=mesos …

.. important::

   If workflow's external dependencies contain native code (i.e. are not pure
   Python) then they must be manually installed on each worker.

.. warning::

   Neither ``python setup.py develop`` nor ``pip install -e .`` can be used in
   this process as, instead of copying the source files, they create ``.egg-link``
   files that Toil can't auto-deploy. Similarly, ``python setup.py install``
   doesn't work either as it installs the project as a Python ``.egg`` which is
   also not currently supported by Toil (though it `could be`_ in the future).

   Also note that using the
   ``--single-version-externally-managed`` flag with ``setup.py`` will
   prevent the installation of your package as an ``.egg``. It will also disable
   the automatic installation of your project's dependencies.

.. _setuptools: http://setuptools.readthedocs.io/en/latest/index.html
.. _could be: https://github.com/BD2KGenomics/toil/issues/1367

Auto Deployment with Sibling Modules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This scenario applies if the user script imports modules that are its siblings::

   $ cd my_project
   $ ls
   userScript.py utilities.py
   $ ./userScript.py --batchSystem=mesos …

Here ``userScript.py`` imports additional functionality from ``utilities.py``.
Toil detects that ``userScript.py`` has sibling modules and copies them to the
workers, alongside the user script. Note that sibling modules will be
auto-deployed regardless of whether they are actually imported by the user
script–all .py files residing in the same directory as the user script will
automatically be auto-deployed.

Sibling modules are a suitable method of organizing the source code of
reasonably complicated workflows.


Auto-Deploying a Package Hierarchy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

Relying on Shared Filesystems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bare-metal clusters typically mount a shared file system like NFS on each node.
If every node has that file system mounted at the same path, you can place your
project on that shared filesystem and run your user script from there.
Additionally, you can clone the Toil source tree into a directory on that
shared file system and you won't even need to install Toil on every worker. Be
sure to add both your project directory and the Toil clone to ``PYTHONPATH``. Toil
replicates ``PYTHONPATH`` from the leader to every worker.

.. admonition:: Using a shared filesystem

   Toil currently only supports a ``tempdir`` set to a local, non-shared directory.

.. _deploying_toil:

Toil Appliance
--------------

The term Toil Appliance refers to the Mesos Docker image that Toil uses to simulate the machines in the virtual mesos
cluster.  It's easily deployed, only needs Docker, and allows for workflows to be run in single-machine mode and for
clusters of VMs to be provisioned.  To specify a different image, see the Toil :ref:`envars` section.  For more
information on the Toil Appliance, see the :ref:`runningAWS` section.
