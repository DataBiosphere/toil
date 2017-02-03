.. highlight:: console

Deploying a workflow
====================

If a Toil workflow is run on a single machine (that is, single machine mode),
there is nothing special you need to do. You change into the directory
containing your user script and invoke it like any Python script::

   $ cd my_project
   $ ls
   userScript.py …
   $ ./userScript.py …

This assumes that your script has the executable permission bit set and
contains a *shebang*, i.e. a line of the form

.. code-block:: python

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


Hot-deploying Toil
------------------

Toil can be easily deployed to a remote host, given that both Python and Toil
are present. The first order of business after copying your workflow to each
host is to create and activate a virtualenv::

   $ virtualenv --system-site-packages venv
   $ . venv/bin/activate

Note that the virtualenv was created with the ``--system-site-packages`` option,
which ensures that globally-installed packages are accessible inside the virtualenv.
This is necessary as Toil and its dependencies must be installed globally.

From here, you can install your project and its dependencies::

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
   $ pip install fairydust
   $ cp -R workflow util venv/lib/python2.7/site-packages

Ideally, your project would have a ``setup.py`` file (see `setuptools`_) which
streamlines the installation process::

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

In each case, we have created a virtualenv with the ``--system-site-packages``
flag in the ``venv`` subdirectory then installed the ``fairydust`` distribution
from PyPI along with the two packages that our project consists of. (Again, both
Python and Toil are assumed to be present on the leader and all worker nodes.)
We can now run our workflow::

   $ python -m workflow.main --batchSystem=mesos …

.. important::

   If workflow's external dependencies contain native code (i.e. are not pure
   Python) then they must be manually installed on each worker.

.. note::

   Neither ``python setup.py develop`` nor ``pip install -e .`` can be used in
   this process as, instead of copying the source files, they create ``.egg-link``
   files that Toil can't hot-deploy. Similarly, ``python setup.py install``
   doesn't work either as it installs the project as a Python ``.egg`` which is
   also not currently supported by Toil (though it `could be`_ in the future).

   It should also be noted that while using the
   ``--single-version-externally-managed`` flag with ``setup.py`` will
   prevent the installation of your package as an ``.egg``, it will also disable
   the automatic installation of your project's dependencies.


.. _setuptools: http://setuptools.readthedocs.io/en/latest/index.html
.. _could be: https://github.com/BD2KGenomics/toil/issues/1367

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

Relying on shared filesystems
-----------------------------

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

Deploying Toil
--------------

Toil comes with the Toil Appliance, a Docker image with Mesos and Toil baked in.
It's easily deployed, only needs Docker, and allows for workflows to be run in
single-machine mode and for clusters of VMs to be provisioned. For more
information, see the :ref:`cloudInstallation` section.

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

