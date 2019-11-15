.. _tutorial-ref:

Developing a Workflow
=====================

This tutorial walks through the features of Toil necessary for developing a
workflow using the Toil Python API.

.. note::

    "script" and "workflow" will be used interchangeably

Scripting Quick Start
---------------------

To begin, consider this short toil script which illustrates defining a
workflow:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_quickstart.py

The workflow consists of a single job. The resource requirements for that job
are (optionally) specified by keyword arguments (memory, cores, disk). The
script is run using :func:`toil.job.Job.Runner.getDefaultOptions`. Below we
explain the components of this code in detail.


Job Basics
----------

The atomic unit of work in a Toil workflow is a :class:`~toil.job.Job`.
User scripts inherit from this base class to define units of work. For example,
here is a more long-winded class-based version of the job in the quick start
example::

    from toil.job import Job

    class HelloWorld(Job):
        def __init__(self, message):
            Job.__init__(self,  memory="2G", cores=2, disk="3G")
            self.message = message

        def run(self, fileStore):
            return "Hello, world!, here's a message: %s" % self.message


In the example a class, HelloWorld, is defined. The constructor requests 2
gigabytes of memory, 2 cores and 3 gigabytes of local disk to complete the work.

The :func:`toil.job.Job.run` method is the function the user overrides to get
work done. Here it just logs a message using
:func:`toil.job.Job.log`, which will be registered in the log
output of the leader process of the workflow.


Invoking a Workflow
-------------------

We can add to the previous example to turn it into a complete workflow by
adding the necessary function calls to create an instance of HelloWorld and to
run this as a workflow containing a single job. This uses the
:class:`toil.job.Job.Runner` class, which is used to start and resume Toil
workflows. For example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_invokeworkflow.py

.. note::

    Do not include a `.` in the name of your python script (besides `.py` at the end). 
    This is to allow toil to import the types and  functions defined in your file while starting a new process. 

Alternatively, the more powerful :class:`toil.common.Toil` class can be used to
run and resume workflows. It is used as a context manager and allows for
preliminary setup, such as staging of files into the job store on the leader
node. An instance of the class is initialized by specifying an options object.
The actual workflow is then invoked by calling the
:func:`toil.common.Toil.start` method, passing the root job of the workflow,
or, if a workflow is being restarted, :func:`toil.common.Toil.restart` should
be used. Note that the context manager should have explicit if else branches
addressing restart and non restart cases. The boolean value for these if else
blocks is toil.options.restart.

For example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_invokeworkflow2.py

The call to :func:`toil.job.Job.Runner.getDefaultOptions` creates a set of
default options for the workflow. The only argument is a description of how to
store the workflow's state in what we call a *job-store*. Here the job-store is
contained in a directory within the current working directory called
"toilWorkflowRun". Alternatively this string can encode other ways to store the
necessary state, e.g. an S3 bucket object store location. By default
the job-store is deleted if the workflow completes successfully.

The workflow is executed in the final line, which creates an instance of
HelloWorld and runs it as a workflow. Note all Toil workflows start from a
single starting job, referred to as the *root* job. The return value of the
root job is returned as the result of the completed workflow (see promises
below to see how this is a useful feature!).


Specifying Commandline Arguments
--------------------------------

To allow command line control of the options we can use the
:func:`toil.job.Job.Runner.getDefaultArgumentParser`
method to create a :class:`argparse.ArgumentParser` object which can be used to
parse command line options for a Toil script. For example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_arguments.py

Creates a fully fledged script with all the options Toil exposed as command
line arguments. Running this script with "--help" will print the full list of
options.

Alternatively an existing :class:`argparse.ArgumentParser` or
:class:`optparse.OptionParser` object can have Toil script command line options
added to it with the :func:`toil.job.Job.Runner.addToilOptions` method.


Resuming a Workflow
-------------------

In the event that a workflow fails, either because of programmatic error within
the jobs being run, or because of node failure, the workflow can be resumed.
Workflows can only not be reliably resumed if the job-store itself becomes
corrupt.

Critical to resumption is that jobs can be rerun, even if they have apparently
completed successfully. Put succinctly, a user defined job should not corrupt
its input arguments. That way, regardless of node, network or leader failure
the job can be restarted and the workflow resumed.

To resume a workflow specify the "restart" option in the options object passed
to :func:`toil.common.Toil.start`. If node failures are expected it can
also be useful to use the integer "retryCount" option, which will attempt to
rerun a job retryCount number of times before marking it fully failed.

In the common scenario that a small subset of jobs fail (including retry
attempts) within a workflow Toil will continue to run other jobs until it can
do no more, at which point :func:`toil.common.Toil.start` will raise a
:exc:`toil.leader.FailedJobsException` exception. Typically at this point
the user can decide to fix the script and resume the workflow or delete the
job-store manually and rerun the complete workflow.


Functions and Job Functions
---------------------------

Defining jobs by creating class definitions generally involves the boilerplate
of creating a constructor. To avoid this the classes
:class:`toil.job.FunctionWrappingJob` and
:class:`toil.job.JobFunctionWrappingTarget` allow functions to be directly
converted to jobs. For example, the quick start example (repeated here):

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_quickstart.py

Is equivalent to the previous example, but using a function to define the job.

The function call::

    Job.wrapFn(helloWorld, "Woot")

Creates the instance of the :class:`toil.job.FunctionWrappingTarget` that wraps
the function.

The keyword arguments *memory*, *cores* and *disk* allow resource requirements
to be specified as before. Even if they are not included as keyword arguments
within a function header they can be passed as arguments when wrapping a
function as a job and will be used to specify resource requirements.

We can also use the function wrapping syntax to a *job function*, a function
whose first argument is a reference to the wrapping job. Just like a *self*
argument in a class, this allows access to the methods of the wrapping job, see
:class:`toil.job.JobFunctionWrappingTarget`. For example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_jobfunctions.py

Here ``helloWorld()`` is a job function. It uses the :func:`toil.job.Job.log`
to log a message that will
be printed to the output console. Here the only subtle difference to note is
the line::

    hello_job = Job.wrapJobFn(helloWorld, "Woot")

Which uses the function :func:`toil.job.Job.wrapJobFn` to wrap the job function
instead of :func:`toil.job.Job.wrapFn` which wraps a vanilla function.


Workflows with Multiple Jobs
----------------------------

A *parent* job can have *child* jobs and *follow-on* jobs. These relationships
are specified by methods of the job class, e.g. :func:`toil.job.Job.addChild`
and :func:`toil.job.Job.addFollowOn`.

Considering a set of jobs the nodes in a job graph and the child and follow-on
relationships the directed edges of the graph, we say that a job B that is on a
directed path of child/follow-on edges from a job ``A`` in the job graph is a
*successor* of ``A``, similarly ``A`` is a *predecessor* of ``B``.

A parent job's child jobs are run directly after the parent job has completed,
and in parallel. The follow-on jobs of a job are run after its child jobs and
their successors have completed. They are also run in parallel. Follow-ons
allow the easy specification of cleanup tasks that happen after a set of
parallel child tasks. The following shows a simple example that uses the
earlier ``helloWorld()`` job function:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_multiplejobs.py

In the example four jobs are created, first ``j1`` is run, then ``j2`` and
``j3`` are run in parallel as children of ``j1``, finally ``j4`` is run as a
follow-on of ``j1``.

There are multiple short hand functions to achieve the same workflow, for
example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_multiplejobs2.py

Equivalently defines the workflow, where the functions
:func:`toil.job.Job.addChildJobFn` and :func:`toil.job.Job.addFollowOnJobFn`
are used to create job functions as children or follow-ons of an earlier job.

Jobs graphs are not limited to trees, and can express arbitrary directed acyclic
graphs. For a precise definition of legal graphs see
:func:`toil.job.Job.checkJobGraphForDeadlocks`. The previous example could be
specified as a DAG as follows:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_multiplejobs3.py

Note the use of an extra child edge to make ``j4`` a child of both ``j2`` and
``j3``.


Dynamic Job Creation
--------------------

The previous examples show a workflow being defined outside of a job. However,
Toil also allows jobs to be created dynamically within jobs. For example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_dynamic.py

The job function ``binaryStringFn`` logs all possible binary strings of length
``n`` (here ``n=5``), creating a total of ``2^(n+2) - 1`` jobs dynamically and
recursively. Static and dynamic creation of jobs can be mixed in a Toil
workflow, with jobs defined within a job or job function being created at
run time.


.. _promises:

Promises
--------

The previous example of dynamic job creation shows variables from a parent job
being passed to a child job. Such forward variable passing is naturally
specified by recursive invocation of successor jobs within parent jobs. This
can also be achieved statically by passing around references to the return
variables of jobs. In Toil this is achieved with promises, as illustrated in
the following example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_promises.py

Running this workflow results in three log messages from the jobs: ``i is 1``
from ``j1``, ``i is 2`` from ``j2`` and ``i is 3`` from ``j3``.

The return value from the first job is *promised* to the second job by the call
to :func:`toil.job.Job.rv` in the following line::

    j2 = j1.addChildFn(fn, j1.rv())

The value of ``j1.rv()`` is a *promise*, rather than the actual return value of
the function, because ``j1`` for the given input has at that point not been
evaluated. A promise (:class:`toil.job.Promise`) is essentially a pointer to
for the return value that is replaced by the actual return value once it has
been evaluated. Therefore, when ``j2`` is run the promise becomes 2.

Promises also support indexing of return values::

    def parent(job):
        indexable = Job.wrapJobFn(fn)
        job.addChild(indexable)
        job.addFollowOnFn(raiseWrap, indexable.rv(2))

    def raiseWrap(arg):
        raise RuntimeError(arg) # raises "2"

    def fn(job):
        return (0, 1, 2, 3)

Promises can be quite useful. For example, we can combine dynamic job creation
with promises to achieve a job creation process that mimics the functional
patterns possible in many programming languages:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_promises2.py

The return value ``l`` of the workflow is a list of all binary strings of
length 10, computed recursively. Although a toy example, it demonstrates how
closely Toil workflows can mimic typical programming patterns.

Promised Requirements
---------------------

Promised requirements are a special case of :ref:`promises` that allow a job's
return value to be used as another job's resource requirements.

This is useful when, for example, a job's storage requirement is determined by a
file staged to the job store by an earlier job:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_requirements.py

Note that this also makes use of the ``size`` attribute of the :ref:`FileID` object.
This promised requirements mechanism can also be used in combination with an aggregator for
multiple jobs' output values::

    def parentJob(job):
        aggregator = []
        for fileNum in range(0,10):
            downloadJob = Job.wrapJobFn(stageFn, "File://"+os.path.realpath(__file__), cores=0.1, memory='32M', disk='1M')
            job.addChild(downloadJob)
            aggregator.append(downloadJob)

        analysis = Job.wrapJobFn(analysisJob, fileStoreID=downloadJob.rv(0),
                                 disk=PromisedRequirement(lambda xs: sum(xs), [j.rv(1) for j in aggregator]))
        job.addFollowOn(analysis)


.. admonition:: Limitations

    Just like regular promises, the return value must be determined prior to
    scheduling any job that depends on the return value. In our example above, notice
    how the dependent jobs were follow ons to the parent while promising jobs are
    children of the parent. This ordering ensures that all promises are
    properly fulfilled.

.. _FileID:


FileID
------

The :class:`toil.fileStore.FileID` class is a small wrapper around Python's builtin string class. It is used to
represent a file's ID in the file store, and has a ``size`` attribute that is the
file's size in bytes. This object is returned by ``importFile`` and ``writeGlobalFile``.


.. _managingFiles:

Managing files within a workflow
--------------------------------

It is frequently the case that a workflow will want to create files, both
persistent and temporary, during its run. The
:class:`toil.fileStores.abstractFileStore.AbstractFileStore` class is used by
jobs to manage these files in a manner that guarantees cleanup and resumption
on failure.

The :func:`toil.job.Job.run` method has a file store instance as an argument.
The following example shows how this can be used to create temporary files that
persist for the length of the job, be placed in a specified local disk of the
node and that will be cleaned up, regardless of failure, when the job finishes:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_managing.py

Job functions can also access the file store for the job. The equivalent of the
``LocalFileStoreJob`` class is ::

    def localFileStoreJobFn(job):
        scratchDir = job.tempDir
        scratchFile = job.fileStore.getLocalTempFile()

Note that the ``fileStore`` attribute is accessed as an attribute of the
``job`` argument.

In addition to temporary files that exist for the duration of a job, the file
store allows the creation of files in a *global* store, which persists during
the workflow and are globally accessible (hence the name) between jobs. For
example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_managing2.py

The example demonstrates the global read, write and delete functionality of the
file-store, using both local copies of the files and streams to read and write
the files. It covers all the methods provided by the file store interface.

What is obvious is that the file-store provides no functionality to update an
existing "global" file, meaning that files are, barring deletion, immutable.
Also worth noting is that there is no file system hierarchy for files in the
global file store. These limitations allow us to fairly easily support
different object stores and to use caching to limit the amount of network file
transfer between jobs.


Staging of Files into the Job Store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

External files can be imported into or exported out of the job store prior to
running a workflow when the :class:`toil.common.Toil` context manager is used
on the leader. The context manager provides methods
:func:`toil.common.Toil.importFile`, and :func:`toil.common.Toil.exportFile`
for this purpose. The destination and source locations of such files are
described with URLs passed to the two methods. A list of the currently
supported URLs can be found at
:func:`toil.jobStores.abstractJobStore.AbstractJobStore.importFile`. To import
an external file into the job store as a shared file, pass the optional
``sharedFileName`` parameter to that method.

If a workflow fails for any reason an imported file acts as any other file in
the job store. If the workflow was configured such that it not be cleaned up on
a failed run, the file will persist in the job store and needs not be staged
again when the workflow is resumed.

Example:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_staging.py

Using Docker Containers in Toil
-------------------------------

Docker containers are commonly used with Toil. The combination of Toil and Docker
allows for pipelines to be fully portable between any platform that has both Toil
and Docker installed. Docker eliminates the need for the user to do any other tool
installation or environment setup.

In order to use Docker containers with Toil, Docker must be installed on all
workers of the cluster. Instructions for installing Docker can be found on the
`Docker`_ website.

.. _Docker: https://docs.docker.com/engine/getstarted/step_one/

When using Toil-based autoscaling, Docker will be automatically set up
on the cluster's worker nodes, so no additional installation steps are necessary.
Further information on using Toil-based autoscaling can be found in the :ref:`Autoscaling`
documentation.

In order to use docker containers in a Toil workflow, the container can be built
locally or downloaded in real time from an online docker repository like Quay_. If
the container is not in a repository, the container's layers must be accessible on
each node of the cluster.

.. _Quay: quay.io

When invoking docker containers from within a Toil workflow, it is strongly
recommended that you use :func:`dockerCall`, a toil job function provided in
``toil.lib.docker``. ``dockerCall`` leverages docker's own python API,
and provides container cleanup on job failure. When docker containers are
run without this feature, failed jobs can result in resource leaks.  Docker's
API can be found at `docker-py`_.

.. _docker-py: https://docker-py.readthedocs.io/en/stable/

In order to use ``dockerCall``, your installation of Docker must be set up to run
without ``sudo``. Instructions for setting this up can be found here_.

.. _here: https://docs.docker.com/engine/installation/linux/ubuntulinux/#/create-a-docker-group

An example of a basic ``dockerCall`` is below::

    dockerCall(job=job,
                tool='quay.io/ucsc_cgl/bwa',
                workDir=job.tempDir,
                parameters=['index', '/data/reference.fa'])

Note the assumption that `reference.fa` file is located in `/data`. This is Toil's
standard convention as a mount location to reduce boilerplate when calling `dockerCall`.
Users can choose their own mount locations by supplying a `volumes` kwarg to `dockerCall`,
such as: `volumes={working_dir: {'bind': '/data', 'mode': 'rw'}}`, where `working_dir`
is an absolute path on the user's filesystem.

``dockerCall`` can also be added to workflows like any other job function:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_docker.py

`cgl-docker-lib`_ contains ``dockerCall``-compatible Dockerized tools that are
commonly used in bioinformatics analysis.

.. _cgl-docker-lib: https://github.com/BD2KGenomics/cgl-docker-lib/blob/master/README.md

The documentation provides guidelines for developing your own Docker containers
that can be used with Toil and ``dockerCall``. In order for a container to be
compatible with ``dockerCall``, it must have an ``ENTRYPOINT`` set to a wrapper
script, as described in cgl-docker-lib containerization standards.  This can be
set by passing in the optional keyword argument, 'entrypoint'.  Example:

     entrypoint=["/bin/bash","-c"]


dockerCall supports currently the 75 keyword arguments found in the python
`Docker API`_, under the 'run' command.

.. _Docker API: https://docker-py.readthedocs.io/en/stable/containers.html


.. _service-dev-ref:

Services
--------

It is sometimes desirable to run *services*, such as a database or server,
concurrently with a workflow. The :class:`toil.job.Job.Service` class provides
a simple mechanism for spawning such a service within a Toil workflow, allowing
precise specification of the start and end time of the service, and providing
start and end methods to use for initialization and cleanup. The following
simple, conceptual example illustrates how services work:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_services.py

In this example the DemoService starts a database in the start method,
returning an object from the start method indicating how a client job would
access the database. The service's stop method cleans up the database, while
the service's check method is polled periodically to check the service is alive.

A DemoService instance is added as a service of the root job ``j``, with
resource requirements specified. The return value from
:func:`toil.job.Job.addService` is a promise to the return value of the
service's start method. When the promised is fulfilled it will represent how to
connect to the database. The promise is passed to a child job of ``j``, which
uses it to make a database connection. The services of a job are started before
any of its successors have been run and stopped after all the successors of the
job have completed successfully.

Multiple services can be created per job, all run in parallel. Additionally,
services can define sub-services using :func:`toil.job.Job.Service.addChild`.
This allows complex networks of services to be created, e.g. Apache Spark
clusters, within a workflow.


.. _checkpoints:

Checkpoints
-----------

Services complicate resuming a workflow after failure, because they can create
complex dependencies between jobs. For example, consider a service that
provides a database that multiple jobs update. If the database service fails
and loses state, it is not clear that just restarting the service will allow
the workflow to be resumed, because jobs that created that state may have
already finished. To get around this problem Toil supports *checkpoint* jobs,
specified as the boolean keyword argument ``checkpoint`` to a job or wrapped
function, e.g.::

    j = Job(checkpoint=True)

A checkpoint job is rerun if one or more of its successors fails its retry
attempts, until it itself has exhausted its retry attempts. Upon restarting a
checkpoint job all its existing successors are first deleted, and then the job
is rerun to define new successors. By checkpointing a job that defines a
service, upon failure of the service the database and the jobs that access the
service can be redefined and rerun.

To make the implementation of checkpoint jobs simple, a job can only be a
checkpoint if when first defined it has no successors, i.e. it can only define
successors within its run method.


Encapsulation
-------------

Let ``A`` be a root job potentially with children and follow-ons. Without an
encapsulated job the simplest way to specify a job ``B`` which runs after ``A``
and all its successors is to create a parent of ``A``, call it ``Ap``, and then
make ``B`` a follow-on of ``Ap``. e.g.:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_encapsulation.py

An *encapsulated job* ``E(A)`` of ``A`` saves making ``Ap``, instead we can
write:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_encapsulation2.py

Note the call to :func:`toil.job.Job.encapsulate` creates the
:class:`toil.job.Job.EncapsulatedJob`.

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
not the worker nodes. Auto-deployment does not work here because Toil cannot
auto-deploy itself, the classic "Which came first, chicken or egg?" problem.

In other words, you shouldn't explicitly depend on Toil. Document the
dependency instead (as in "This workflow needs Toil version X.Y.Z to be
installed") and optionally add a version check to your ``setup.py``. Refer to
the ``check_version()`` function in the ``toil-lib`` project's `setup.py`_ for
an example. Alternatively, you can also just depend on ``toil-lib`` and you'll
get that check for free.

.. _setup.py: https://github.com/BD2KGenomics/toil-lib/blob/master/setup.py

If your workflow depends on a dependency of Toil,
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

Best Practices for Dockerizing Toil Workflows
---------------------------------------------

`Computational Genomics Lab`_'s `Dockstore`_ based production system provides workflow authors a
way to run Dockerized versions of their pipeline in an automated, scalable fashion. To be compatible
with this system of a workflow should meet the following requirements. In addition
to the Docker container, a common workflow language `descriptor file`_ is needed. For inputs:

* Only command line arguments should be used for configuring the workflow. If
  the workflow relies on a configuration file, like `Toil-RNAseq`_ or `ProTECT`_, a
  wrapper script inside the Docker container can be used to parse the CLI and
  generate the necessary configuration file.
* All inputs to the pipeline should be explicitly enumerated rather than implicit.
  For example, don't rely on one FASTQ read's path to discover the location of its
  pair. This is necessary since all inputs are mapped to their own isolated directories
  when the Docker is called via Dockstore.
* All inputs must be documented in the CWL descriptor file. Examples of this file can be seen in
  both `Toil-RNAseq`_ and `ProTECT`_.

For outputs:

* All outputs should be written to a local path rather than S3.
* Take care to package outputs in a local and user-friendly way. For example,
  don't tar up all output if there are specific files that will care to see individually.
* All output file names should be deterministic and predictable. For example,
  don't prepend the name of an output file with PASS/FAIL depending on the outcome
  of the pipeline.
* All outputs must be documented in the CWL descriptor file. Examples of this file can be seen in
  both `Toil-RNAseq`_ and `ProTECT`_.

.. _descriptor file: https://dockstore.org/docs/getting-started-with-cwl
.. _Computational Genomics Lab: https://cgl.genomics.ucsc.edu/
.. _Dockstore: https://dockstore.org/docs
.. _Toil-RNAseq: https://github.com/BD2KGenomics/toil-rnaseq
.. _ProTECT: https://github.com/BD2KGenomics/protect
