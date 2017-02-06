.. _tutorial-ref:

Developing a workflow
=====================

This tutorial walks through the features of Toil necessary for developing a
workflow using the Toil Python API.


Scripting quick start
---------------------

To begin, consider this short toil script which illustrates defining a
workflow::

    from toil.job import Job

    def helloWorld(message, memory="2G", cores=2, disk="3G"):
        return "Hello, world!, here's a message: %s" % message

    j = Job.wrapFn(helloWorld, "woot")

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflow")
        print Job.Runner.startToil(j, options) #Prints Hello, world!, ...

The workflow consists of a single job. The resource requirements for that job
are (optionally) specified by keyword arguments (memory, cores, disk). The
script is run using :func:`toil.job.Job.Runner.getDefaultOptions`. Below we
explain the components of this code in detail.


Job basics
----------

The atomic unit of work in a Toil workflow is a *job* (:class:`toil.job.Job`).
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
:func:`toil.fileStore.FileStore.logToMaster`, which will be registered in the log
output of the leader process of the workflow.


Invoking a workflow
-------------------

We can add to the previous example to turn it into a complete workflow by
adding the necessary function calls to create an instance of HelloWorld and to
run this as a workflow containing a single job. This uses the
:class:`toil.job.Job.Runner` class, which is used to start and resume Toil
workflows. For example::

    from toil.job import Job

    class HelloWorld(Job):
        def __init__(self, message):
            Job.__init__(self,  memory="2G", cores=2, disk="3G")
            self.message = message

        def run(self, fileStore):
            return "Hello, world!, here's a message: %s" % self.message

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        print Job.Runner.startToil(HelloWorld("woot"), options)


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

For example::

    from toil.job import Job
    from toil.common import Toil

    class HelloWorld(Job):
        def __init__(self, message):
            Job.__init__(self,  memory="2G", cores=2, disk="3G")
            self.message = message

        def run(self, fileStore):
            fileStore.logToMaster("Hello, world!, I have a message: %s"
                                  % self.message)
    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"

        with Toil(options) as toil:
            if not toil.options.restart:
                job = HelloWorld("Smitty Werbenmanjensen, he was #1")
                toil.start(job)
            else:
                toil.restart()


The call to :func:`toil.job.Job.Runner.getDefaultOptions` creates a set of
default options for the workflow. The only argument is a description of how to
store the workflow's state in what we call a *job-store*. Here the job-store is
contained in a directory within the current working directory called
"toilWorkflowRun". Alternatively this string can encode other ways to store the
necessary state, e.g. an S3 bucket or Azure object store location. By default
the job-store is deleted if the workflow completes successfully.

The workflow is executed in the final line, which creates an instance of
HelloWorld and runs it as a workflow. Note all Toil workflows start from a
single starting job, referred to as the *root* job. The return value of the
root job is returned as the result of the completed workflow (see promises
below to see how this is a useful feature!).


Specifying arguments via the command line
-----------------------------------------

To allow command line control of the options we can use the
:func:`toil.job.Job.Runner.getDefaultArgumentParser`
method to create a :class:`argparse.ArgumentParser` object which can be used to
parse command line options for a Toil script. For example::

    from toil.job import Job

    class HelloWorld(Job):
        def __init__(self, message):
            Job.__init__(self,  memory="2G", cores=2, disk="3G")
            self.message = message

        def run(self, fileStore):
            return "Hello, world!, here's a message: %s" % self.message

    if __name__=="__main__":
        parser = Job.Runner.getDefaultArgumentParser()
        options = parser.parse_args()
        print Job.Runner.startToil(HelloWorld("woot"), options)

Creates a fully fledged script with all the options Toil exposed as command
line arguments. Running this script with "--help" will print the full list of
options.

Alternatively an existing :class:`argparse.ArgumentParser` or
:class:`optparse.OptionParser` object can have Toil script command line options
added to it with the :func:`toil.job.Job.Runner.addToilOptions` method.


Resuming a workflow
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
to :func:`toil.job.Job.Runner.startToil`. If node failures are expected it can
also be useful to use the integer "retryCount" option, which will attempt to
rerun a job retryCount number of times before marking it fully failed.

In the common scenario that a small subset of jobs fail (including retry
attempts) within a workflow Toil will continue to run other jobs until it can
do no more, at which point :func:`toil.job.Job.Runner.startToil` will raise a
:class:`toil.leader.FailedJobsException` exception. Typically at this point
the user can decide to fix the script and resume the workflow or delete the
job-store manually and rerun the complete workflow.


Functions and job functions
---------------------------

Defining jobs by creating class definitions generally involves the boilerplate
of creating a constructor. To avoid this the classes
:class:`toil.job.FunctionWrappingJob` and
:class:`toil.job.JobFunctionWrappingTarget` allow functions to be directly
converted to jobs. For example, the quick start example (repeated here)::

    from toil.job import Job

    def helloWorld(message, memory="2G", cores=2, disk="3G"):
        return "Hello, world!, here's a message: %s" % message

    j = Job.wrapFn(helloWorld, "woot")

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        print Job.Runner.startToil(j, options)

Is equivalent to the previous example, but using a function to define the job.

The function call::

    Job.wrapFn(helloWorld, "woot")

Creates the instance of the :class:`toil.job.FunctionWrappingTarget` that wraps
the function.

The keyword arguments *memory*, *cores* and *disk* allow resource requirements
to be specified as before. Even if they are not included as keyword arguments
within a function header they can be passed as arguments when wrapping a
function as a job and will be used to specify resource requirements.

We can also use the function wrapping syntax to a *job function*, a function
whose first argument is a reference to the wrapping job. Just like a *self*
argument in a class, this allows access to the methods of the wrapping job, see
:class:`toil.job.JobFunctionWrappingTarget`. For example::

    from toil.job import Job

    def helloWorld(job, message):
        job.fileStore.logToMaster("Hello world, "
        "I have a message: %s" % message) # This uses a logging function
        # of the toil.fileStore.FileStore class

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        print Job.Runner.startToil(Job.wrapJobFn(helloWorld, "woot"), options)

Here ``helloWorld()`` is a job function. It accesses the
:class:`toil.fileStore.FileStore` attribute of the job to log a message that will
be printed to the output console. Here the only subtle difference to note is
the line::

    Job.Runner.startToil(Job.wrapJobFn(helloWorld, "woot"), options)

Which uses the function :func:`toil.job.Job.wrapJobFn` to wrap the job function
instead of :func:`toil.job.Job.wrapFn` which wraps a vanilla function.


Workflows with multiple jobs
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
earlier ``helloWorld()`` job function::

    from toil.job import Job

    def helloWorld(job, message, memory="2G", cores=2, disk="3G"):
        job.fileStore.logToMaster("Hello world, "
        "I have a message: %s" % message) # This uses a logging function
        # of the toil.fileStore.FileStore class

    j1 = Job.wrapJobFn(helloWorld, "first")
    j2 = Job.wrapJobFn(helloWorld, "second or third")
    j3 = Job.wrapJobFn(helloWorld, "second or third")
    j4 = Job.wrapJobFn(helloWorld, "last")
    j1.addChild(j2)
    j1.addChild(j3)
    j1.addFollowOn(j4)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        Job.Runner.startToil(j1, options)

In the example four jobs are created, first ``j1`` is run, then ``j2`` and
``j3`` are run in parallel as children of ``j1``, finally ``j4`` is run as a
follow-on of ``j1``.

There are multiple short hand functions to achieve the same workflow, for
example::

    from toil.job import Job

    def helloWorld(job, message, memory="2G", cores=2, disk="3G"):
        job.fileStore.logToMaster("Hello world, "
        "I have a message: %s" % message) # This uses a logging function
        # of the toil.fileStore.FileStore class

    j1 = Job.wrapJobFn(helloWorld, "first")
    j2 = j1.addChildJobFn(helloWorld, "second or third")
    j3 = j1.addChildJobFn(helloWorld, "second or third")
    j4 = j1.addFollowOnJobFn(helloWorld, "last")

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        Job.Runner.startToil(j1, options)

Equivalently defines the workflow, where the functions
:func:`toil.job.Job.addChildJobFn` and :func:`toil.job.Job.addFollowOnJobFn`
are used to create job functions as children or follow-ons of an earlier job.

Jobs graphs are not limited to trees, and can express arbitrary directed acylic
graphs. For a precise definition of legal graphs see
:func:`toil.job.Job.checkJobGraphForDeadlocks`. The previous example could be
specified as a DAG as follows::

    from toil.job import Job

    def helloWorld(job, message, memory="2G", cores=2, disk="3G"):
        job.fileStore.logToMaster("Hello world, "
        "I have a message: %s" % message) # This uses a logging function
        # of the toil.fileStore.FileStore class

    j1 = Job.wrapJobFn(helloWorld, "first")
    j2 = j1.addChildJobFn(helloWorld, "second or third")
    j3 = j1.addChildJobFn(helloWorld, "second or third")
    j4 = j2.addChildJobFn(helloWorld, "last")
    j3.addChild(j4)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        Job.Runner.startToil(j1, options)

Note the use of an extra child edge to make ``j4`` a child of both ``j2`` and
``j3``.


Dynamic job creation
--------------------

The previous examples show a workflow being defined outside of a job. However,
Toil also allows jobs to be created dynamically within jobs. For example::

    from toil.job import Job

    def binaryStringFn(job, depth, message=""):
        if depth > 0:
            job.addChildJobFn(binaryStringFn, depth-1, message + "0")
            job.addChildJobFn(binaryStringFn, depth-1, message + "1")
        else:
            job.fileStore.logToMaster("Binary string: %s" % message)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        Job.Runner.startToil(Job.wrapJobFn(binaryStringFn, depth=5), options)

The job function ``binaryStringFn`` logs all possible binary strings of length
``n`` (here ``n=5``), creating a total of ``2^(n+2) - 1`` jobs dynamically and
recursively. Static and dynamic creation of jobs can be mixed in a Toil
workflow, with jobs defined within a job or job function being created at
run time.


Promises
--------

The previous example of dynamic job creation shows variables from a parent job
being passed to a child job. Such forward variable passing is naturally
specified by recursive invocation of successor jobs within parent jobs. This
can also be achieved statically by passing around references to the return
variables of jobs. In Toil this is achieved with promises, as illustrated in
the following example::

    from toil.job import Job

    def fn(job, i):
        job.fileStore.logToMaster("i is: %s" % i, level=100)
        return i+1

    j1 = Job.wrapJobFn(fn, 1)
    j2 = j1.addChildJobFn(fn, j1.rv())
    j3 = j1.addFollowOnJobFn(fn, j2.rv())

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        Job.Runner.startToil(j1, options)

Running this workflow results in three log messages from the jobs: ``i is 1``
from ``j1``, ``i is 2`` from ``j2`` and ``i is 3`` from ``j3``.

The return value from the first job is *promised* to the second job by the call
to :func:`toil.job.Job.rv` in the line::

    j2 = j1.addChildFn(fn, j1.rv())

The value of ``j1.rv()`` is a *promise*, rather than the actual return value of
the function, because ``j1`` for the given input has at that point not been
evaluated. A promise (:class:`toil.job.Promise`) is essentially a pointer to
for the return value that is replaced by the actual return value once it has
been evaluated. Therefore, when ``j2`` is run the promise becomes 2.

Promises can be quite useful. For example, we can combine dynamic job creation
with promises to achieve a job creation process that mimics the functional
patterns possible in many programming languages::

    from toil.job import Job

    def binaryStrings(job, message="", depth):
        if depth > 0:
            s = [ job.addChildJobFn(binaryStrings, message + "0",
                                    depth-1).rv(),
                  job.addChildJobFn(binaryStrings, message + "1",
                                    depth-1).rv() ]
            return job.addFollowOnFn(merge, s).rv()
        return [message]

    def merge(strings):
        return strings[0] + strings[1]

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        l = Job.Runner.startToil(Job.wrapJobFn(binaryStrings, depth=5), options)
        print l #Prints a list of all binary strings of length 5

The return value ``l`` of the workflow is a list of all binary strings of
length 10, computed recursively. Although a toy example, it demonstrates how
closely Toil workflows can mimic typical programming patterns.


Managing files within a workflow
--------------------------------

It is frequently the case that a workflow will want to create files, both
persistent and temporary, during its run. The :class:`toil.fileStore.FileStore`
class is used by jobs to manage these files in a manner that guarantees cleanup
and resumption on failure.

The :func:`toil.job.Job.run` method has a file store instance as an argument.
The following example shows how this can be used to create temporary files that
persist for the length of the job, be placed in a specified local disk of the
node and that will be cleaned up, regardless of failure, when the job finishes::

    from toil.job import Job

    class LocalFileStoreJob(Job):
        def run(self, fileStore):
            scratchDir = fileStore.getLocalTempDir() #Create a temporary
            # directory safely within the allocated disk space
            # reserved for the job.

            scratchFile = fileStore.getLocalTempFile() #Similarly
            # create a temporary file.

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        #Create an instance of FooJob which will
        # have at least 10 gigabytes of storage space.
        j = LocalFileStoreJob(disk="10G")
        #Run the workflow
        Job.Runner.startToil(j, options)

Job functions can also access the file store for the job. The equivalent of the
``LocalFileStoreJob`` class is::

    def localFileStoreJobFn(job):
        scratchDir = job.fileStore.getLocalTempDir()
        scratchFile = job.fileStore.getLocalTempFile()

Note that the ``fileStore`` attribute is accessed as an attribute of the
``job`` argument.

In addition to temporary files that exist for the duration of a job, the file
store allows the creation of files in a *global* store, which persists during
the workflow and are globally accessible (hence the name) between jobs. For
example::

    from toil.job import Job
    import os

    def globalFileStoreJobFn(job):
        job.fileStore.logToMaster("The following example exercises all the"
                                  " methods provided by the"
                                  " toil.fileStore.FileStore class")

        scratchFile = job.fileStore.getLocalTempFile() # Create a local
        # temporary file.

        with open(scratchFile, 'w') as fH: # Write something in the
            # scratch file.
            fH.write("What a tangled web we weave")

        # Write a copy of the file into the file-store;
        # fileID is the key that can be used to retrieve the file.
        fileID = job.fileStore.writeGlobalFile(scratchFile) #This write
        # is asynchronous by default

        # Write another file using a stream; fileID2 is the
        # key for this second file.
        with job.fileStore.writeGlobalFileStream(cleanup=True) as (fH, fileID2):
            fH.write("Out brief candle")

        # Now read the first file; scratchFile2 is a local copy of the file
        # that is read only by default.
        scratchFile2 = job.fileStore.readGlobalFile(fileID)

        # Read the second file to a desired location: scratchFile3.
        scratchFile3 = os.path.join(job.fileStore.getLocalTempDir(), "foo.txt")
        job.fileStore.readGlobalFile(fileID, userPath=scratchFile3)

        # Read the second file again using a stream.
        with job.fileStore.readGlobalFileStream(fileID2) as fH:
            print fH.read() #This prints "Out brief candle"

        # Delete the first file from the global file-store.
        job.fileStore.deleteGlobalFile(fileID)

        # It is unnecessary to delete the file keyed by fileID2
        # because we used the cleanup flag, which removes the file after this
        # job and all its successors have run (if the file still exists)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        Job.Runner.startToil(Job.wrapJobFn(globalFileStoreJobFn), options)

The example demonstrates the global read, write and delete functionality of the
file-store, using both local copies of the files and streams to read and write
the files. It covers all the methods provided by the file store interface.

What is obvious is that the file-store provides no functionality to update an
existing "global" file, meaning that files are, barring deletion, immutable.
Also worth noting is that there is no file system hierarchy for files in the
global file store. These limitations allow us to fairly easily support
different object stores and to use caching to limit the amount of network file
transfer between jobs.


Staging of files into the job store
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

Example::

    from toil.common import Toil
    from toil.job import Job

    class HelloWorld(Job):
        def __init__(self, inputFileID):
            Job.__init__(self,  memory="2G", cores=2, disk="3G")
            self.inputFileID = inputFileID

        with fileStore.readGlobalFileStream(self.inputFileID) as fi:
            with fileStore.writeGlobalFileStream() as (fo, outputFileID):
                fo.write(fi.read() + 'World!')
            return outputFileID


    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"


        with Toil(options) as toil:
            if not toil.options.restart:
                inputFileID = toil.importFile('file:///some/local/path')
                outputFileID = toil.start(HelloWorld(inputFileID))
            else:
                outputFileID = toil.restart()

            toil.exportFile(outputFileID, 'file:///some/other/local/path')


Using Docker containers in Toil
-------------------------------

Docker containers are commonly used with Toil. The combination of Toil and Docker
allows for pipelines to be fully portable between any platform that has both Toil
and Docker installed. Docker eliminates the need for the user to do any other tool
installation or environment setup.

In order to use Docker containers with Toil, Docker must be installed on all
workers of the cluster. Instructions for installing Docker can be found on the
`Docker`_ website.

.. _Docker: https://docs.docker.com/engine/getstarted/step_one/

When using CGCloud or Toil-based autoscaling, Docker will be automatically set up
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
``toil.lib.docker``. ``dockerCall`` provides a layer of abstraction over using the
``subprocess`` module to call Docker directly, and provides container cleanup on
job failure. When docker containers are run without this feature, failed jobs can
result in resource leaks.

In order to use ``dockerCall``, your installation of Docker must be set up to run
without ``sudo``. Instructions for setting this up can be found here_.

.. _here: https://docs.docker.com/engine/installation/linux/ubuntulinux/#/create-a-docker-group

An example of a basic ``dockerCall`` is below:

    dockerCall(job=job,
                tool='quay.io/ucsc_cgl/bwa',
                work_dir=job.fileStore.getLocalTempDir(),
                parameters=['index', '/data/reference.fa'])

``dockerCall`` can also be added to workflows like any other job function:

     from toil.job import Job
 
     align = Job.wrapJobFn(dockerCall,
                           tool='quay.io/ucsc_cgl/bwa',
                           work_dir=job.fileStore.getLocalTempDir(),
                           parameters=['index', '/data/reference.fa']))

     if __name__=="__main__":
         options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
         options.logLevel = "INFO"
         Job.Runner.startToil(align, options)

`cgl-docker-lib`_ contains ``dockerCall``-compatible Dockerized tools that are
commonly used in bioinformatics analysis. 

.. _cgl-docker-lib: https://github.com/BD2KGenomics/cgl-docker-lib/blob/master/README.md

The documentation provides guidelines for developing your own Docker containers
that can be used with Toil and ``dockerCall``. In order for a container to be
compatible with ``dockerCall``, it must have an ``ENTRYPOINT`` set to a wrapper
script, as described in cgl-docker-lib containerization standards. Alternately,
the entrypoint to the container can be set using the docker option
``--entrypoint``. The container should be runnable directly with Docker as:

    $ docker run <docker parameters> <tool name> <tool parameters>

For example:

    $ docker run -d quay.io/ucsc-cgl/bwa -s -o /data/aligned /data/ref.fa'


.. _service-dev-ref:

Services
--------

It is sometimes desirable to run *services*, such as a database or server,
concurrently with a workflow. The :class:`toil.job.Job.Service` class provides
a simple mechanism for spawning such a service within a Toil workflow, allowing
precise specification of the start and end time of the service, and providing
start and end methods to use for initialization and cleanup. The following
simple, conceptual example illustrates how services work::

    from toil.job import Job

    class DemoService(Job.Service):

        def start(self, fileStore):
            # Start up a database/service here
            return "loginCredentials" # Return a value that enables another
            # process to connect to the database

        def check(self):
            # A function that if it returns False causes the service to quit
            # If it raises an exception the service is killed and an error is reported
            return True

        def stop(self, fileStore):
            # Cleanup the database here
            pass

    j = Job()
    s = DemoService()
    loginCredentialsPromise = j.addService(s)

    def dbFn(loginCredentials):
        # Use the login credentials returned from the service's start method
        # to connect to the service
        pass

    j.addChildFn(dbFn, loginCredentialsPromise)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        Job.Runner.startToil(j, options)

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
make ``B`` a follow-on of ``Ap``. e.g.::

    from toil.job import Job

    # A is a job with children and follow-ons, for example:
    A = Job()
    A.addChild(Job())
    A.addFollowOn(Job())

    # B is a job which needs to run after A and its successors
    B = Job()

    # The way to do this without encapsulation is to make a
    # parent of A, Ap, and make B a follow-on of Ap.
    Ap = Job()
    Ap.addChild(A)
    Ap.addFollowOn(B)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        Job.Runner.startToil(Ap, options)

An *encapsulated job* ``E(A)`` of ``A`` saves making ``Ap``, instead we can
write::

    from toil.job import Job

    # A
    A = Job()
    A.addChild(Job())
    A.addFollowOn(Job())

    #Encapsulate A
    A = A.encapsulate()

    # B is a job which needs to run after A and its successors
    B = Job()

    # With encapsulation A and its successor subgraph appear
    # to be a single job, hence:
    A.addChild(B)

    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        Job.Runner.startToil(A, options)

Note the call to :func:`toil.job.Job.encapsulate` creates the
:class:`toil.job.Job.EncapsulatedJob`.
