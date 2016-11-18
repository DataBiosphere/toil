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
work done. Here it just logs a message using :func:`toil.fileStore.CachingFileStore.logToMaster`,
which will be registered in the log output of the leader process of the workflow.


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
:class:`toil.job.leader.FailedJobsException` exception. Typically at this point
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
        # of the toil.fileStore.CachingFileStore class
        
    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
        options.logLevel = "INFO"
        print Job.Runner.startToil(Job.wrapJobFn(helloWorld, "woot"), options)

Here ``helloWorld()`` is a job function. It accesses the
:class:`toil.fileStore.CachingFileStore` attribute of the job to log a message that will
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
        # of the toil.fileStore.CachingFileStore class
        
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
        # of the toil.fileStore.CachingFileStore class
    
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
        # of the toil.fileStore.CachingFileStore class
    
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
    
    def binaryStringFn(job, message="", depth):
        if depth > 0:
            job.addChildJobFn(binaryStringFn, message + "0", depth-1)
            job.addChildJobFn(binaryStringFn, message + "1", depth-1)
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
persistent and temporary, during its run. The :class:`toil.fileStore.CachingFileStore`
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
                                  " toil.fileStore.CachingFileStore class")
    
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


Deploying a workflow
====================

If a Toil workflow is run on a single machine, there is nothing special you
need to do. You change into the directory containing your user script and
invoke like any Python script::

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

The script can have dependencies, as long as those are installed on the
machine, either globally, in a user-specific location or in a virtualenv. In
the latter case, the virtualenv must of course be active when you run the user
script.

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

In this scenario, the script is invoked as follows::

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

If you create a ``setup.py`` for your project (see `setuptools`_), the ``cp``
step can be replaced with ``python setup.py install``. Note that ``python
setup.py develop`` would not work here because it does not copy source files
but creates .egg-links instead, which Toil is not able to hot-deploy.

.. _setuptools: http://setuptools.readthedocs.io/en/latest/index.html

The main caveat to this solution is that the workflow's external dependencies
may not contain native code, i.e. they must be pure Python. If you have
dependencies that rely on native code, you must manually install them on each
worker.

The ``--system-site-packages`` option to ``virtualenv`` makes globally
installed packages visible inside the virtualenv. It is essential because, as
we'll see later, Toil and its dependencies must be installed globally and would
be inaccessible without that option.


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

.. _CGCloud: https://github.com/BD2KGenomics/cgcloud


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
