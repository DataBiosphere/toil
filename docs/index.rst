.. Toil documentation master file, created by
   sphinx-quickstart on Tue Aug 25 12:37:16 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Toil
====

Toil is a workflow engine written in 100% Python. It features:
    *  Easy installation 
        - e.g. "pip install toil".
    *  A small API 
        - Easily mastered, the user API is built upon one core class.
    *  Cross platform support 
        - Develop and test on your laptop then deploy on any of the following:
            - Commercial clouds
                + Amazon Web Services (including the spot-market)
                + Microsoft Azure
            - Private clouds
                + Open stack
            - High Performance Computing Environments:
                + Grid Engine
                + Mesos
                + Parasol
            - Individual multi-core machines
    *  Scalability 
        - Toil can easily handle workflows concurrently using hundreds of nodes and thousands of cores. 
    *  Robustness 
        - Toil workflows support arbitrary worker and leader failure, with strong check-pointing that always allows resumption.
    *  Complete file management 
        - Temporary and persistent file management that abstracts over the details of the underlying file system, providing a uniform interface regardless of environment.
    *  Declarative and dynamic workflow creation
        - Workflows can be declared statically, but new jobs can be added dynamically during execution within any existing job, allowing arbitrarily complex workflow graphs with, ultimately, millions of jobs within them.
    *  Support for databases and services. 
        - For example, Apache Spark clusters can be created in seconds and easily integrated within a toil workflow as a service, with precisely defined time start and end times that fits with the flow of other jobs in the workflow.

Scripting Quick Start
=====================

See README for installation. Toil's Job class (:class:`toil.job.Job`) contains the Toil API, documented below.
To begin, consider this short toil script which illustrates defining a workflow and passing 
arguments between jobs::
    from toil.job import Job
    class HelloWorld(Job):
        def __init__(self):
            Job.__init__(self,  memory="1G", cores=2, disk="3G")
        def run(self, fileStore):
            message = self.addChildJobFn(hello, memory="1M").rv()
            return self.addFollowOnJobFn(world(message)).rv() 
    
    def hello(job, cores=2):
        return "Hello,"
        
    def world(job, message, memory="2G"):
        return message + " world!"
    
    def main():
        parser = Job.Runner.getDefaultArgumentParser()
        options = parser.parse_args()
        print Job.Runner.startToil(HelloWorld(), options) # Prints 
        # "Hello, World!", the return value from the HelloWorld 
        # class's run function.
    
    if __name__=="__main__":
        main()

The script consists of three jobs: HelloWorld, hello and world.

HelloWorld inherits from the :class:`toil.job.Job` class,
and invokes the Job constructor and implements the run method, where the user's code
to be executed is placed. 

HelloWorld specifies two successor jobs, hello and world. All child jobs
are executed immediately after the parent job, and in parallel. Hence after the HelloWorld
run function has been evaluated hello is run.

The follow-on jobs of a job are executed after the job, its children and any successor
jobs defined by its children have completed. In this example HelloWorld is run, 
then hello and finally, as a follow-on, world is evaluated. All follow-ons of a job are 
executed in parallel. 

Note that the HelloWorld constructor and the function declarations can take optional resource parameters, 
which specify the cores, memory, and disk space needed for the job to run successfully. These resources can be
specified in bytes, or by passing in a human readable string. For functions resource requirements
can be specified either within the function declaration or when the function is defined as job.

Return values can be passed between jobs using "promises" (see the :func:`toil.job.Job.rv` calls), 
a promise being a reference to a return value that is
replaced with the actual return value when the job in question is executed. 

The :class:`toil.job.Job.Runner` class handles the invocation of Toil workflows. In this example it is used
to generate an argument parser that can be used to collect command line inputs. Running this script from the command
line therefore gives the many optional Toil parameters. 

Toil Docs
=========

Contents:

.. toctree::
   :maxdepth: 2

   tutorial
   toilAPI
   batchSystem
   jobStore

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

