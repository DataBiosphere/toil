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
            - High Performance Computing Environments
                + Grid Engine
                + Mesos
                + Parasol
            - Individual multi-core machines
    *  Complete file and stream management 
        - Temporary and persistent file management that abstracts over the details of the underlying file system, providing a uniform interface regardless of environment. Supports both atomic file transfer and streaming interfaces, and provides encryption of user data.
    *  Scalability 
        - Toil can easily handle workflows concurrently using hundreds of nodes and thousands of cores. 
    *  Robustness 
        - Toil workflows support arbitrary worker and leader failure, with strong check-pointing that always allows resumption.
    *  Efficiency
        - Caching and fine grained resource requirement specification and support for AWS spot market mean workflows can be executed with little waste. 
    *  Declarative and dynamic workflow creation
        - Workflows can be declared statically, but new jobs can be added dynamically during execution within any existing job, allowing arbitrarily complex workflow graphs with millions of jobs within them.
    *  Support for databases and services. 
        - For example, Apache Spark clusters can be created in seconds and easily integrated within a toil workflow as a service, with precisely defined time start and end times that fits with the flow of other jobs in the workflow.
    *  Draft Common Workflow Language (CWL) support
        - Complete support for the draft 2.0 CWL specification, allowing it to execute CWL workflows.
    *  Fully open source
        - An Apache license allows unrestricted use.

Scripting Quick Start
=====================

See README for installation. Toil's Job class (:class:`toil.job.Job`) contains the Toil API, documented below.
To begin, consider this short toil script which illustrates defining a workflow:: 
    from toil.job import Job
         
    def helloWorld(message, memory="2G", cores=2, disk="3G"):
        return "Hello, world!, here's a message: %s" % message
            
    j = Job.wrapFn(helloWorld, "woot")
               
    if __name__=="__main__":
        options = Job.Runner.getDefaultOptions("./toilWorkflow")
        print Job.Runner.startToil(j, options) #Prints Hello, world!, ..

The workflow consists of a single job, which calls the helloWorld function. The resource
requirements for that job are (optionally) specified by keyword arguments (memory, cores, disk).

The :class:`toil.job.Job.Runner` class handles the invocation of Toil workflows. 
It is fed an options object that configures the running the workflow. 
This can be populated by an argument parser object using 
:func:`toil.job.Job.Runner.getDefaultArgumentParser`, allowing all these options to specified 
via the command line to the script.

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

