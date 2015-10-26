.. Toil documentation master file, created by
   sphinx-quickstart on Tue Aug 25 12:37:16 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Toil Overview
=============

Toil is a workflow engine written in 100% Python. It features:
    1)  Easy installation - e.g. "pip install toil".
    2)  A small easily mastered API built upon one core class.
    3)  Cross platform support that makes it easy to run a workflow in any one of 
        the supported environments without alteration. Currently this supports includes commercial clouds 
        like Amazon Web Services (including the spot-market) and Microsoft Azure, 
        as well as HPC cluster systems running GridEngine, Mesos or Parasol, 
        and individual multi-core machines. 
    4)  Scalability - Toil can easily handle workflows concurrently using hundreds of nodes
        and thousands of cores. 
    5)  Robustness - Toil workflows support arbitrary worker and leader failure, with strong 
        check-pointing that always allows resumption.
    6)  Temporary and persistent file management - by abstracting over the details of the underlying 
        file system, providing a uniform interface regardless of environment.
    7)  Declarative and dynamic workflow creation. Workflows can be declared statically, but new jobs
        can be added dynamically during execution within any existing job, allowing arbitrarily complex
        workflow graphs with, ultimately, millions of jobs within them.
    8)  Support for running databases and services within a workflow. For example, Apache Spark clusters
        can be easily integrated within a toil workflow as a service.

Scripting Quick Start
=====================

See README for installation. Toil's Job class contains the Toil API, documented below.
To begin, consider this short toil script:

TODO: Revise this example

.. code:: python

    from toil.job import Job
    from argparse import ArgumentParser

    class HelloWorld(Job):
        def __init__(self):
            Job.__init__(self,  memory=100000, cores=2, disk=20000)
        def run(self, fileStore):
            fileID = self.addChildJobFn(childFn, cores=1, memory="1M", disk="10M").rv()
            self.addFollowOn(FollowOn(fileID))
    
    def childFn(job):
        with job.fileStore.writeGlobalFileStream() as (fH, fileID):
            fH.write("Hello, World!")
            return fileID
        
    class FollowOn(Job):
        def __init__(self,fileId):
            Job.__init__(self)
            self.fileId=fileId
        def run(self, fileStore):
            tempDir = fileStore.getLocalTempDir()
            tempFilePath = "/".join([tempDir,"LocalCopy"])
            with fileStore.readGlobalFileStream(self.fileId) as globalFile:
                with open(tempFilePath, "w") as localFile:
                    localFile.write(globalFile.read())

    def main():
        parser = ArgumentParser()
        Job.Runner.addToilOptions(parser)
        options = parser.parse_args()
        Job.Runner.startToil(HelloWorld(),  options)

    if __name__=="__main__":
        main()

The script consists of three Jobs - the object based HelloWorld and FollowOn Jobs,
and the function based childFn Job. The object based Jobs inherit from the Job class,
and must invoke the Job constructor and implement the run method, where the user's code
to be executed should be placed.

Note that the constructor takes optional resource parameters, which specify the cores,
memory, and disk space needed for the job to run successfully. These resources can be
specified in bytes, or by passing in a string as in the constructor for the wrapJobFn().

Also notice the two types of descendant Jobs. HelloWorld specifies childFn as a child Job,
and FollowOn as a follow on Job. The only difference between a parent, children, and a
follow on is the order of execution. Parents are executed first, its children, finally
followed by the follow ons. The children and follow on jobs are run in parallel.


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

