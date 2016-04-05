Running A Workflow
==================

.. _quickstart:

Running Quick Start
-------------------

Starting with Python, a Toil workflow can be run with just three simple steps.

1. ``pip install toil``
2. Copy and paste the following code block into **HelloWorld.py**::

        from toil.job import Job

        def helloWorld(message, memory="2G", cores=2, disk="3G"):
            return "Hello, world!, here's a message: %s" % message

        j = Job.wrapFn(helloWorld, "You did it!")

        if __name__=="__main__":
            parser = Job.Runner.getDefaultArgumentParser()
            options = parser.parse_args()
            print Job.Runner.startToil(j, options) #Prints Hello, world!, ...

3. ``python HelloWorld.py file:jobStore``

Now you have run Toil on **singleMachine** (default batch system) using the **FileStore** job store. The first
positional argument after the ``.py`` is the location of the job store, a place where intermediate files are
written to. In this example, a folder called **jobStore** will be created where **HelloWorld.py** is run from.
Information on the jobStore can be found at :ref:`jobStoreInterface`.

Run ``python HelloWorld.py --help`` to see a complete list of available options.

Running CWL and WDL Workflows
-----------------------------

TODO


.. _runningAWS:

Running on AWS
--------------
See :ref:`installationAWS` to get setup for running on AWS.

Having followed the :ref:`quickstart` guide, the user can run their **HelloWorld.py** script on a distributed cluster
just by modifiying the run command.  We'll use the **AWS Jobstore**, which uses S3 as a intermediate job store.

Place the HelloWorld.py script on the leader node, and run::

    python --batchSystem=mesos --mesosMaster=mesos-master:5050 \
                    HelloWorld.py aws:us-west-2:my-s3-jobstore

Running on Azure
----------------

TODO

.. _runningOpenStack:

Running on Open Stack
---------------------

After getting setup with :ref:`installationOpenStack`, Toil scripts can be run just be designating a location of
the job store as shown in :ref:`quickstart`.  The temporary directories Toil creates to run jobs can be specified
with ``--workDir``::

    python HelloWorld.py file:jobStore --workDir /tmp/

