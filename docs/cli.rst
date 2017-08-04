.. _commandRef:


.. _workflowOptions:

Toil Workflow Options
=====================

Toil provides many command line options when running a toil script (see :ref:`running`),
or using Toil to run a CWL script. Many of these are described below.
For most Toil scripts, executing::

    $ python MY_TOIL_SCRIPT.py --help

will show this list of options.

It is also possible to set and manipulate the options described when invoking a
Toil workflow from within Python using :func:`toil.job.Job.Runner.getDefaultOptions`, e.g.

.. code-block:: python

    options = Job.Runner.getDefaultOptions("./toilWorkflow") # Get the options object
    options.logLevel = "INFO" # Set the log level to the info level.

    with Toil(options) as toil:
        toil.start(Job())  # Run the script

.. _loggingRef:

Logging
-------
Toil hides stdout and stderr by default except in case of job failure.
For more robust logging options (default is INFO), use ``--logDebug`` or more generally, use
``--logLevel=``, which may be set to either ``OFF`` (or ``CRITICAL``), ``ERROR``, ``WARN`` (or ``WARNING``),
``INFO`` or ``DEBUG``. Logs can be directed to a file with ``--logFile=``.

If large logfiles are a problem, ``--maxLogFileSize`` (in bytes) can be set as well as ``--rotatingLogging``, which
prevents logfiles from getting too large.

Stats
-----
The ``--stats`` argument records statistics about the Toil workflow in the job store. After a Toil run has finished,
the entrypoint ``toil stats <jobStore>`` can be used to return statistics about cpu, memory, job duration, and more.
The job store will never be deleted with ``--stats``, as it overrides ``--clean``.



Restart
-------
In the event of failure, Toil can resume the pipeline by adding the argument ``--restart`` and rerunning the
python script. Toil pipelines can even be edited and resumed which is useful for development or troubleshooting.

Clean
-----
If a Toil pipeline didn't finish successfully, or is using a variation of ``--clean``, the job store will exist
until it is deleted. ``toil clean <jobStore>`` ensures that all artifacts associated with a job store are removed.
This is particularly useful for deleting AWS job stores, which reserves an SDB domain as well as an S3 bucket.

The deletion of the job store can be modified by the ``--clean`` argument, and may be set to ``always``, ``onError``,
``never``, or ``onSuccess`` (default).

Temporary directories where jobs are running can also be saved from deletion using the ``--cleanWorkDir``, which has
the same options as ``--clean``.  This option should only be run when debugging, as intermediate jobs will fill up
disk space.


Batch system
------------

Toil supports several different batch systems using the ``--batchSystem`` argument.
More information in the :ref:`batchsysteminterface`.


Default cores, disk, and memory
-------------------------------

Toil uses resource requirements to intelligently schedule jobs. The defaults for cores (1), disk (2G), and memory (2G),
can all be changed using ``--defaultCores``, ``--defaultDisk``, and ``--defaultMemory``. Standard suffixes
like K, Ki, M, Mi, G or Gi are supported.


Job store
---------

Running toil scripts has one required positional argument: the job store.  The default job store is just a path
to where the user would like the job store to be created. To use the :ref:`quick start <quickstart>` example,
if you're on a node that has a large **/scratch** volume, you can specify the jobstore be created there by
executing: ``python HelloWorld.py /scratch/my-job-store``, or more explicitly,
``python HelloWorld.py file:/scratch/my-job-store``. Toil uses the colon as way to explicitly name what type of
job store the user would like. The other job store types are AWS (``aws:region-here:job-store-name``),
Azure (``azure:account-name-here:job-store-name``), and the experimental Google
job store (``google:projectID-here:job-store-name``). Different types of job store options can be
looked up in :ref:`jobStoreInterface`.

Miscellaneous
-------------
Here are some additional useful arguments that don't fit into another category.

* ``--workDir`` sets the location where temporary directories are created for running jobs.
* ``--retryCount`` sets the number of times to retry a job in case of failure. Useful for non-systemic failures like HTTP requests.
* ``--sseKey`` accepts a path to a 32-byte key that is used for server-side encryption when using the AWS job store.
* ``--cseKey`` accepts a path to a 256-bit key to be used for client-side encryption on Azure job store.
* ``--setEnv <NAME=VALUE>`` sets an environment variable early on in the worker

For implementation-specific flags for schedulers like timelimits, queues, accounts, etc.. An environment variable can be
defined before launching the Job, i.e:

.. code-block:: console

    export TOIL_SLURM_ARGS="-t 1:00:00 -q fatq"

Running Workflows with Services
-------------------------------

Toil supports jobs, or clusters of jobs, that run as *services* (see :ref:`service-dev-ref` ) to other
*accessor* jobs. Example services include server databases or Apache Spark
Clusters. As service jobs exist to provide services to accessor jobs their
runtime is dependent on the concurrent running of their accessor jobs. The dependencies
between services and their accessor jobs can create potential deadlock scenarios,
where the running of the workflow hangs because only service jobs are being
run and their accessor jobs can not be scheduled because of too limited resources
to run both simultaneously. To cope with this situation Toil attempts to
schedule services and accessors intelligently, however to avoid a deadlock
with workflows running service jobs it is advisable to use the following parameters:

* ``--maxServiceJobs`` The maximum number of service jobs that can be run concurrently, excluding service jobs running on preemptable nodes.
* ``--maxPreemptableServiceJobs`` The maximum number of service jobs that can run concurrently on preemptable nodes.

Specifying these parameters so that at a maximum cluster size there will be
sufficient resources to run accessors in addition to services will ensure that
such a deadlock can not occur.

If too low a limit is specified then a deadlock can occur in which toil can
not schedule sufficient service jobs concurrently to complete the workflow.
Toil will detect this situation if it occurs and throw a
:class:`toil.DeadlockException` exception. Increasing the cluster size
and these limits will resolve the issue.

.. _clusterRef:

