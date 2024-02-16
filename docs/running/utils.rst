.. _utils:

Toil Utilities
==============

Toil includes some utilities for inspecting or manipulating workflows during and after their execution. (There are additional :ref:`clusterUtils` available for working with Toil-managed clusters in the cloud.) 

The generic ``toil`` subcommand utilities are:

    ``stats`` --- Reports runtime and resource usage for all jobs in a specified jobstore (workflow must have originally been run using the ``--stats`` option).

    ``status`` --- Inspects a job store to see which jobs have failed, run successfully, etc.

    ``clean`` --- Delete the job store used by a previous Toil workflow invocation.

    ``kill`` --- Kills any running jobs in a rogue toil.

For information on a specific utility, run it with the ``--help`` option::

    toil stats --help

.. _cli_stats:

Stats Command
--------------

To use the stats command, a workflow must first be run using the ``--stats`` option.  Using this command makes certain
that toil does not delete the job store, no matter what other options are specified (i.e. normally the option
``--clean=always`` would delete the job store, but ``--stats`` will override this).

Running an Example
~~~~~~~~~~~~~~~~~~

We can run an example workflow and record stats::

    python3 discoverfiles.py file:my-jobstore --stats

Where ``discoverfiles.py`` is the following:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_stats.py

Notice the ``displayName`` key, which can rename a job, giving it an alias when it is finally displayed in stats.

Displaying Stats
~~~~~~~~~~~~~~~~

To see the runtime and resources used for each job when it was run, type ::

    toil stats file:my-jobstore

This should output something like the following::

    Batch System: single_machine
    Default Cores: 1  Default Memory: 2097152KiB
    Max Cores: unlimited
    Local CPU Time: 55.54 core·s  Overall Runtime: 26.23 s
    Worker
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            3 |     0.34   10.83   10.80   21.23   32.40 |     0.33   10.43   17.94   43.07   53.83 |     0.01    0.40   14.08   41.85   42.25 |  177168Ki 179312Ki 178730Ki 179712Ki 536192Ki |      0Ki     4Ki    22Ki    64Ki    68Ki
    Job
     Worker Jobs  |     min    med    ave    max
                  |       1      1 1.3333      2
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            4 |     0.33   10.83    8.10   10.85   32.38 |     0.33   10.43   13.46   41.70   53.82 |     0.01    1.68    2.78    9.02   11.10 |  177168Ki 179488Ki 178916Ki 179696Ki 715664Ki |      0Ki     4Ki    18Ki    64Ki    72Ki
     multithreadedJob
        Total Cores: 4.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.85   10.85   10.85   10.85   10.85 |    41.70   41.70   41.70   41.70   41.70 |     1.68    1.68    1.68    1.68    1.68 |  179488Ki 179488Ki 179488Ki 179488Ki 179488Ki |      4Ki     4Ki     4Ki     4Ki     4Ki
     efficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.83   10.83   10.83   10.83   10.83 |    10.43   10.43   10.43   10.43   10.43 |     0.40    0.40    0.40    0.40    0.40 |  179312Ki 179312Ki 179312Ki 179312Ki 179312Ki |      4Ki     4Ki     4Ki     4Ki     4Ki
     inefficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.38   10.38   10.38   10.38   10.38 |     1.36    1.36    1.36    1.36    1.36 |     9.02    9.02    9.02    9.02    9.02 |  179696Ki 179696Ki 179696Ki 179696Ki 179696Ki |     64Ki    64Ki    64Ki    64Ki    64Ki
     doNothing
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |     0.33    0.33    0.33    0.33    0.33 |     0.33    0.33    0.33    0.33    0.33 |     0.01    0.01    0.01    0.01    0.01 |  177168Ki 177168Ki 177168Ki 177168Ki 177168Ki |      0Ki     0Ki     0Ki     0Ki     0Ki

This report gives information on the resources used by your workflow. **Note that right now it does NOT track CPU and memory used inside Docker containers**, only Singularity containers.

There are three parts to this report.

Overall Summary
~~~~~~~~~~~~~~~

At the top is a section with overall summary statistics for the run::

    Batch System: single_machine
    Default Cores: 1  Default Memory: 2097152KiB
    Max Cores: unlimited
    Local CPU Time: 55.54 core·s  Overall Runtime: 26.23 s

This lists some important the settings for the Toil batch system that actually executed jobs. It also lists:

* The CPU time used on the local machine, in core seconds. This includes time used by the Toil leader itself (excluding some startup time), and time used by jobs that run under the leader (which, for the ``single_machine`` batch system, is all jobs). It does **not** include CPU used by jobs that ran on other machines.

* The overall wall-clock runtime of the workflow in seconds, as measured by the leader.

These latter two numbers don't count some startup/shutdown time spent loading and saving files, so you still may want to use the ``time`` shell built-in to time your Toil runs overall.

Worker Summary
~~~~~~~~~~~~~~

After the overall summary, there is a section with statistics about the Toil worker processes, which Toil used to execute your workflow's jobs::

    Worker
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            3 |     0.34   10.83   10.80   21.23   32.40 |     0.33   10.43   17.94   43.07   53.83 |     0.01    0.40   14.08   41.85   42.25 |  177168Ki 179312Ki 178730Ki 179712Ki 536192Ki |      0Ki     4Ki    22Ki    64Ki    68Ki

* The ``Count`` column shows that, to run this workflow, Toil had to submit 3 Toil worker processes to the backing scheduler. (In this case, it ran them all on the local machine.)

* The ``Real Time`` column shows satistics about the wall clock times that all the worker process took. All the sub-column values are in seconds.

* The ``CPU Time`` column shows statistics about the CPU usage amounts of all the worker processes. All the sub-column values are in core seconds.

* The ``CPU Wait`` column shows statistics about CPU time reserved for but **not** consumed by worker processes. In this example, the ``max`` and ``total`` are relatively high compared to both real time and CPU time, indicating that a lot of reserved CPU time went unused. This can indicate that the workflow is overestimating its required cores, that small jobs are running in the same resource reservations as large jobs via chaining, or that the workflow is having to wait around for slow disk I/O.

* The ``Memory`` column shows the peak memory usage of each worker process and its child processes.

* The ``Disk`` column shows the disk usage in each worker. This is polled at the **end** of each job that is run by the worker, so it may not always reflect the actual peak disk usage.

Job Breakdown
~~~~~~~~~~~~~

Finally, there is the breakdown of resource usage by jobs. This starts with a table summarizing the counts of jobs that ran on each worker::

    Job
     Worker Jobs  |     min    med    ave    max
                  |       1      1 1.3333      2

In this example, most of the workers ran one job each, but one worker managed to run two jobs, via chaining. (Jobs will chain when a job has only one dependant job, which in turn depends on only that first job, and the second job needs no more resources than the first job did.)

Next, we have statistics for resource usage over all jobs together::


        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            4 |     0.33   10.83    8.10   10.85   32.38 |     0.33   10.43   13.46   41.70   53.82 |     0.01    1.68    2.78    9.02   11.10 |  177168Ki 179488Ki 178916Ki 179696Ki 715664Ki |      0Ki     4Ki    18Ki    64Ki    72Ki

And finally, for each kind of job (as determined by the job's ``displayName``), we have statistics summarizing the resources used by the instances of that kind of job::

     multithreadedJob
        Total Cores: 4.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.85   10.85   10.85   10.85   10.85 |    41.70   41.70   41.70   41.70   41.70 |     1.68    1.68    1.68    1.68    1.68 |  179488Ki 179488Ki 179488Ki 179488Ki 179488Ki |      4Ki     4Ki     4Ki     4Ki     4Ki
     efficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.83   10.83   10.83   10.83   10.83 |    10.43   10.43   10.43   10.43   10.43 |     0.40    0.40    0.40    0.40    0.40 |  179312Ki 179312Ki 179312Ki 179312Ki 179312Ki |      4Ki     4Ki     4Ki     4Ki     4Ki
     inefficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.38   10.38   10.38   10.38   10.38 |     1.36    1.36    1.36    1.36    1.36 |     9.02    9.02    9.02    9.02    9.02 |  179696Ki 179696Ki 179696Ki 179696Ki 179696Ki |     64Ki    64Ki    64Ki    64Ki    64Ki
     doNothing
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |     0.33    0.33    0.33    0.33    0.33 |     0.33    0.33    0.33    0.33    0.33 |     0.01    0.01    0.01    0.01    0.01 |  177168Ki 177168Ki 177168Ki 177168Ki 177168Ki |      0Ki     0Ki     0Ki     0Ki     0Ki

For each job, we first list its name, and then the total cores that it asked for, summed across all instances of it. Then we show a table of statistics.

Here the ``*`` marker in the table headers becomes relevant; it shows that jobs are being sorted by the median of the real time used. You can control this with the ``--sortCategory`` option.

The columns meanings are the same as for the workers:

* The ``Count`` column shows the number of jobs of each type that ran.

* The ``Real Time`` column shows satistics about the wall clock times that instances of the job type took. All the sub-column values are in seconds.

* The ``CPU Time`` column shows statistics about the CPU usage amounts of each job. Note that ``multithreadedJob`` managed to use CPU time at faster than one core second per second, because it reserved multiple cores and ran multiple threads.

* The ``CPU Wait`` column shows statistics about CPU time reserved for but **not** consumed by jobs. Note that ``inefficientJob`` used hardly any of the cores it requested for most of its real time.

* The ``Memory`` column shows the peak memory usage of each job.

* The ``Disk`` column shows the disk usage at the **end** of each job. It may not always reflect the actual peak disk usage.

Example Cleanup
~~~~~~~~~~~~~~~

Once we're done looking at the stats, we can clean up the job store by running::

   toil clean file:my-jobstore

.. _cli_status:

Status Command
--------------

Continuing the example from the stats section above, if we ran our workflow with the command ::

    python3 discoverfiles.py file:my-jobstore --stats

We could interrogate our jobstore with the status command, for example::

    toil status file:my-jobstore

If the run was successful, this would not return much valuable information, something like ::

    2018-01-11 19:31:29,739 - toil.lib.bioio - INFO - Root logger is at level 'INFO', 'toil' logger at level 'INFO'.
    2018-01-11 19:31:29,740 - toil.utils.toilStatus - INFO - Parsed arguments
    2018-01-11 19:31:29,740 - toil.utils.toilStatus - INFO - Checking if we have files for Toil
    The root job of the job store is absent, the workflow completed successfully.

Otherwise, the ``status`` command should return the following:

    There are ``x`` unfinished jobs, ``y`` parent jobs with children, ``z`` jobs with services, ``a`` services, and ``b`` totally failed jobs currently in  ``c``.

Clean Command
-------------

If a Toil pipeline didn't finish successfully, or was run using ``--clean=always`` or ``--stats``, the job store will exist
until it is deleted. ``toil clean <jobStore>`` ensures that all artifacts associated with a job store are removed.
This is particularly useful for deleting AWS job stores, which reserves an SDB domain as well as an S3 bucket.

The deletion of the job store can be modified by the ``--clean`` argument, and may be set to ``always``, ``onError``,
``never``, or ``onSuccess`` (default).

Temporary directories where jobs are running can also be saved from deletion using the ``--cleanWorkDir``, which has
the same options as ``--clean``.  This option should only be run when debugging, as intermediate jobs will fill up
disk space.

Kill Command
------------

To kill all currently running jobs for a given jobstore, use the command ::

    toil kill file:my-jobstore

