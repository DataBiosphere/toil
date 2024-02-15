.. _utils:

Toil Utilities
--------------

Toil includes some utilities for inspecting or manipulating workflows during and after their execution. (THere are additional :ref:`clusterUtils` available for working with Toil-managed clusters in the cloud.) 

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

.. literalinclude:: ../../../src/toil/test/docs/scripts/tutorial_stats.py

Notice the ``displayName`` key, which can rename a job, giving it an alias when it is finally displayed in stats.

Displaying Stats
~~~~~~~~~~~~~~~~

To see the runtime and resources used for each job when it was run, type ::

    toil stats file:my-jobstore

This should output something like the following::

    Batch System: single_machine
    Default Cores: 1  Default Memory: 2097152KiB
    Max Cores: unlimited
    Total CPU Time: 56.34 core·s  Overall Runtime: 16.14 s
    Worker
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            4 |     0.35   10.80    8.21   10.90   32.83 |     0.33   10.38   13.47   41.71   53.90 |   -30.80    0.40   -5.27    9.33  -21.06 |  175968Ki 179968Ki 179104Ki 180608Ki 716416Ki |      0Ki     0Ki     0Ki     0Ki     0Ki
    Job
     Worker Jobs  |     min    med    ave    max
                  |       4      4      4      4
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            4 |     0.34   10.80    8.20   10.90   32.82 |     0.33   10.38   13.47   41.71   53.88 |     0.01    1.90    2.91    9.33   11.64 |  175968Ki 179968Ki 179044Ki 180368Ki 716176Ki |      0Ki     0Ki     0Ki     0Ki     0Ki
     doNothing
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |     0.34    0.34    0.34    0.34    0.34 |     0.33    0.33    0.33    0.33    0.33 |     0.01    0.01    0.01    0.01    0.01 |  175968Ki 175968Ki 175968Ki 175968Ki 175968Ki |      0Ki     0Ki     0Ki     0Ki     0Ki
     efficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.77   10.77   10.77   10.77   10.77 |    10.38   10.38   10.38   10.38   10.38 |     0.40    0.40    0.40    0.40    0.40 |  179872Ki 179872Ki 179872Ki 179872Ki 179872Ki |      0Ki     0Ki     0Ki     0Ki     0Ki
     inefficientJob
        Total Cores: 1.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.80   10.80   10.80   10.80   10.80 |     1.47    1.47    1.47    1.47    1.47 |     9.33    9.33    9.33    9.33    9.33 |  180368Ki 180368Ki 180368Ki 180368Ki 180368Ki |      0Ki     0Ki     0Ki     0Ki     0Ki
     multithreadedJob
        Total Cores: 4.0
        Count |                           Real Time (s)* |                        CPU Time (core·s) |                        CPU Wait (core·s) |                                    Memory (B) |                                 Disk (B)
            n |      min    med*     ave     max   total |      min     med     ave     max   total |      min     med     ave     max   total |       min      med      ave      max    total |      min     med     ave     max   total
            1 |    10.90   10.90   10.90   10.90   10.90 |    41.71   41.71   41.71   41.71   41.71 |     1.90    1.90    1.90    1.90    1.90 |  179968Ki 179968Ki 179968Ki 179968Ki 179968Ki |      0Ki     0Ki     0Ki     0Ki     0Ki

        
There are three parts to this report.

Overall Summary
~~~~~~~~~~~~~~~

At the top is a section with overall summary statistics for the run::

    Batch System: single_machine
    Default Cores: 1  Default Memory: 2097152KiB
    Max Cores: unlimited
    Total CPU Time: 56.34 core·s  Overall Runtime: 16.14 s

This lists some important the settings for the Toil batch system that actually executed jobs, and also the overall CPU time used in the workflow process, and the overall wall-clock runtime of the workflow as measured by the leader..


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

