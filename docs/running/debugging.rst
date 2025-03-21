.. _debugging:

Toil Debugging
==============

Toil has a number of tools to assist in debugging.  Here we provide help in working through potential problems that a user might encounter in attempting to run a workflow.

Failing Fast
------------

If you have a workflow you are testing, you can pass ``--stopOnFirstFailure=True`` to have Toil stop the workflow as soon as any job is complely failed (i.e. runs out of retry attempts). If you don't want Toil to retry failing jobs, you can pass ``--retryCount=0``.

Reading the Log
---------------

Usually, at the end of a failed Toil worklfow, Toil will reproduce the job logs for the jobs that failed. You can look at the end of your workflow log and use the job logs to identify which jobs are failing and why.

.. _debuggingLog:

Using ``DEBUG`` Logging
-----------------------

When debugging a broken workflow, you can run the workflow with ``--logDebug``, to set the log level to ``DEBUG``.

When debug logging is on, the log from every Toil job is inserted in the main Toil log between these markers::

    =========>
         Toil job log is here
    <=========

Normally, only the logs of failing jobs and the output of commands run from WDL are reproduced like this.

Finding Failed Jobs in the Jobstore 
-----------------------------------

The ``toil status`` command (:ref:`cli_status`) can be used with the ``--failed`` option to list all failed jobs in a Toil job store.

You can also use it with the ``--logs`` option to retrieve per-job logs from the job store, for failed jobs that left them. These logs might be useful for diagnosing and fixing the problem.

Running a Job Locally
---------------------

If you have a failing job's ID or name, you can reproduce its failure on your local machine with ``toil debug-job``. See :ref:`cli_debug_job`.

For example, say you have this WDL workflow in ``test.wdl``. This workflow **cannot succeed**, due to the typo in the echo command:

.. code-block::
   
    version 1.0
    workflow test {
      call hello
    }
    task hello {
      input {
      }
      command <<<
        set -e
        echoo "Hello"
      >>>
      output {
      }
    }

You could try to run it with::

    toil-wdl-runner --jobStore ./store test.wdl --retryCount 0

But it will fail.

If you want to reproduce the failure later, or on another machine, you can first find out what jobs failed with ``toil status``::

    toil status --failed --noAggStats ./store

This will produce something like::

    [2024-03-14T17:45:15-0400] [MainThread] [I] [toil.utils.toilStatus] Traversing the job graph gathering jobs. This may take a couple of minutes.
    Failed jobs:
    'WDLTaskJob' test.hello.command kind-WDLTaskJob/instance-r9u6_dcs v6

And we can see a failed job with the display name ``test.hello.command``, which describes the job's location in the WDL workflow as the command section of the ``hello`` task called from the ``test`` workflow. (If you are writing a Toil Python script, this is the job's ``displayName``.) We can then run that job again locally by name with::

    toil debug-job ./store test.hello.command

If there were multiple failed jobs with that name (perhaps because of a WDL scatter), we would need to select one by Toil job ID instead::

    toil debug-job ./store kind-WDLTaskJob/instance-r9u6_dcs

And if we know there's only one failed WDL task, we can just tell Toil to rerun the failed ``WDLTaskJob`` by Python class name::

    toil debug-job ./store WDLTaskJob

Any of these will run the job (including any containers) on the local machine, where its execution can be observed live or monitored with a debugger.

Fetching Job Inputs
~~~~~~~~~~~~~~~~~~~

The ``--retrieveTaskDirectory`` option to ``toil debug-job`` allows you to send the input files for a job to a directory, and then stop running the job. It works for CWL and WDL jobs, and for Python workflows that call :meth:`toil.job.Job.files_downloaded_hook` after downloading their files. It will make the worker work in the specified directory, so the job's temporary directory will be at ``worker/job`` inside it. For WDL and CWL jobs that mount files into containers, there will also be an ``inside`` directory populated with symlinks to the files as they would be visible from the root of the container's filesystem.

For example, say you have a **broken WDL workflow** named ``example_alwaysfail_with_files.wdl``, like this:

.. literalinclude:: ../../src/toil/test/docs/scripts/example_alwaysfail_with_files.wdl

You can try and fail to run it like this::

    toil-wdl-runner --jobStore ./store example_alwaysfail_with_files.wdl --retryCount 0

If you dump the files from the failing job::

    toil debug-job ./store WDLTaskJob --retrieveTaskDirectory dumpdir

You will end up with a directory tree that looks, accorfing to ``tree``, something like this::

    dumpdir
    ├── inside
    │   └── mnt
    │       └── miniwdl_task_container
    │           └── work
    │               └── _miniwdl_inputs
    │                   ├── 0
    │                   │   └── test.txt -> ../../../../../../worker/job/2c6b3dc4-1d21-4abf-9937-db475e6a6bc2/test.txt
    │                   └── 1
    │                       └── test.txt -> ../../../../../../worker/job/e3d724e1-e6cc-4165-97f1-6f62ab0fb1ef/test.txt
    └── worker
        └── job
            ├── 2c6b3dc4-1d21-4abf-9937-db475e6a6bc2
            │   └── test.txt
            ├── e3d724e1-e6cc-4165-97f1-6f62ab0fb1ef
            │   └── test.txt
            ├── tmpr2j5yaic
            ├── tmpxqr9__y4
            └── work

    15 directories, 4 files

You can see where Toil downloaded the input files for the job to the worker's temporary directory, and how they would be mounted into the container.

.. _shellInContainer:

Interactively Investigating Running Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Say you have a **broken WDL workflow** that can't complete. Whenever you run ``tutorial_debugging_hangs.wdl``, it hangs:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_debugging_hangs.wdl
   :language: python

You can try to run it like this, using Docker containers. Pretend this was actually a run on a large cluster:

.. code-block:: console

    $ toil-wdl-runner --jobStore ./store tutorial_debugging_hangs.wdl --container docker

If you run this, it will hang at the ``TutorialDebugging.CompressFiles.command`` step:

.. code-block:: none

    [2024-06-18T12:12:49-0400] [MainThread] [I] [toil.leader] Issued job 'WDLTaskJob' TutorialDebugging.CompressFiles.command kind-WDLTaskJob/instance-y0ga_907 v1 with job batch system ID: 16 and disk: 2.0 Gi, memory: 2.0 Gi, cores: 1, accelerators: [], preemptible: False

    Workflow Progress  94%|██████████▎| 15/16 (0 failures) [00:36<00:02, 0.42 jobs/s]

Say you want to find out why it is stuck. First, you need to kill the workflow. Open a new shell in the same directory and run:

.. code-block:: console

    # toil kill ./store

You can also hit ``Control+C`` in its terminal window and wait for it to stop.

Then, you need to use ``toil debug-job`` to run the stuck job on your local machine:

.. code-block:: console

    $ toil debug-job ./store TutorialDebugging.CompressFiles.command

This produces some more informative logging messages, showing that the Docker container is managing to start up, but that it stays running indefinitely, with a repeating message:

.. code-block:: none

    [2024-06-18T12:18:00-0400] [MainThread] [N] [MiniWDLContainers] docker task running :: service: "lhui2bdzmzmg", task: "sg371eb2yk", node: "zyu9drdp6a", message: "started"
    [2024-06-18T12:18:01-0400] [MainThread] [D] [MiniWDLContainers] docker task status :: Timestamp: "2024-06-18T16:17:58.545272049Z", State: "running", Message: "started", ContainerStatus: {"ContainerID": "b7210b346637210b49e7b6353dd24108bc3632bbf2ce7479829d450df6ee453a", "PID": 36510, "ExitCode": 0}, PortStatus: {}
    [2024-06-18T12:18:03-0400] [MainThread] [D] [MiniWDLContainers] docker task status :: Timestamp: "2024-06-18T16:17:58.545272049Z", State: "running", Message: "started", ContainerStatus: {"ContainerID": "b7210b346637210b49e7b6353dd24108bc3632bbf2ce7479829d450df6ee453a", "PID": 36510, "ExitCode": 0}, PortStatus: {}
    [2024-06-18T12:18:04-0400] [MainThread] [D] [MiniWDLContainers] docker task status :: Timestamp: "2024-06-18T16:17:58.545272049Z", State: "running", Message: "started", ContainerStatus: {"ContainerID": "b7210b346637210b49e7b6353dd24108bc3632bbf2ce7479829d450df6ee453a", "PID": 36510, "ExitCode": 0}, PortStatus: {}
    ...

This also gives you the Docker container ID of the running container, ``b7210b346637210b49e7b6353dd24108bc3632bbf2ce7479829d450df6ee453a``. You can use that to get a shell inside the running container:

.. code-block:: console

    $ docker exec -ti b7210b346637210b49e7b6353dd24108bc3632bbf2ce7479829d450df6ee453a bash
    root@b7210b346637:/mnt/miniwdl_task_container/work#

Your shell is already in the working directory of the task, so we can inspect the files there to get an idea of how far the task has gotten. Has it managed to create ``script.py``? Has the script managed to create ``compressed.zip``? Let's check:

.. code-block:: console

    # ls -lah
    total 6.1M
    drwxrwxr-x 6 root root  192 Jun 18 16:17 .
    drwxr-xr-x 3 root root 4.0K Jun 18 16:17 ..
    drwxr-xr-x 3 root root   96 Jun 18 16:17 .toil_wdl_runtime
    drwxrwxr-x 8 root root  256 Jun 18 16:17 _miniwdl_inputs
    -rw-r--r-- 1 root root 6.0M Jun 18 16:23 compressed.zip
    -rw-r--r-- 1 root root 1.3K Jun 18 16:17 script.py

So we can see that the script exists, and the zip file also exists. So maybe the script is still running? We can check with ``ps``, but we need the ``-x`` option to include processes not under the current shell. We can also include the ``-u`` option to get statistics:

.. code-block:: console

    # ps -xu
    USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
    root         1  0.0  0.0   2316   808 ?        Ss   16:17   0:00 /bin/sh -c /bin/
    root         7  0.0  0.0   4208  3056 ?        S    16:17   0:00 /bin/bash ../com
    root         8  0.1  0.0   4208  1924 ?        S    16:17   0:00 /bin/bash ../com
    root        20 95.0  0.4  41096 36428 ?        R    16:17   7:09 python script.py
    root       645  0.0  0.0   4472  3492 pts/0    Ss   16:21   0:00 bash
    root      1379  0.0  0.0   2636   764 ?        S    16:25   0:00 sleep 1
    root      1380  0.0  0.0   8584  3912 pts/0    R+   16:25   0:00 ps -xu

Here we can see that ``python`` is indeed running, and it is using 95% of a CPU core. So we can surmise that Python is probably stuck spinning around in **an infinite loop**. Let's look at our files again:

.. code-block:: console

    # ls -lah
    total 8.1M
    drwxrwxr-x 6 root root  192 Jun 18 16:17 .
    drwxr-xr-x 3 root root 4.0K Jun 18 16:17 ..
    drwxr-xr-x 3 root root   96 Jun 18 16:17 .toil_wdl_runtime
    drwxrwxr-x 8 root root  256 Jun 18 16:17 _miniwdl_inputs
    -rw-r--r-- 1 root root 7.6M Jun 18  2024 compressed.zip
    -rw-r--r-- 1 root root 1.3K Jun 18 16:17 script.py

Note that, while we've been investigating, our ``compressed.zip`` file has grown from ``6.0M`` to ``7.6M``. So we now know that, not only is the Python script stuck in a loop, it is also **writing to the ZIP file** inside that loop.

Let's inspect the inputs:

.. code-block:: console

    # ls -lah _miniwdl_inputs/*
    _miniwdl_inputs/0:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root  65 Jun 18 16:15 stdout.txt

    _miniwdl_inputs/1:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root  65 Jun 18 16:15 stdout.txt

    _miniwdl_inputs/2:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root  65 Jun 18 16:15 stdout.txt

    _miniwdl_inputs/3:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root 384 Jun 18 16:15 stdout.txt

    _miniwdl_inputs/4:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root 387 Jun 18 16:15 stdout.txt

    _miniwdl_inputs/5:
    total 4.0K
    drwxrwxr-x 3 root root  96 Jun 18 16:17 .
    drwxrwxr-x 8 root root 256 Jun 18 16:17 ..
    -rw-r--r-- 1 root root 378 Jun 18 16:15 stdout.txt

There are the files that are meant to be being compressed into that ZIP file. But, hang on, there are only six of these files, and none of them is over 400 bytes in size. How did we get a multi-megabyte ZIP file? The script must be putting **more data than we expected** into the ZIP file it is writing.

Taking what we know, we can now inspect the Python script again and see if we can find **a way in which it could get stuck in an infinite loop, writing much more data to the ZIP than is actually in the input files**. We can also inspect it for WDL variable substitutions (there aren't any). Let's look at it with line numbers using the ``nl`` tool, numbering even blank lines with ``-b a``:

.. code-block:: console

    # nl -b a script.py
     1	import sys
     2	from zipfile import ZipFile
     3	import os
     4
     5	# Interpret command line arguments
     6	to_compress = list(reversed(sys.argv[1:]))
     7
     8	with ZipFile("compressed.zip", "w") as z:
     9	    while to_compress != []:
    10	        # Grab the file to add off the end of the list
    11	        input_filename = to_compress[-1]
    12	        # Now we need to write this to the zip file.
    13	        # What internal filename should we use?
    14	        basename = os.path.basename(input_filename)
    15	        disambiguation_number = 0
    16	        while True:
    17	            target_filename = str(disambiguation_number) + basename
    18	            try:
    19	                z.getinfo(target_filename)
    20	            except KeyError:
    21	                # Filename is free
    22	                break
    23	            # Otherwise try another name
    24	            disambiguation_number += 1
    25	        # Now we can actually make the compressed file
    26	        with z.open(target_filename, 'w') as out_stream:
    27	            with open(input_filename) as in_stream:
    28	                for line in in_stream:
    29	                    # Prefix each line of text with the original input file
    30	                    # it came from.
    31	                    # Also remember to encode the text as the zip file
    32	                    # stream is in binary mode.
    33	                    out_stream.write(f"{basename}: {line}".encode("utf-8"))

We have three loops here: ``while to_compress != []`` on line 9, ``while True`` on line 16, and ``for line in in_stream`` on line 28.

The ``while True`` loop is immediately suspicious, but none of the code inside it writes to the ZIP file, so we know we can't be stuck in there.

The ``for line in in_stream`` loop contains the only call that writes data to the ZIP, so we must be spending time inside it, but it is constrained to loop over a single file at a time, so it can't be the *infinite* loop we're looking for.

So then we must be infinitely looping at ``while to_compress != []``, and indeed we can see that ``to_compress`` **is never modified**, so it can never become ``[]``.

So now we have a theory as to what the problem is, and we can ``exit`` out of our shell in the container, and stop ``toil debug-job`` with ``Control+C``. Then we can make the following change to our workflow, adding code to the script to actually pop the handled files off the end of the list:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_debugging.patch
   :language: diff

If we apply that change and produce a new file, ``tutorial_debugging_works.wdl``, we can clean up from the old failed run and run a new one:

.. code-block:: console

    $ toil clean ./store
    $ toil-wdl-runner --jobStore ./store tutorial_debugging_works.wdl --container docker

This will produce a successful log, ending with something like:

.. code-block:: none

    [2024-06-18T12:42:20-0400] [MainThread] [I] [toil.leader] Finished toil run successfully.

    Workflow Progress 100%|███████████| 17/17 (0 failures) [00:24<00:00, 0.72 jobs/s]
    {"TutorialDebugging.compressed": "/Users/anovak/workspace/toil/src/toil/test/docs/scripts/wdl-out-u7fkgqbe/f5e16468-0cf6-4776-a5c1-d93d993c4db2/compressed.zip"}
    [2024-06-18T12:42:20-0400] [MainThread] [I] [toil.common] Successfully deleted the job store: FileJobStore(/Users/anovak/workspace/toil/src/toil/test/docs/scripts/store)

Note the line to standard output giving us the path on disk where the ``TutorialDebugging.compressed`` output from the workflow is. If you look at that ZIP file, you can see it contains the expected files, such as ``3stdout.txt``, which should contain this suitably prefixed dismayed whale:

.. code-block:: none

    stdout.txt:  ________ 
    stdout.txt: < Uh-oh! >
    stdout.txt:  -------- 
    stdout.txt:     \
    stdout.txt:      \
    stdout.txt:       \     
    stdout.txt:                     ##        .            
    stdout.txt:               ## ## ##       ==            
    stdout.txt:            ## ## ## ##      ===            
    stdout.txt:        /""""""""""""""""___/ ===        
    stdout.txt:   ~~~ {~~ ~~~~ ~~~ ~~~~ ~~ ~ /  ===- ~~~   
    stdout.txt:        \______ o          __/            
    stdout.txt:         \    \        __/             
    stdout.txt:           \____\______/   

When we're done inspecting the output, and satisfied that the workflow now works, we might want to clean up all the auto-generated WDL output directories from the successful and failed run(s):

.. code-block:: console

    $ rm -Rf wdl-out-*

Introspecting the Job Store
---------------------------

Note: Currently these features are only implemented for use locally (single machine) with the fileJobStore.

To view what files currently reside in the jobstore, run the following command::

    $ toil debug-file file:path-to-jobstore-directory \
          --listFilesInJobStore

When run from the commandline, this should generate a file containing the contents of the job store (in addition to
displaying a series of log messages to the terminal).  This file is named "jobstore_files.txt" by default and will be
generated in the current working directory.

If one wishes to copy any of these files to a local directory, one can run for example::

    $ toil debug-file file:path-to-jobstore \
          --fetch overview.txt *.bam *.fastq \
          --localFilePath=/home/user/localpath

To fetch ``overview.txt``, and all ``.bam`` and ``.fastq`` files.  This can be used to recover previously used input and output
files for debugging or reuse in other workflows, or use in general debugging to ensure that certain outputs were imported
into the jobStore.

Stats and Status
----------------
See :ref:`cli_stats` and :ref:`cli_status` for more about gathering statistics about job success, runtime, and resource usage from workflows.

Using a Python debugger
-----------------------

If you execute a workflow using the :code:`--debugWorker` flag, or if you use ``toil debug-job``, Toil will run the job in the process you started from the command line. This means
you can either use `pdb <https://docs.python.org/3/library/pdb.html>`_, or an `IDE that supports debugging Python <https://wiki.python.org/moin/PythonDebuggingTools#IDEs_with_Debug_Capabilities>`_ to interact with the Python process as it runs your job. Note that the :code:`--debugWorker` flag will
only work with the :code:`single_machine` batch system (the default), and not any of the custom job schedulers.
