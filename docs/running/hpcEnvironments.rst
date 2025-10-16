.. _hpcEnvironmentsOverview:

HPC Environments
================

Toil is a flexible framework that can be leveraged in a variety of environments, including high-performance computing (HPC) environments.
Toil provides support for a number of batch systems, including `Grid Engine`_, `Slurm`_, `Torque`_ and `LSF`_, which are popular schedulers used in these environments.
Toil also supports `HTCondor`_, which is a popular scheduler for high-throughput computing (HTC).
To use one of these batch systems specify the ``--batchSystem`` argument to the workflow.

Due to the cost and complexity of maintaining support for these schedulers we currently consider all but Slurm to be "community supported", that is the core development team does not regularly test or develop support for these systems. However, there are members of the Toil community currently deploying Toil in a wide variety of HPC environments and we welcome external contributions.

Developing the support of a new or existing batch system involves extending the abstract batch system class :class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem`.

.. _runningSlurm:

Running on Slurm
----------------

When running Toil workflows on Slurm, you usually want to run the workflow itself from the head node. Toil will take care of running all the required ``sbatch`` commands for you. You probably do not want to submit the Toil workflow as a Slurm job with ``sbatch`` (although you can if you have a large number of workflows to run). You also probably do not want to manually allocate resources with ``sallocate``.

To run a Toil workflow on Slurm, include ``--batchSystem slurm`` in your command line arguments. Generally Slurm clusters have shared filesystems, meaning the file job store would be appropriate. You want to make sure to use a job store location that is shared across your Slurm cluster. Additionally, you will likely want to provide *another* shared directory with the ``--batchLogsDir`` option, to allow the Slurm job logs to be retrieved by Toil in case something goes wrong with a job.

For example, to run the sort example :ref:`sort example <sortExample>` on Slurm, assuming you are currently in a shared directory, you would type, on the cluster head node::

    $ mkdir -p logs
    $ python3 sort.py ./store --batchSystem slurm --batchLogsDir ./logs

By default, this does not include any time limit or particular Slurm partition. If your Slurm cluster requires time limits, add the ``--slurmTime`` option to set the time limit to use for jobs.

If you do specify a time limit, a partition will be automatically selected that can accommodate jobs of that duration, and a partition will be automatically selected for jobs that need GPUs. To use a particular partition, use the ``--slurmPartition`` argument. If you are running GPU jobs and they need to go to a different partition, use the ``--slurmGPUPartition`` argument. For example, to :ref:`run a WDL workflow from Dockstore <runWdl>` using GPUs on Slurm, with a time limit of 4 hours per job and partitions manually specified, you would run::

    $ toil-wdl-runner '#workflow/github.com/vgteam/vg_wdl/GiraffeDeepVariantFromGAF:gbz' \
      https://raw.githubusercontent.com/vgteam/vg_wdl/refs/heads/gbz/params/giraffe_and_deepvariant_gaf.json \
      --jobStore ./store --batchSystem slurm --batchLogsDir ./logs \
      --slurmTime 4:00:00 --slurmPartition medium --slurmGPUPartition gpu

Any additional Slurm ``sbatch`` arguments your cluster needs that aren't directly supported by Toil can be passed via the Toil ``--slurmArgs`` option, or the ``TOIL_SLURM_ARGS`` environment variable. (There is special handling for some Slurm options Toil needs to interact with, like ``--time`` or ``--partition``, if they are passed this way.)

Slurm Tips
~~~~~~~~~~

#. If using Toil workflows that run containers with Singularity on Slurm (such as WDL workflows), you will want to make sure that Singularity caching, and Toil's MiniWDL caching, use a shared directory across your cluster nodes. By default, Toil will configure Singularity to cache per-workflow and per-node, but in Slurm a shared filesystem is almost always available. Assuming your home directory is shared, to set this up, you can::

      $ echo 'export SINGULARITY_CACHEDIR="${HOME}/.singularity/cache"' >>~/.bashrc
      $ echo 'export MINIWDL__SINGULARITY__IMAGE_CACHE="${HOME}/.cache/miniwdl"' >>~/.bashrc
   
   Then make sure to log out and back in again for the setting to take effect.

#. If your home directory is *not* shared across the cluster nodes, make sure that you have installed Toil in such a way that it is in your ``PATH`` on the cluster nodes.

#. Slurm sandboxing and resource limitation does *not* apply to Docker containers, because there is no relationship between the sandbox cgroup that your Toil job runs in and the sandbox cgroup that the Docker daemon creates to run the Docker container your job requested to run. If you want your Toil jobs' containers to actually be *inside* their Slurm job resource allocations, you should make sure to run containers with Singularity or another user-mode or daemon-less containerization system.

#. Slurm can sometimes report that a job has finished before that job's changes to the cluster's shared filesystem are visible to other nodes or to the head node. Toil *tries* to anticipate and compensate for this situation, but there is no amount of waiting or retrying that Toil could do to guarantee correct behavior *in theory* in these situations; the shared filesystem could in theory be days or months behind. In practice, the delay is usually no more than a few seconds, and Toil can handle it. But if you are seeing odd behavior from Toil related to files not existing when they should or still existing when they shouldn't, your problem could be that your cluster's filesystem is unusually slow to reach consistency across nodes.

#. If you see warnings about ``XDG_RUNTIME_DIR``, your Slurm cluster might not be managing XDG login sessions correctly for Slurm jobs. Toil can work around this, but as a result of the workaround it might have trouble finding an appropriate "coordination directory" where it can store state files local to each Slurm node. If you are seeing unusual behavior like Toil jobs on one node waiting for operations on a different node, you can try giving Toil a path to a per-node, writable directory with the ``--coordinationDir`` option, to tell it where to put those files explicitly.

#. With a shared filesystem, Toil's caching system is not necessarily going to help your workflow. Try running and timing test workflows with ``--caching true`` and with ``--caching false``, to determine whether it is worth it for your workload to copy files from the shared filesystem to local storage on each node.

#. If running CWL workflows on Slurm, with a shared filesystem, you can try the ``--bypass-file-store`` option to ``toil-cwl-runner``. It may speed up your workflow, but you may also need to make sure to change Toil's work directory to a shared directory provided with the ``--workDir`` option in order for it to work properly across machines.


Standard Output/Error from Batch System Jobs
--------------------------------------------

Standard output and error from batch system jobs (except for the Mesos batch system) are redirected to files in the ``toil-<workflowID>`` directory created within the temporary directory specified by the ``--workDir`` option; see :ref:`optionsRef`.
Each file is named as follows: ``toil_job_<Toil job ID>_batch_<name of batch system>_<job ID from batch system>_<file description>.log``, where ``<file description>`` is ``std_output`` for standard output, and ``std_error`` for standard error.
HTCondor will also write job event log files with ``<file description> = job_events``.

If capturing standard output and error is desired, ``--workDir`` will generally need to be on a shared file system; otherwise if these are written to local temporary directories on each node (e.g. ``/tmp``) Toil will not be able to retrieve them.
Alternatively, the ``--noStdOutErr`` option forces Toil to discard all standard output and error from batch system jobs.

.. _Grid Engine: http://www.univa.com/oracle

.. _Slurm: https://www.schedmd.com/

.. _Torque: http://www.adaptivecomputing.com/products/open-source/torque/

.. _LSF: https://en.wikipedia.org/wiki/Platform_LSF

.. _HTCondor: https://research.cs.wisc.edu/htcondor/
