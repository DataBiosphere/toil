.. _envars:

Environment Variables
=====================
There are several environment variables that affect the way Toil runs.

+----------------------------------+----------------------------------------------------+
| TOIL_CHECK_ENV                   | A flag that determines whether Toil will try to    |
|                                  | refer back to a Python virtual environment in      |
|                                  | which it is installed when composing commands that |
|                                  | may be run on other hosts. If set to ``True``, if  |
|                                  | Toil is installed in the current virtual           |
|                                  | environment, it will use absolute paths to its own |
|                                  | executables (and the virtual environment must thus |
|                                  | be available on at the same path on all nodes).    |
|                                  | Otherwise, Toil internal commands such as          |
|                                  | ``_toil_worker`` will be resolved according to the |
|                                  | ``PATH`` on the node where they are executed. This |
|                                  | setting can be useful in a shared HPC environment, |
|                                  | where users may have their own Toil installations  |
|                                  | in virtual environments.                           |
+----------------------------------+----------------------------------------------------+
| TOIL_WORKDIR                     | An absolute path to a directory where Toil will    |
|                                  | write its temporary files. This directory must     |
|                                  | exist on each worker node and may be set to a      |
|                                  | different value on each worker. The ``--workDir``  |
|                                  | command line option overrides this. When using the |
|                                  | Toil docker container, such as on Kubernetes, this |
|                                  | defaults to ``/var/lib/toil``. When using Toil     |
|                                  | autoscaling with Mesos, this is somewhere inside   |
|                                  | the Mesos sandbox. In all other cases, the         |
|                                  | system's `standard temporary directory`_ is used.  |
+----------------------------------+----------------------------------------------------+
| TOIL_WORKDIR_OVERRIDE            | An absolute path to a directory where Toil will    |
|                                  | write its temporary files. This overrides          |
|                                  | ``TOIL_WORKDIR`` and the  ``--workDir`` command    |
|                                  | line option.                                       |
+----------------------------------+----------------------------------------------------+
| TOIL_COORDINATION_DIR            | An absolute path to a directory where Toil will    |
|                                  | write its lock files. This directory must exist on |
|                                  | each worker node and may be set to a different     |
|                                  | value on each worker. The ``--coordinationDir``    |
|                                  | command line option overrides this.                |
+----------------------------------+----------------------------------------------------+
| TOIL_COORDINATION_DIR_OVERRIDE   | An absolute path to a directory where Toil will    |
|                                  | write its lock files. This overrides               |
|                                  | ``TOIL_COORDINATION_DIR`` and the                  |
|                                  | ``--coordinationDir`` command    line option.      |
+----------------------------------+----------------------------------------------------+
| TOIL_BATCH_LOGS_DIR              | A directory to save batch system logs into, where  |
|                                  | the leader can access them. The ``--batchLogsDir`` |
|                                  | option overrides this. Only works for grid engine  |
|                                  | batch systems such as gridengine, htcondor,        |
|                                  | torque, slurm, and lsf.                            |
+----------------------------------+----------------------------------------------------+
| TOIL_KUBERNETES_HOST_PATH        | A path on Kubernetes hosts that will be mounted as |
|                                  | the Toil work directory in the workers, to allow   |
|                                  | for shared caching. Will be created if it doesn't  |
|                                  | already exist.                                     |
+----------------------------------+----------------------------------------------------+
| TOIL_KUBERNETES_OWNER            | A name prefix for easy identification of           |
|                                  | Kubernetes jobs. If not set, Toil will use the     |
|                                  | current user name.                                 |
+----------------------------------+----------------------------------------------------+
| TOIL_KUBERNETES_SERVICE_ACCOUNT  | A service account name to apply when creating      |
|                                  | Kubernetes pods.                                   |
+----------------------------------+----------------------------------------------------+
| TOIL_KUBERNETES_POD_TIMEOUT      | Seconds to wait for a scheduled Kubernetes pod to  |
|                                  | start running.                                     |
+----------------------------------+----------------------------------------------------+
| KUBE_WATCH_ENABLED               | A boolean variable that allows for users           |
|                                  | to utilize kubernetes watch stream feature         |
|                                  | instead of polling for running jobs. Default       |
|                                  | value is set to False.                             |
+----------------------------------+----------------------------------------------------+
| TOIL_APPLIANCE_SELF              | The fully qualified reference for the Toil         |
|                                  | Appliance you wish to use, in the form             |
|                                  | ``REPO/IMAGE:TAG``.                                |
|                                  | ``quay.io/ucsc_cgl/toil:3.6.0`` and                |
|                                  | ``cket/toil:3.5.0`` are both examples of valid     |
|                                  | options. Note that since Docker defaults to        |
|                                  | Dockerhub repos, only quay.io repos need to        |
|                                  | specify their registry.                            |
+----------------------------------+----------------------------------------------------+
| TOIL_DOCKER_REGISTRY             | The URL of the registry of the Toil Appliance      |
|                                  | image you wish to use. Docker will use Dockerhub   |
|                                  | by default, but the quay.io registry is also       |
|                                  | very popular and easily specifiable by setting     |
|                                  | this option to ``quay.io``.                        |
+----------------------------------+----------------------------------------------------+
| TOIL_DOCKER_NAME                 | The name of the Toil Appliance image you           |
|                                  | wish to use. Generally this is simply ``toil`` but |
|                                  | this option is provided to override this,          |
|                                  | since the image can be built with arbitrary names. |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_SECRET_NAME             | For the Kubernetes batch system, the name of a     |
|                                  | Kubernetes secret which contains a ``credentials`` |
|                                  | file granting access to AWS resources. Will be     |
|                                  | mounted as ``~/.aws`` inside Kubernetes-managed    |
|                                  | Toil containers. Enables the AWSJobStore to be     |
|                                  | used with the Kubernetes batch system, if the      |
|                                  | credentials allow access to S3 and SimpleDB.       |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_ZONE                    | Zone to use when using AWS. Also determines region.|
|                                  | Overrides TOIL_AWS_REGION.                         |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_REGION                  | Region to use when using AWS.                      |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_AMI                     | ID of the AMI to use in node provisioning. If in   |
|                                  | doubt, don't set this variable.                    |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_NODE_DEBUG              | Determines whether to preserve nodes that have     |
|                                  | failed health checks. If set to ``True``, nodes    |
|                                  | that fail EC2 health checks won't immediately be   |
|                                  | terminated so they can be examined and the cause   |
|                                  | of failure determined. If any EC2 nodes are left   |
|                                  | behind in this manner, the security group will     |
|                                  | also be left behind by necessity as it cannot be   |
|                                  | deleted until all associated nodes have been       |
|                                  | terminated.                                        |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_BATCH_QUEUE             | Name or ARN of an AWS Batch Queue to use with the  |
|                                  | AWS Batch batch system.                            |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_BATCH_JOB_ROLE_ARN      | ARN of an IAM role to run AWS Batch jobs as with   |
|                                  | the AWS Batch batch system. If the jobs are not    |
|                                  | run with an IAM role or on machines that have      |
|                                  | access to S3 and SimpleDB, the AWS job store will  |
|                                  | not be usable.                                     |
+----------------------------------+----------------------------------------------------+
| TOIL_GOOGLE_PROJECTID            | The Google project ID to use when generating       |
|                                  | Google job store names for tests or CWL workflows. |
+----------------------------------+----------------------------------------------------+
| TOIL_SLURM_ALLOCATE_MEM          | Whether to allocate memory in Slurm with --mem.    |
|                                  | True by default.                                   |
+----------------------------------+----------------------------------------------------+
| TOIL_SLURM_ARGS                  | Arguments for sbatch for the slurm batch system.   |
|                                  | Do not pass CPU or memory specifications here.     |
|                                  | Instead, define resource requirements for the job. |
|                                  | There is no default value for this variable.       |
|                                  | If neither ``--export`` nor ``--export-file`` is   |
|                                  | in the argument list, ``--export=ALL`` will be     |
|                                  | provided.                                          |
+----------------------------------+----------------------------------------------------+
| TOIL_SLURM_PE                    | Name of the slurm partition to use for parallel    |
|                                  | jobs. Useful for Slurm clusters that do not offer  |
|                                  | a partition accepting both single-core and         |
|                                  | multi-core jobs.                                   |
|                                  | There is no default value for this variable.       |
+----------------------------------+----------------------------------------------------+
| TOIL_SLURM_TIME                  | Slurm job time limit, in [DD-]HH:MM:SS format. For |
|                                  | example, ``2-07:15:30`` for 2 days, 7 hours, 15    |
|                                  | minutes and 30 seconds, or ``4:00:00`` for 4 hours.|
+----------------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_ARGS             | Arguments for qsub for the gridengine batch        |
|                                  | system. Do not pass CPU or memory specifications   |
|                                  | here. Instead, define resource requirements for    |
|                                  | the job. There is no default value for this        |
|                                  | variable.                                          |
+----------------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_PE               | Parallel environment arguments for qsub and for    |
|                                  | the gridengine batch system. There is no default   |
|                                  | value for this variable.                           |
+----------------------------------+----------------------------------------------------+
| TOIL_TORQUE_ARGS                 | Arguments for qsub for the Torque batch system.    |
|                                  | Do not pass CPU or memory specifications here.     |
|                                  | Instead, define extra parameters for the job such  |
|                                  | as queue. Example: -q medium                       |
|                                  | Use TOIL_TORQUE_REQS to pass extra values for the  |
|                                  | -l resource requirements parameter.                |
|                                  | There is no default value for this variable.       |
+----------------------------------+----------------------------------------------------+
| TOIL_TORQUE_REQS                 | Arguments for the resource requirements for Torque |
|                                  | batch system. Do not pass CPU or memory            |
|                                  | specifications here. Instead, define extra resource|
|                                  | requirements as a string that goes after the -l    |
|                                  | argument to qsub. Example:                         |
|                                  | walltime=2:00:00,file=50gb                         |
|                                  | There is no default value for this variable.       |
+----------------------------------+----------------------------------------------------+
| TOIL_LSF_ARGS                    | Additional arguments for the LSF's bsub command.   |
|                                  | Instead, define extra parameters for the job such  |
|                                  | as queue. Example: -q medium.                      |
|                                  | There is no default value for this variable.       |
+----------------------------------+----------------------------------------------------+
| TOIL_HTCONDOR_PARAMS             | Additional parameters to include in the HTCondor   |
|                                  | submit file passed to condor_submit. Do not pass   |
|                                  | CPU or memory specifications here. Instead define  |
|                                  | extra parameters which may be required by HTCondor.|
|                                  | This variable is parsed as a semicolon-separated   |
|                                  | string of ``parameter = value`` pairs. Example:    |
|                                  | ``requirements = TARGET.has_sse4_2 == true;        |
|                                  | accounting_group = test``.                         |
|                                  | There is no default value for this variable.       |
+----------------------------------+----------------------------------------------------+
| TOIL_CUSTOM_DOCKER_INIT_COMMAND  | Any custom bash command to run in the Toil docker  |
|                                  | container prior to running the Toil services.      |
|                                  | Can be used for any custom initialization in the   |
|                                  | worker and/or primary nodes such as private docker |
|                                  | docker authentication. Example for AWS ECR:        |
|                                  | ``pip install awscli && eval $(aws ecr get-login   |
|                                  | --no-include-email --region us-east-1)``.          |
+----------------------------------+----------------------------------------------------+
| TOIL_CUSTOM_INIT_COMMAND         | Any custom bash command to run prior to starting   |
|                                  | the Toil appliance. Can be used for any custom     |
|                                  | initialization in the worker and/or primary nodes  |
|                                  | such as private docker authentication for the Toil |
|                                  | appliance itself (i.e. from TOIL_APPLIANCE_SELF).  |
+----------------------------------+----------------------------------------------------+
| TOIL_S3_HOST                     | the IP address or hostname to use for connecting   |
|                                  | to S3. Example: ``TOIL_S3_HOST=127.0.0.1``         |
+----------------------------------+----------------------------------------------------+
| TOIL_S3_PORT                     | a port number to use for connecting to S3.         |
|                                  | Example: ``TOIL_S3_PORT=9001``                     |
+----------------------------------+----------------------------------------------------+
| TOIL_S3_USE_SSL                  | enable or disable the usage of SSL for connecting  |
|                                  | to S3 (``True`` by default).                       |
|                                  | Example: ``TOIL_S3_USE_SSL=False``                 |
+----------------------------------+----------------------------------------------------+
| TOIL_WES_BROKER_URL              | An optional broker URL to use to communicate       |
|                                  | between the WES server and Celery task queue. If   |
|                                  | unset, ``amqp://guest:guest@localhost:5672//`` is  |
|                                  | used.                                              |
+----------------------------------+----------------------------------------------------+
| TOIL_WES_JOB_STORE_TYPE          | Type of job store to use by default for workflows  |
|                                  | run via the WES server. Can be ``file``, ``aws``,  |
|                                  | or ``google``.                                     |
+----------------------------------+----------------------------------------------------+
| TOIL_OWNER_TAG                   | This will tag cloud resources with a tag reading:  |
|                                  | "Owner: $TOIL_OWNER_TAG". This is used internally  |
|                                  | at UCSC to stop a bot we have that terminates      |
|                                  | untagged resources.                                |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_PROFILE                 | The name of an AWS profile to run TOIL with.       |
+----------------------------------+----------------------------------------------------+
| TOIL_AWS_TAGS                    | This will tag cloud resources with any arbitrary   |
|                                  | tags given in a JSON format. These are overwritten |
|                                  | in favor of CLI options when using launch cluster. |
|                                  | For information on valid AWS tags, see `AWS Tags`_.|
+----------------------------------+----------------------------------------------------+
| SINGULARITY_DOCKER_HUB_MIRROR    | An http or https URL for the Singularity wrapper   |
|                                  | in the Toil Docker container to use as a mirror    |
|                                  | for Docker Hub.                                    |
+----------------------------------+----------------------------------------------------+
| OMP_NUM_THREADS                  | The number of cores set for OpenMP applications in |
|                                  | the workers. If not set, Toil will use the number  |
|                                  | of job threads.                                    |
+----------------------------------+----------------------------------------------------+
| GUNICORN_CMD_ARGS                | Specify additional Gunicorn configurations for the |
|                                  | Toil WES server. See `Gunicorn settings`_.         |
+----------------------------------+----------------------------------------------------+

.. _standard temporary directory: https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir
.. _Gunicorn settings: https://docs.gunicorn.org/en/stable/settings.html#settings
.. _AWS Tags: https://docs.aws.amazon.com/general/latest/gr/aws_tagging.html
