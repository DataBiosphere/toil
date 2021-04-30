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
| TOIL_KUBERNETES_HOST_PATH        | A path on Kubernetes hosts that will be mounted as |
|                                  | /tmp in the workers, to allow for shared caching.  |
+----------------------------------+----------------------------------------------------+
| TOIL_KUBERNETES_OWNER            | A name prefix for easy identification of           |
|                                  | Kubernetes jobs. If not set, Toil will use the     |
|                                  | current user name.                                 |
+----------------------------------+----------------------------------------------------+
| KUBE_WATCH_ENABLED               | A boolean variable that allows for users           |
|                                  | to utilize kubernetes watch stream feature         |
|                                  | intead of polling for running jobs. Default        |
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
| TOIL_AWS_ZONE                    | The EC2 zone to provision nodes in if using        |
|                                  | Toil's provisioner.                                |
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
| TOIL_GOOGLE_PROJECTID            | The Google project ID to use when generating       |
|                                  | Google job store names for tests or CWL workflows. |
+----------------------------------+----------------------------------------------------+
| TOIL_SLURM_ARGS                  | Arguments for sbatch for the slurm batch system.   |
|                                  | Do not pass CPU or memory specifications here.     |
|                                  | Instead, define resource requirements for the job. |
|                                  | There is no default value for this variable.       |
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
| TOIL_OWNER_TAG                   | This will tag cloud resources with a tag reading:  |
|                                  | "Owner: $TOIL_OWNER_TAG".  Currently only on AWS   |
|                                  | buckets, this is an internal UCSC flag to stop a   |
|                                  | bot we have that terminates untagged resources.    |
+----------------------------------+----------------------------------------------------+
| SINGULARITY_DOCKER_HUB_MIRROR    | An http or https URL for the Singularity wrapper   |
|                                  | in the Toil Docker container to use as a mirror    |
|                                  | for Docker Hub.                                    |
+----------------------------------+----------------------------------------------------+

.. _standard temporary directory: https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir
