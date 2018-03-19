.. _envars:

Environment Variables
=====================
There are several environment variables that affect the way Toil runs.

+------------------------+----------------------------------------------------+
| TOIL_WORKDIR           | An absolute path to a directory where Toil will    |
|                        | write its temporary files. This directory must     |
|                        | exist on each worker node and may be set to a      |
|                        | different value on each worker. The ``--workDir``  |
|                        | command line option overrides this. On Mesos nodes,|
|                        | ``TOIL_WORKDIR`` generally defaults to the Mesos   |
|                        | sandbox, except on CGCloud-provisioned nodes where |
|                        | it defaults to ``/var/lib/mesos``. In all other    |
|                        | cases, the system's `standard temporary directory`_|
|                        | is used.                                           |
+------------------------+----------------------------------------------------+
| TOIL_APPLIANCE_SELF    | The fully qualified reference for the Toil         |
|                        | Appliance you wish to use, in the form             |
|                        | ``REPO/IMAGE:TAG``.                                |
|                        | ``quay.io/ucsc_cgl/toil:3.6.0`` and                |
|                        | ``cket/toil:3.5.0`` are both examples of valid     |
|                        | options. Note that since Docker defaults to        |
|                        | Dockerhub repos, only quay.io repos need to        |
|                        | specify their registry.                            |
+------------------------+----------------------------------------------------+
| TOIL_DOCKER_REGISTRY   | The URL of the registry of the Toil Appliance      |
|                        | image you wish to use. Docker will use Dockerhub   |
|                        | by default, but the quay.io registry is also       |
|                        | very popular and easily specifiable by settting    |
|                        | this option to ``quay.io``.                        |
+------------------------+----------------------------------------------------+
| TOIL_DOCKER_NAME       | The name of the Toil Appliance image you           |
|                        | wish to use. Generally this is simply ``toil`` but |
|                        | this option is provided to override this,          |
|                        | since the image can be built with arbitrary names. |
+------------------------+----------------------------------------------------+
| TOIL_AWS_ZONE          | The EC2 zone to provision nodes in if using        |
|                        | Toil's provisioner.                                |
+------------------------+----------------------------------------------------+
| TOIL_AWS_AMI           | ID of the AMI to use in node provisioning. If in   |
|                        | doubt, don't set this variable.                    |
+------------------------+----------------------------------------------------+
| TOIL_AWS_NODE_DEBUG    | Determines whether to preserve nodes that have     |
|                        | failed health checks. If set to ``True``, nodes    |
|                        | that fail EC2 health checks won't immediately be   |
|                        | terminated so they can be examined and the cause   |
|                        | of failure determined. If any EC2 nodes are left   |
|                        | behind in this manner, the security group will     |
|                        | also be left behind by necessity as it cannot be   |
|                        | deleted until all associated nodes have been       |
|                        | terminated.                                        |
+------------------------+----------------------------------------------------+
| TOIL_SLURM_ARGS        | Arguments for sbatch for the slurm batch system.   |
|                        | Do not pass CPU or memory specifications here.     |
|                        | Instead, define resource requirements for the job. |
|                        | There is no default value for this variable.       |
+------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_ARGS   | Arguments for qsub for the gridengine batch        |
|                        | system. Do not pass CPU or memory specifications   |
|                        | here. Instead, define resource requirements for    |
|                        | the job. There is no default value for this        |
|                        | variable.                                          |
+------------------------+----------------------------------------------------+
| TOIL_GRIDENGINE_PE     | Parallel environment arguments for qsub and for    |
|                        | the gridengine batch system. There is no default   |
|                        | value for this variable.                           |
+------------------------+----------------------------------------------------+
| TOIL_TORQUE_ARGS       | Arguments for qsub for the Torque batch system.    |
|                        | Do not pass CPU or memory specifications here.     |
|                        | Instead, define extra parameters for the job such  |
|                        | as queue. Example: -q medium                       |
|                        | Use TOIL_TORQUE_REQS to pass extra values for the  |
|                        | -l resource requirements parameter.                |
|                        | There is no default value for this variable.       |
+------------------------+----------------------------------------------------+
| TOIL_TORQUE_REQS       | Arguments for the resource requirements for Torque |
|                        | batch system. Do not pass CPU or memory            |
|                        | specifications here. Instead, define extra resource| 
|                        | requirements as a string that goes after the -l    |
|                        | argument to qsub. Example:                         |
|                        | walltime=2:00:00,file=50gb                         |
|                        | There is no default value for this variable.       |
+------------------------+----------------------------------------------------+
| TOIL_LSF_ARGS          | Additional arguments for the LSF's bsub command.   |
|                        | Instead, define extra parameters for the job such  |
|                        | as queue. Example: -q medium                       |
|                        | There is no default value for this variable.       |
+------------------------+----------------------------------------------------+

.. _standard temporary directory: https://docs.python.org/2/library/tempfile.html#tempfile.gettempdir
