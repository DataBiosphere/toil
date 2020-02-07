# Toil Environment Variables
#
# Configure how toil runs in different environments.  For more detailed descriptions, see:
#     https://toil.readthedocs.io/en/latest/appendices/environment_vars.html
#
# Source this file in your bash shell using "source environment.sh".

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done

set -a

TOIL_HOME="$(cd -P "$(dirname "$SOURCE")" && pwd)"  # the directory this file is in

############
### MAIN ###
############

# this specifies the toil docker "appliance"
# which is the docker image used for all toil autoscaling runs
# https://toil.readthedocs.io/en/latest/running/cloud/amazon.html#toil-provisioner
# TODO: Add and link a better readthedocs for the toil appliance
TOIL_DOCKER_REGISTRY=quay.io/ucsc_cgl
TOIL_DOCKER_NAME=toil
TOIL_DOCKER_TAG=$(python $TOIL_HOME/version_template.py dockerTag)
TOIL_APPLIANCE_SELF=$TOIL_DOCKER_REGISTRY/$TOIL_DOCKER_NAME:$TOIL_DOCKER_TAG

# TOIL_WORKDIR=''  # Absolute directory path where Toil will write its temporary files.
# TOIL_CHECK_ENV=''  # Determines whether toil refers to the same virtualenv paths it spawned from (across machines)

###########
### AWS ###
###########

TOIL_AWS_ZONE=us-west-2a
TOIL_AWS_NODE_DEBUG=False  # Don't shut down EC2 instances that fail so that they can be debugged

# TOIL_AWS_AMI=''  # ID of the (normally CoreOS) AMI to use in node provisioning.  Defaults to latest.

#######################
### KUBERNETES ONLY ###
#######################

# TOIL_AWS_SECRET_NAME=''  # Name of the AWS secret, if any, to mount in containers.

##############
### GOOGLE ###
##############

# GOOGLE_APPLICATION_CREDENTIALS='' https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable

###########
### HPC ###
###########

# https://toil.readthedocs.io/en/latest/developingWorkflows/toilAPIBatchsystem.html#batch-system-enivronmental-variables

# TOIL_SLURM_ARGS=''
# TOIL_GRIDENGINE_ARGS=''
# TOIL_GRIDENGINE_PE=''
# TOIL_TORQUE_ARGS=''
# TOIL_TORQUE_REQS=''
# TOIL_LSF_ARGS=''
# TOIL_HTCONDOR_PARAMS=''

############
### MISC ###
############

# TOIL_CUSTOM_DOCKER_INIT_COMMAND=''  # Any custom bash command run prior to initiating the toil run.

set +a
