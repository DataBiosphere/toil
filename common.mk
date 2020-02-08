# Toil Environment Variables
#
# Configure how toil runs in different environments.  For more detailed descriptions, see:
#     https://toil.readthedocs.io/en/latest/appendices/environment_vars.html
SHELL=bash
python=python

############
### MAIN ###
############

# this specifies the toil docker "appliance"
# which is the docker image used for all toil autoscaling runs
# https://toil.readthedocs.io/en/latest/running/cloud/amazon.html#toil-provisioner
# TODO: Add and link a better readthedocs for the toil appliance
export TOIL_DOCKER_REGISTRY?=quay.io/ucsc_cgl
export TOIL_DOCKER_NAME?=toil
export TOIL_DOCKER_TAG?=$(shell $(python) version_template.py dockerTag)
export TOIL_APPLIANCE_SELF?=$(TOIL_DOCKER_REGISTRY)/$(TOIL_DOCKER_NAME):$(TOIL_DOCKER_TAG)

# TOIL_CHECK_ENV=''  # Determines whether toil refers to the same virtualenv paths it spawned from (across machines)

###########
### AWS ###
###########

export TOIL_AWS_ZONE?=us-west-2a
export TOIL_AWS_NODE_DEBUG?=False  # Don't shut down EC2 instances that fail so that they can be debugged

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

####################
### TESTING ONLY ###
####################

export TOIL_TEST_INTEGRATIVE?=False  # If ``True``, this allows the integration tests to run.
export TOIL_TEST_QUICK?=False  # If ``True``, long running tests are skipped.
export TOIL_SKIP_DOCKER?=False  # Skip docker dependent tests
export TRAVIS?=True  # Run tests for travis (shorter unit tests)

export TOIL_AWS_KEYNAME?=id_rsa  # SSH key to use for tests in AWS.
export TOIL_GOOGLE_KEYNAME?=id_rsa  # SSH key to use for tests in google.
# TOIL_GOOGLE_PROJECTID=''  # Project ID required to to run the google cloud tests.  TODO: Add this.

# Required for running Mesos master and slave daemons as part of the tests
# http://mesos.apache.org/documentation/latest/configuration/libprocess/
export LIBPROCESS_IP?=127.0.0.1

# TOIL_TEST_TEMP=''  # Where Toil tests will write their temporary files.  Defaults to the system's temp directory.
# TOIL_BOTO_DIR=''  # Only used for google scale testing; should likely be removed?
