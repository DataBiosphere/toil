# Toil Environment Variables for Running Tests
#
# Configures how toil runs tests.  For more detailed descriptions, see:
#     https://toil.readthedocs.io/en/latest/appendices/environment_vars.html
#
# Source this file in your bash shell using "source environment-dev.sh".

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done

set a-

TOIL_HOME="$(cd -P "$(dirname "$SOURCE")" && pwd)"  # the directory this file is in

###############
### TESTING ###
###############

TOIL_TEST_INTEGRATIVE=False  # If ``True``, this allows the integration tests to run.
TOIL_TEST_QUICK=False  # If ``True``, long running tests are skipped.
TOIL_SKIP_DOCKER=False  # Skip docker dependent tests
TRAVIS=True  # Run tests for travis (shorter unit tests)

TOIL_AWS_KEYNAME=id_rsa  # SSH key to use for tests in AWS.
TOIL_GOOGLE_KEYNAME=id_rsa  # SSH key to use for tests in google.
# TOIL_GOOGLE_PROJECTID=''  # Project ID required to to run the google cloud tests.  TODO: Add this.

# Required for running Mesos master and slave daemons as part of the tests
# http://mesos.apache.org/documentation/latest/configuration/libprocess/
LIBPROCESS_IP=127.0.0.1

# TOIL_TEST_TEMP=''  # Where Toil tests will write their temporary files.  Defaults to the system's temp directory.
# TOIL_BOTO_DIR=''  # Only used for google scale testing; should likely be removed?

set a+

source $TOIL_HOME/environment.sh
