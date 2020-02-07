# Toil Environment Variables for Running Tests
#
# Configures how toil runs tests
#
# Source this file in your bash shell using "source environment-dev.sh".

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done

set a-

# the directory this file is in
TOIL_HOME="$(cd -P "$(dirname "$SOURCE")" && pwd)"

###############
### TESTING ###
###############

# If ``True``, this allows the integration tests to run. Only valid when running the tests from the source
# directory via ``make test`` or ``make test_parallel``.
TOIL_TEST_INTEGRATIVE=False

# If ``True``, long running tests are skipped.
TOIL_TEST_QUICK=False

# Skip docker dependent tests
TOIL_SKIP_DOCKER=False

# Run tests for travis (usually shorter unit tests)
TRAVIS=True

# SSH key to use for tests in AWS.
TOIL_AWS_KEYNAME=id_rsa

# SSH key to use for tests in google.
TOIL_GOOGLE_KEYNAME=id_rsa

# Required for running Mesos master and slave daemons as part of the tests
# http://mesos.apache.org/documentation/latest/configuration/libprocess/
LIBPROCESS_IP=127.0.0.1

# An absolute path to a directory where Toil tests will write their temporary files.
# Defaults to the system's standard temporary directory.
TOIL_TEST_TEMP=''  # unset unless filled in

# Only used for google scale testing; should likely be removed?
TOIL_BOTO_DIR=''

set a+

source $TOIL_HOME/environment.sh
