# This file is sourced by Jenkins during a CI build for both PRs and master/release branches.
# A PR may *temporarily* modify this file but a PR will only be merged if this file is identical
# between the PR branch and the target branch. The make_targets variable will contain a space-
# separated list of Makefile targets to invoke.

# Passing --system-site-packages ensures that mesos.native and mesos.interface are included
virtualenv --system-site-packages venv
. venv/bin/activate
pip2.7 install sphinx
make develop extras=[aws,mesos,azure,encryption,cwl]
export LIBPROCESS_IP=127.0.0.1
export PYTEST_ADDOPTS="--junitxml=test-report.xml"
make $make_targets
