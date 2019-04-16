#!/usr/bin/env bash
# Runs custom initialization before running the main docker service.
# The first argument is a single string containing the custom init command.
# The second argument is the docker service binary (e.g. mesos-slave).
# The rest of the arguments are passed into the service.
# All arguments are required!
#
# Example usage:
# $ customDockerInit.sh 'echo "hello world"' mesos-slave --log_dir=/var/lib/mesos ...

CUSTOM_INIT_COMMAND="${1}"
SERVICE_COMMAND="${2}"
SERVICE_COMMAND_ARGS="${@:3}"

bash -c "${CUSTOM_INIT_COMMAND}"
eval "${SERVICE_COMMAND}" "${SERVICE_COMMAND_ARGS}"
