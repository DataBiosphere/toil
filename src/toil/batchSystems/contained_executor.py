# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Executor for running inside a container.

Useful for Kubernetes and TES batch systems.
"""
import base64
import logging
import os
import pickle
import subprocess
import sys
from typing import Any, Dict, List, Optional

from toil.batchSystems.abstractBatchSystem import EXIT_STATUS_UNAVAILABLE_VALUE
from toil.job import JobDescription
from toil.resource import Resource
from toil.statsAndLogging import configure_root_logger, set_log_level

logger = logging.getLogger(__name__)


def pack_job(job_desc: JobDescription, user_script: Optional[Resource] = None, environment: Optional[Dict[str, str]] = None) -> List[str]:
    """
    Create a command that, when run, will execute the given job.

    :param job_desc: Job description for the job to run.
    :param user_script: User script that will be loaded before the job is run.
    :param environment: Environment variable dict that will be applied before
    the job is run.

    :returns: Command to run the job, as an argument list that can be run
    inside the Toil appliance container.
    """
    # Make a job dict to send to the executor.
    # TODO: Factor out executor setup from here and Kubernetes and TES
    job: Dict[str, Any] = {"command": job_desc.command}
    if user_script is not None:
        # If there's a user script resource be sure to send it along
        job['userScript'] = user_script
    if environment is not None:
        # We also may have an environment to send.
        job['environment'] = environment
    # Encode it in a form we can send in a command-line argument. Pickle in
    # the highest protocol to prevent mixed-Python-version workflows from
    # trying to work. Make sure it is text so we can ship it via JSON.
    encoded_job = base64.b64encode(pickle.dumps(job, pickle.HIGHEST_PROTOCOL)).decode('utf-8')
    # Make a command to run it in the executor
    command_list = ['_toil_contained_executor', encoded_job]

    return command_list


def executor() -> None:
    """
    Main function of the _toil_contained_executor entrypoint.

    Runs inside the Toil container.

    Responsible for setting up the user script and running the command for the
    job (which may in turn invoke the Toil worker entrypoint).

    """

    configure_root_logger()
    set_log_level("DEBUG")
    logger.debug("Starting executor")

    # If we don't manage to run the child, what should our exit code be?
    exit_code = EXIT_STATUS_UNAVAILABLE_VALUE

    if len(sys.argv) != 2:
        logger.error('Executor requires exactly one base64-encoded argument')
        sys.exit(exit_code)

    # Take in a base64-encoded pickled dict as our first argument and decode it
    try:
        # Make sure to encode the text arguments to bytes before base 64 decoding
        job = pickle.loads(base64.b64decode(sys.argv[1].encode('utf-8')))
    except:
        exc_info = sys.exc_info()
        logger.error('Exception while unpickling task: ', exc_info=exc_info)
        sys.exit(exit_code)

    if 'environment' in job:
        # Adopt the job environment into the executor.
        # This lets us use things like TOIL_WORKDIR when figuring out how to talk to other executors.
        logger.debug('Adopting environment: %s', str(job['environment'].keys()))
        for var, value in job['environment'].items():
            os.environ[var] = value

    # Set JTRES_ROOT and other global state needed for resource
    # downloading/deployment to work.
    # TODO: Every worker downloads resources independently.
    # We should have a way to share a resource directory.
    logger.debug('Preparing system for resource download')
    Resource.prepareSystem()
    try:
        if 'userScript' in job:
            job['userScript'].register()

        # Start the child process
        logger.debug("Invoking command: '%s'", job['command'])
        child = subprocess.Popen(job['command'],
                                 preexec_fn=lambda: os.setpgrp(),
                                 shell=True)

        # Reproduce child's exit code
        exit_code = child.wait()
    except:
        # This will print a traceback for us, since exit() in the finally
        # will bypass the normal way of getting one.
        logger.exception('Encountered exception running child')
    finally:
        logger.debug('Cleaning up resources')
        # TODO: Change resource system to use a shared resource directory for everyone.
        # Then move this into worker cleanup somehow
        Resource.cleanSystem()
        logger.debug('Shutting down')
        sys.exit(exit_code)


