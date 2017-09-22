"""
    Module for calling Docker. Assumes `docker` is on the PATH.
    Contains two user-facing functions: dockerCall and dockerCheckOutput
    Uses Toil's defer functionality to ensure containers are shutdown even in case of job or pipeline failure
    Example of using dockerCall in a Toil pipeline to index a FASTA file with SAMtools:
        def toil_job(job):
            work_dir = job.fileStore.getLocalTempDir()
            path = job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta')
            parameters = ['faidx', path]
            dockerCall(job, tool='quay.io/ucgc_cgl/samtools:latest', work_dir=work_dir, parameters=parameters)
"""
# from builtins import str
# from builtins import map
import logging
import os
import pipes
import subprocess
import docker
import base64
import time
from tarfile import TarFile

from docker.errors import ContainerError
from docker.errors import ImageNotFound
from docker.errors import APIError
from docker.errors import NotFound
from docker.utils.types import LogConfig
from docker.api.container import ContainerApiMixin

from bd2k.util.retry import retry
from docker import client
from pwd import getpwuid

from toil.lib import dockerPredicate
from toil.lib import FORGO
from toil.lib import STOP
from toil.lib import RM
from toil.test import timeLimit

_logger = logging.getLogger(__name__)


def dockerCheckOutput(job, tool, parameters=None, workDir=None, dockerParameters=None, deferParam=None,
                      checkOutput=True, containerName=None):
    return dockerCall(job=job, tool=tool, parameters=parameters, workDir=workDir, dockerParameters=dockerParameters,
                      deferParam=deferParam, checkOutput=True, containerName=containerName)

def dockerCall(job, tool, parameters=[], workDir=None,
               dockerParameters=None, deferParam=None, checkOutput=False, containerName=None, outfile=None,
               entrypoint=['/bin/bash', '-c'], detach=False, stderr=None, dockerAPIparams=None, data_dir='/data',
               logDriver=None, removeUponExit=False):
    """
    Throws CalledProcessorError if the Docker invocation returns a non-zero exit code
    This function blocks until the subprocess call to Docker returns
    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools:latest).
    :param list[str] parameters: Command line arguments to be passed to the tool.
           If list of lists: list[list[str]], then treat as successive commands chained with pipe.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] dockerParameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param int deferParam: What action should be taken on the container upon job completion?
           FORGO (0) will leave the container untouched.
           STOP (1) will attempt to stop the container with `docker stop` (useful for debugging).
           RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`. This is the default behavior if defer is set to None.
    """
    thisUser = os.getuid()
    thisGroup = os.getgid()

    if containerName is None:
        containerName = _getContainerName(job)
    if workDir is None:
        workDir = os.getcwd()

    # If 'parameters' is a list of lists, treat each list as a separate command and
    # chain with pipes.
    if len(parameters) > 0 and type(parameters[0]) is list:
        chain_params = [' '.join(p) for p in [map(pipes.quote, q) for q in parameters]]
        command = ' | '.join(chain_params)
        pipe_prefix = "set -eo pipefail && "
        command = [pipe_prefix + command]
        _logger.info("Calling docker with: " + repr(command))

    # If 'parameters' is a normal list, join all elements into a single string element
    # Example: ['echo','the', 'Oread'] becomes: ['echo the Oread']
    # Note that this is still a list, and the docker API prefers this as best practice:
    # http://docker-py.readthedocs.io/en/stable/containers.html
    elif len(parameters) > 0 and type(parameters) is list:
        command = [' '.join(p) for p in [map(pipes.quote, parameters)]]
        _logger.info("Calling docker with: " + repr(command))

    # If the 'parameters' lists are empty, they are respecified as None, which tells the API
    # to simply create and run the container
    else:
        entrypoint = None
        command = None

    workDir = os.path.abspath(workDir)

    # Ensure the user has passed a valid value for deferParam
    assert deferParam in (None, FORGO, STOP, RM), 'Please provide a valid value for deferParam.'

    client = docker.from_env()

    if deferParam == STOP:
        job.defer(_dockerStop, containerName, client)

    if deferParam == FORGO:
        removeUponExit = False
    elif deferParam == RM:
        removeUponExit = True
        job.defer(_dockerKill, containerName, client)
    # really shaky on this ->>
    elif removeUponExit is True:
        job.defer(_dockerKill, containerName, client)

    try:
        out = client.containers.run(image=tool,
                                    command=command,
                                    working_dir=workDir,
                                    entrypoint=entrypoint,
                                    name=containerName,
                                    detach=detach,
                                    volumes={workDir: {'bind': data_dir, 'mode': 'rw'}},
                                    auto_remove=removeUponExit,
                                    remove=removeUponExit,
                                    log_config=logDriver,
                                    user=str(thisUser) + ":" + str(thisGroup))
        return out
    # If the container exits with a non-zero exit code and detach is False.
    except ContainerError:
        _logger.error("Docker had non-zero exit.  Check your command: " + repr(command))
        raise ContainerError(container=containerName, exit_status=1, command=command, image=tool, stderr=stderr)
    except ImageNotFound:
        _logger.error("Docker image not found.")
        raise ImageNotFound("Docker image not found.")
    except APIError:
        _logger.error("The server returned an error.")
        raise APIError("The server returned an error.")
    except:
        raise ("An unexpected error occurred while attempting to call docker.")

def _dockerKill(container_name, client):
    """
    Immediately kills a container.  Equivalent to "docker kill":
    https://docs.docker.com/engine/reference/commandline/kill/

    :param container_name: Name of the container being killed.
    :param client: The docker API client object to call.
    """
    try:
        this_container = client.containers.get(container_name)
        if this_container.status == 'running':
            this_container.kill(signal=None)
            while (this_container.status == 'running'):
                this_container = client.containers.get(container_name)
    except NotFound:
        _logger.debug("Attempted to kill container, but container does not exist: ", container_name)

def _dockerStop(container_name, client):
    """
    Gracefully kills a container.  Equivalent to "docker stop":
    https://docs.docker.com/engine/reference/commandline/stop/

    :param container_name: Name of the container being stopped.
    :param client: The docker API client object to call.
    """
    try:
        this_container = client.containers.get(container_name)
        while this_container.status == 'running':
            try:
                client.containers.get(container_name).stop()
            except:
                pass
            this_container = client.containers.get(container_name)
    except NotFound:
        _logger.debug("Attempted to stop container, but container does not exist: ", container_name)

def _containerIsRunning(container_name):
    """
    Checks whether the container is running or not.

    :param container_name: Name of the container being checked.
    :returns: True if status is 'running', False if status is anything else,
    and None if the container does not exist.
    """
    client = docker.from_env()
    try:
        this_container = client.containers.get(container_name)
        if this_container.status == 'running':
            return True
        elif this_container.status == 'exited':
            return False
        elif this_container.status == 'restarting':
            return False
        elif this_container.status == 'paused':
            return False
    except NotFound:
        return None
    except APIError:
        _logger.debug("Server error attempting to call container: ", container_name)
        raise

def _getContainerName(job):
    """Create a random string including the job name, and return it."""
    return '--'.join([str(job), base64.b64encode(os.urandom(9), '-_')]).replace("'", '').replace('_', '')

def getDeprecatedCmdlineParams(cmdline_params):
    '''Not sure whether to include this yet.'''
    pass
