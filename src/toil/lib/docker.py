from __future__ import absolute_import
import logging
import os
import pipes
import subprocess
import docker
import base64
import time
import requests

from docker.errors import create_api_error_from_http_exception
from docker.errors import ContainerError
from docker.errors import ImageNotFound
from docker.errors import APIError
from docker.errors import NotFound
from docker.errors import DockerException
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


def dockerCheckOutput(job, *args, **kwargs):
    """
    Runs dockerCall().

    Provided for backwards compatibility with a previous implementation that
    used 'subprocess.check_call()' and 'subprocess.check_output()'.  This has
    since been supplanted and all dockerCall() runs now return the same output
    so there is no need for a separate dockerCheckOutput() function that calls
    'subprocess.check_output()'.

    See dockerCall().
    """
    return dockerCall(job=job, *args, **kwargs)

def dockerCall(job,
               tool,
               parameters=None,
               workDir=None,
               dockerParameters=None,
               deferParam=None,
               containerName=None,
               entrypoint=['/bin/bash', '-c'],
               detach=False,
               stderr=None,
               dataDir='/data',
               logDriver=None,
               removeOnExit=False,
               userPrivilege=None,
               environment=None,
               **kwargs):
    """
    A toil wrapper for the python docker API.

    Docker API Docs: https://docker-py.readthedocs.io/en/stable/index.html
    Docker API Code: https://github.com/docker/docker-py

    This implements docker's python API within toil so that calls are run as
    jobs, with the intention that failed/orphaned docker jobs be handled
    appropriately.

    Example of using dockerCall in toil to index a FASTA file with SAMtools:

    def toil_job(job):
        workDir = job.fileStore.getLocalTempDir()
        path = job.fileStore.readGlobalFile(ref_id,
                                            os.path.join(work_dir, 'ref.fasta')
        parameters = ['faidx', path]
        dockerCall(job,
                   tool='quay.io/ucgc_cgl/samtools:latest',
                   workDir=workDir,
                   parameters=parameters)

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used.
                     (e.g. 'quay.io/ucsc_cgl/samtools:latest')
    :param list[str] parameters: A list of string elements.  If there are
                                 multiple elements, these will be joined with
                                 spaces.  This handling of multiple elements
                                 provides backwards compatibility with previous
                                 versions which called docker using
                                 subprocess.check_call().
                                 **If list of lists: list[list[str]], then treat
                                 as successive commands chained with pipe.
    :param str workDir: The working directory where output files are deposited.
    :param list[str] dockerParameters: Deprecated.  These parameters are bash
                                       options supplied through check_call such
                                       as '--rm' and '-d', and are no longer
                                       used.  This is currently handled for a
                                       limited number of options with a function
                                       that converts to the docker API format.
    :param int deferParam: Action to take on the container upon job completion.
           FORGO (0) leaves the container untouched and running.
           STOP (1) Sends SIGTERM, then SIGKILL if necessary to the container.
           RM (2) Immediately send SIGKILL to the container. This is the default
           behavior if defer is set to None.
    :param str containerName: The name of the container.
    :param str entrypoint: Prepends commands sent to the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool detach: Run the container in detached mode. (equivalent to '-d')
    :param bool stderr: Return stderr after running the container or not.
    :param str dataDir: The path in the container where the data is held.
    :param str logDriver: Specify the logs to return from the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool removeOnExit: Remove the container on exit or not.
    :param str userPrivilege: The container will be run with the privileges of
                              the user specified.  Can be an actual name, such
                              as 'root' or 'lifeisaboutfishtacos', or it can be
                              the uid or gid of the user ('0' is root; '1000' is
                              an example of a less privileged uid or gid), or a
                              complement of the uid:gid (RECOMMENDED), such as
                              '0:0' (root user : root group) or '1000:1000'
                              (some other user : some other user group).
    :param environment: Allows one to set environment variables inside of the
                        container, such as:
    :param kwargs: Additional keyword arguments supplied to the docker API's
                   run command.  The list is 75 keywords total, for examples
                   and full documentation see:

                   https://docker-py.readthedocs.io/en/stable/containers.html

                   * NOTE: A number of the above are redundant with kwargs,
                           mostly due to wanting to supply nonstandard defaults
                           for toil's dockerCall method in order to maintain
                           backwards compatibility with existing toil workflows.
                           Try to use the params provided before using kwargs.
                           For example, don't use 'tool=XXX' AND 'image=XXX', or
                           the image used will be submitted twice.
    """
    dockerApiKeywords = [containerName,
                         workDir,
                         dataDir,
                         removeOnExit,
                         detach,
                         environment,
                         logDriver]
    # dockerParameters is deprecated, but if used will use some of the
    # commandline inputs to define variables.  This is limited to a small subset
    # of supported commandline inputs only intended for backwards compatibility.
    if dockerParameters:
        apiParameters = getDeprecatedCmdlineParams(dockerParameters)
        for k, v in apiParameters:
            for keyword in dockerApiKeywords:
                if k == str(keyword):
                    keyword = v

    # make certain that files have the correct permissions
    thisUser = os.getuid()
    thisGroup = os.getgid()
    if userPrivilege is None:
        userPrivilege = str(thisUser) + ":" + str(thisGroup)

    if containerName is None:
        containerName = _getContainerName(job)

    if workDir is None:
        workDir = os.getcwd()

    if parameters is None:
        parameters = []

    # If 'parameters' is a list of lists, treat each list as a separate command
    # and chain with pipes.
    if len(parameters) > 0 and type(parameters[0]) is list:
        chain_params = \
            [' '.join((pipes.quote(arg) for arg in command)) \
             for command in parameters]
        command = ' | '.join(chain_params)
        pipe_prefix = "set -eo pipefail && "
        command = [pipe_prefix + command]
        _logger.debug("Calling docker with: " + repr(command))

    # If 'parameters' is a normal list, join all elements into a single string
    # element.
    # Example: ['echo','the', 'Oread'] becomes: ['echo the Oread']
    # Note that this is still a list, and the docker API prefers this as best
    # practice:
    # http://docker-py.readthedocs.io/en/stable/containers.html
    elif len(parameters) > 0 and type(parameters) is list:
        command = [' '.join(p) for p in parameters]
        _logger.debug("Calling docker with: " + repr(command))

    # If the 'parameters' lists are empty, they are respecified as None, which
    # tells the API to simply create and run the container
    else:
        entrypoint = None
        command = None

    workDir = os.path.abspath(workDir)

    # Ensure the user has passed a valid value for deferParam
    assert deferParam in (None, FORGO, STOP, RM), \
        'Please provide a valid value for deferParam.'

    client = docker.from_env(version='auto')

    if deferParam == STOP:
        job.defer(_dockerStop, containerName, client)

    if deferParam == FORGO:
        removeOnExit = False
    elif deferParam == RM:
        removeOnExit = True
        job.defer(_dockerKill, containerName, client)
    elif removeOnExit is True:
        job.defer(_dockerKill, containerName, client)

    try:
        out = client.containers.run(image=tool,
                                    command=command,
                                    working_dir=workDir,
                                    entrypoint=entrypoint,
                                    name=containerName,
                                    detach=detach,
                                    volumes=
                                     {workDir: {'bind': dataDir, 'mode': 'rw'}},
                                    auto_remove=removeOnExit,
                                    remove=removeOnExit,
                                    log_config=logDriver,
                                    user=userPrivilege,
                                    environment=environment,
                                    **kwargs)
        return out
    # If the container exits with a non-zero exit code and detach is False.
    except ContainerError:
        _logger.error("Docker had non-zero exit.  Check your command: " + \
                      repr(command))
        raise
    except ImageNotFound:
        _logger.error("Docker image not found.")
        raise
    except requests.exceptions.HTTPError as e:
        _logger.error("The server returned an error.")
        raise create_api_error_from_http_exception(e)
    except:
        raise

def _dockerKill(container_name, client, gentleKill=False):
    """
    Immediately kills a container.  Equivalent to "docker kill":
    https://docs.docker.com/engine/reference/commandline/kill/

    :param container_name: Name of the container being killed.
    :param client: The docker API client object to call.
    """
    try:
        this_container = client.containers.get(container_name)
        while this_container.status == 'running':
            if gentleKill is False:
                client.containers.get(container_name).kill()
            else:
                client.containers.get(container_name).stop()
            this_container = client.containers.get(container_name)
    except NotFound:
        _logger.debug("Attempted to stop container, but container != exist: ",
                      container_name)
    except requests.exceptions.HTTPError as e:
        _logger.debug("Attempted to stop container, but server gave an error: ",
                      container_name)
        raise create_api_error_from_http_exception(e)

def _dockerStop(container_name, client):
    """
    Gracefully kills a container.  Equivalent to "docker stop":
    https://docs.docker.com/engine/reference/commandline/stop/

    :param container_name: Name of the container being stopped.
    :param client: The docker API client object to call.
    """
    _dockerKill(container_name, client, gentleKill=True)

def _containerIsRunning(container_name):
    """
    Checks whether the container is running or not.

    :param container_name: Name of the container being checked.
    :returns: True if status is 'running', False if status is anything else,
    and None if the container does not exist.
    """
    client = docker.from_env(version='auto')
    try:
        this_container = client.containers.get(container_name)
        if this_container.status == 'running':
            return True
        else:
            # this_container.status == 'exited', 'restarting', or 'paused'
            return False
    except NotFound:
        return None
    except requests.exceptions.HTTPError as e:
        _logger.debug("Server error attempting to call container: ",
                      container_name)
        raise create_api_error_from_http_exception(e)

def _getContainerName(job):
    """Create a random string including the job name, and return it."""
    return '--'.join([str(job),
                      base64.b64encode(os.urandom(9), '-_')])\
                      .replace("'", '').replace('"', '').replace('_', '')

def getDeprecatedCmdlineParams(cmdline_params):
    """
    Supports a minimal number of key terms for backwards compatibility with the
    arguments formerly supplied to check_call as a commandline array of strings.
    This is not intended to be maintained or developed beyond those needs, as
    the dockerCall use of the python docker API keywords is strongly encouraged.

    :param cmdline_params: An array of strings intended for use by check_call.
                           e.g. ['--rm', '-d']
    :returns: A dictionary of terms compatible with the python docker API.
    """
    cmdline_dict = {}
    skip_to_name = 0
    skip_to_volume = 0
    for term in cmdline_params:
        if term == '--rm':
            cmdline_dict['removeOnExit'] = True

        if term == '-d':
            cmdline_dict['detach'] = True

        if term.beginswith('--logdriver'):
            log_config = term.split('=')[-1]
            cmdline_dict['log_config'] = log_config

        if skip_to_name == 1:
            skip_to_name = 0
            cmdline_dict['name'] = term

        if term == '--name':
            skip_to_name = 1

        if skip_to_volume == 1:
            skip_to_volume = 0
            workDir = term.split(':')[0]
            dataDir = term.split(':')[-1]
            cmdline_dict['workDir'] = workDir
            cmdline_dict['dataDir'] = dataDir

        if term == '-v':
            skip_to_volume = 1

        if skip_to_envar == 1:
            skip_to_envar = 0
            cmdline_dict['environment'] = [term]

        if term == '--env':
            skip_to_envar = 1

    return cmdline_dict
