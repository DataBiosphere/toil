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
import base64
import logging
import os
import re
import struct
from shlex import quote

import requests

import docker
from docker.errors import (ContainerError,
                           ImageNotFound,
                           NotFound,
                           create_api_error_from_http_exception)
from docker.utils.socket import consume_socket_output, demux_adaptor

logger = logging.getLogger(__name__)

FORGO = 0
STOP = 1
RM = 2


def dockerCheckOutput(*args, **kwargs):
    raise RuntimeError("dockerCheckOutput() using subprocess.check_output() has been removed, "
                       "please switch to apiDockerCall().")


def dockerCall(*args, **kwargs):
    raise RuntimeError("dockerCall() using subprocess.check_output() has been removed, "
                       "please switch to apiDockerCall().")


def subprocessDockerCall(*args, **kwargs):
    raise RuntimeError("subprocessDockerCall() has been removed, "
                       "please switch to apiDockerCall().")


def apiDockerCall(job,
                  image,
                  parameters=None,
                  deferParam=None,
                  volumes=None,
                  working_dir=None,
                  containerName=None,
                  entrypoint=None,
                  detach=False,
                  log_config=None,
                  auto_remove=None,
                  remove=False,
                  user=None,
                  environment=None,
                  stdout=None,
                  stderr=False,
                  stream=False,
                  demux=False,
                  streamfile=None,
                  timeout=365 * 24 * 60 * 60,
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
        working_dir = job.fileStore.getLocalTempDir()
        path = job.fileStore.readGlobalFile(ref_id,
                                          os.path.join(working_dir, 'ref.fasta')
        parameters = ['faidx', path]
        apiDockerCall(job,
                      image='quay.io/ucgc_cgl/samtools:latest',
                      working_dir=working_dir,
                      parameters=parameters)

    Note that when run with detach=False, or with detach=True and stdout=True
    or stderr=True, this is a blocking call. When run with detach=True and
    without output capture, the container is started and returned without
    waiting for it to finish.

    :param toil.Job.job job: The Job instance for the calling function.
    :param str image: Name of the Docker image to be used.
                     (e.g. 'quay.io/ucsc_cgl/samtools:latest')
    :param list[str] parameters: A list of string elements.  If there are
                                 multiple elements, these will be joined with
                                 spaces.  This handling of multiple elements
                                 provides backwards compatibility with previous
                                 versions which called docker using
                                 subprocess.check_call().
                                 **If list of lists: list[list[str]], then treat
                                 as successive commands chained with pipe.
    :param str working_dir: The working directory.
    :param int deferParam: Action to take on the container upon job completion.
           FORGO (0) leaves the container untouched and running.
           STOP (1) Sends SIGTERM, then SIGKILL if necessary to the container.
           RM (2) Immediately send SIGKILL to the container. This is the default
           behavior if deferParam is set to None.
    :param str name: The name/ID of the container.
    :param str entrypoint: Prepends commands sent to the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool detach: Run the container in detached mode. (equivalent to '-d')
    :param bool stdout: Return logs from STDOUT when detach=False (default: True).
                        Block and capture stdout to a file when detach=True
                        (default: False). Output capture defaults to output.log,
                        and can be specified with the "streamfile" kwarg.
    :param bool stderr: Return logs from STDERR when detach=False (default: False).
                        Block and capture stderr to a file when detach=True
                        (default: False). Output capture defaults to output.log,
                        and can be specified with the "streamfile" kwarg.
    :param bool stream: If True and detach=False, return a log generator instead
                        of a string. Ignored if detach=True. (default: False).
    :param bool demux: Similar to `demux` in container.exec_run(). If True and
                       detach=False, returns a tuple of (stdout, stderr). If
                       stream=True, returns a log generator with tuples of
                       (stdout, stderr). Ignored if detach=True. (default: False).
    :param str streamfile: Collect container output to this file if detach=True and
                        stderr and/or stdout are True. Defaults to "output.log".
    :param dict log_config: Specify the logs to return from the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool remove: Remove the container on exit or not.
    :param str user: The container will be run with the privileges of
                     the user specified.  Can be an actual name, such
                     as 'root' or 'lifeisaboutfishtacos', or it can be
                     the uid or gid of the user ('0' is root; '1000' is
                     an example of a less privileged uid or gid), or a
                     complement of the uid:gid (RECOMMENDED), such as
                     '0:0' (root user : root group) or '1000:1000'
                     (some other user : some other user group).
    :param environment: Allows one to set environment variables inside of the
                        container, such as:
    :param int timeout: Use the given timeout in seconds for interactions with
                        the Docker daemon. Note that the underlying docker module is
                        not always able to abort ongoing reads and writes in order
                        to respect the timeout. Defaults to 1 year (i.e. wait
                        essentially indefinitely).
    :param kwargs: Additional keyword arguments supplied to the docker API's
                   run command.  The list is 75 keywords total, for examples
                   and full documentation see:
                   https://docker-py.readthedocs.io/en/stable/containers.html

    :returns: Returns the standard output/standard error text, as requested, when
              detach=False. Returns the underlying
              docker.models.containers.Container object from the Docker API when
              detach=True.
    """

    # make certain that files have the correct permissions
    thisUser = os.getuid()
    thisGroup = os.getgid()
    if user is None:
        user = str(thisUser) + ":" + str(thisGroup)

    if containerName is None:
        containerName = getContainerName(job)

    if working_dir is None:
        working_dir = os.getcwd()

    if volumes is None:
        volumes = {working_dir: {'bind': '/data', 'mode': 'rw'}}

    for volume in volumes:
        if not os.path.exists(volume):
            os.makedirs(volume, exist_ok=True)

    if parameters is None:
        parameters = []

    # If 'parameters' is a list of lists, treat each list as a separate command
    # and chain with pipes.
    if len(parameters) > 0 and type(parameters[0]) is list:
        if entrypoint is None:
            entrypoint = ['/bin/bash', '-c']
        chain_params = \
            [' '.join((quote(arg) for arg in command)) \
             for command in parameters]
        command = ' | '.join(chain_params)
        pipe_prefix = "set -eo pipefail && "
        command = [pipe_prefix + command]
        logger.debug("Calling docker with: " + repr(command))

    # If 'parameters' is a normal list, join all elements into a single string
    # element, quoting and escaping each element.
    # Example: ['echo','the Oread'] becomes: ["echo 'the Oread'"]
    # Note that this is still a list, and the docker API prefers this as best
    # practice:
    # http://docker-py.readthedocs.io/en/stable/containers.html
    elif len(parameters) > 0 and type(parameters) is list:
        command = ' '.join((quote(arg) for arg in parameters))
        logger.debug("Calling docker with: " + repr(command))

    # If the 'parameters' lists are empty, they are respecified as None, which
    # tells the API to simply create and run the container
    else:
        entrypoint = None
        command = None

    working_dir = os.path.abspath(working_dir)

    # Ensure the user has passed a valid value for deferParam
    assert deferParam in (None, FORGO, STOP, RM), \
        'Please provide a valid value for deferParam.'

    client = docker.from_env(version='auto', timeout=timeout)

    if deferParam is None:
        deferParam = RM

    if deferParam == STOP:
        job.defer(dockerStop, containerName)

    if deferParam == FORGO:
        # Leave the container untouched and running
        pass
    elif deferParam == RM:
        job.defer(dockerKill, containerName, remove=True)
    elif remove:
        job.defer(dockerKill, containerName, remove=True)

    if auto_remove is None:
        auto_remove = remove

    try:
        if detach is False:
            # When detach is False, this returns stdout normally:
            # >>> client.containers.run("ubuntu:latest", "echo hello world")
            # 'hello world\n'
            if stdout is None:
                stdout = True
            out = client.containers.run(image=image,
                                        command=command,
                                        working_dir=working_dir,
                                        entrypoint=entrypoint,
                                        name=containerName,
                                        detach=False,
                                        volumes=volumes,
                                        auto_remove=auto_remove,
                                        stdout=stdout,
                                        stderr=stderr,
                                        # to get the generator if demux=True
                                        stream=stream or demux,
                                        remove=remove,
                                        log_config=log_config,
                                        user=user,
                                        environment=environment,
                                        **kwargs)

            if demux is False:
                return out

            # If demux is True (i.e.: we want STDOUT and STDERR separated), we need to decode
            # the raw response from the docker API and preserve the stream type this time.
            response = out._response
            gen = (demux_adaptor(*frame) for frame in _multiplexed_response_stream_helper(response))

            if stream:
                return gen
            else:
                return consume_socket_output(frames=gen, demux=True)

        else:
            if (stdout or stderr) and log_config is None:
                logger.warning('stdout or stderr specified, but log_config is not set.  '
                               'Defaulting to "journald".')
                log_config = dict(type='journald')

            if stdout is None:
                stdout = False

            # When detach is True, this returns a container object:
            # >>> client.containers.run("bfirsh/reticulate-splines", detach=True)
            # <Container '45e6d2de7c54'>
            container = client.containers.run(image=image,
                                              command=command,
                                              working_dir=working_dir,
                                              entrypoint=entrypoint,
                                              name=containerName,
                                              detach=True,
                                              volumes=volumes,
                                              auto_remove=auto_remove,
                                              stdout=stdout,
                                              stderr=stderr,
                                              stream=stream,
                                              remove=remove,
                                              log_config=log_config,
                                              user=user,
                                              environment=environment,
                                              **kwargs)
            if stdout or stderr:
                if streamfile is None:
                    streamfile = 'output.log'
                with open(streamfile, 'wb') as f:
                    # stream=True makes this loop blocking; we will loop until
                    # the container stops and there is no more output.
                    for line in container.logs(stdout=stdout, stderr=stderr, stream=True):
                        f.write(line.encode() if isinstance(line, str) else line)

            # If we didn't capture output, the caller will need to .wait() on
            # the container to know when it is done. Even if we did capture
            # output, the caller needs the container to get at the exit code.
            return container

    except ContainerError:
        logger.error("Docker had non-zero exit.  Check your command: " + repr(command))
        raise
    except ImageNotFound:
        logger.error("Docker image not found.")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error("The server returned an error.")
        raise create_api_error_from_http_exception(e)


def dockerKill(container_name: str,
               gentleKill: bool = False,
               remove: bool = False,
               timeout: int = 365 * 24 * 60 * 60) -> None:
    """
    Immediately kills a container.  Equivalent to "docker kill":
    https://docs.docker.com/engine/reference/commandline/kill/

    :param container_name: Name of the container being killed.
    :param gentleKill: If True, trigger a graceful shutdown.
    :param remove: If True, remove the container after it exits.
    :param int timeout: Use the given timeout in seconds for interactions with
                        the Docker daemon. Note that the underlying docker module is
                        not always able to abort ongoing reads and writes in order
                        to respect the timeout. Defaults to 1 year (i.e. wait
                        essentially indefinitely).
    """
    client = docker.from_env(version='auto', timeout=timeout)
    try:
        this_container = client.containers.get(container_name)
        while this_container.status == 'running':
            if gentleKill is False:
                client.containers.get(container_name).kill()
            else:
                client.containers.get(container_name).stop()
            this_container = client.containers.get(container_name)
        if remove:
            this_container.remove()
    except NotFound:
        logger.debug(f"Attempted to stop container ({container_name}), but container != exist.")
    except requests.exceptions.HTTPError as e:
        logger.debug(f"Attempted to stop container ({container_name}), but server gave an error:")
        raise create_api_error_from_http_exception(e)


def dockerStop(container_name: str, remove: bool = False) -> None:
    """
    Gracefully kills a container.  Equivalent to "docker stop":
    https://docs.docker.com/engine/reference/commandline/stop/

    :param container_name: Name of the container being stopped.
    :param remove: If True, remove the container after it exits.
    """
    dockerKill(container_name, gentleKill=True, remove=remove)


def containerIsRunning(container_name: str, timeout: int = 365 * 24 * 60 * 60):
    """
    Checks whether the container is running or not.

    :param container_name: Name of the container being checked.
    :param int timeout: Use the given timeout in seconds for interactions with
                        the Docker daemon. Note that the underlying docker module is
                        not always able to abort ongoing reads and writes in order
                        to respect the timeout. Defaults to 1 year (i.e. wait
                        essentially indefinitely).
    :returns: True if status is 'running', False if status is anything else,
    and None if the container does not exist.
    """
    client = docker.from_env(version='auto', timeout=timeout)
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
        logger.debug("Server error attempting to call container: ",
                     container_name)
        raise create_api_error_from_http_exception(e)


def getContainerName(job):
    """
    Create a random string including the job name, and return it. Name will
    match [a-zA-Z0-9][a-zA-Z0-9_.-]
    """
    parts = ['toil', str(job.description), base64.b64encode(os.urandom(9), b'-_').decode('utf-8')]
    name = re.sub('[^a-zA-Z0-9_.-]', '', '--'.join(parts))
    return name


def _multiplexed_response_stream_helper(response):
    """
    A generator of multiplexed data blocks coming from a response stream modified from:
    https://github.com/docker/docker-py/blob/4.3.1-release/docker/api/client.py#L370

    :param response: requests.Response
    :return: a generator with tuples of (stream_type, data)
    """
    while True:
        header = response.raw.read(8)
        if not header:
            break
        # header is 8 bytes with format: {STREAM_TYPE, 0, 0, 0, SIZE1, SIZE2, SIZE3, SIZE4}
        # protocol: https://docs.docker.com/engine/api/v1.24/#attach-to-a-container
        stream_type, length = struct.unpack('>BxxxL', header)
        if not length:
            continue
        data = response.raw.read(length)
        if not data:
            break
        yield stream_type, data
