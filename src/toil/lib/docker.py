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
import base64
import logging
import os
import pipes
import uuid

from bd2k.util.exceptions import require
from bd2k.util.retry import retry

from toil.lib import *

_logger = logging.getLogger(__name__)


def dockerCall(job,
               tool,
               parameters=None,
               workDir=None,
               dockerParameters=None,
               outfile=None,
               defer=None):
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
    :param int defer: What action should be taken on the container upon job completion?
           FORGO (0) will leave the container untouched.
           STOP (1) will attempt to stop the container with `docker stop` (useful for debugging).
           RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`. This is the default behavior if defer is set to None.
    """
    _docker(job, tool=tool, parameters=parameters, workDir=workDir, dockerParameters=dockerParameters,
            outfile=outfile, checkOutput=False, defer=defer)


def dockerCheckOutput(job,
                      tool,
                      parameters=None,
                      workDir=None,
                      dockerParameters=None,
                      defer=None):
    """
    Returns the stdout from the Docker invocation (via subprocess.check_output)
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
    :param int defer: What action should be taken on the container upon job completion?
           FORGO (0) will leave the container untouched.
           STOP (1) will attempt to stop the container with `docker stop` (useful for debugging).
           RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`. This is the default behavior if defer is set to None.
    :returns: Stdout from the docker call
    :rtype: str
    """
    return _docker(job, tool=tool, parameters=parameters, workDir=workDir,
                   dockerParameters=dockerParameters, checkOutput=True, defer=defer)


def _docker(job,
            tool,
            parameters=None,
            workDir=None,
            dockerParameters=None,
            outfile=None,
            checkOutput=False,
            defer=None):
    """
    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed to the tool.
           If list of lists: list[list[str]], then treat as successive commands chained with pipe.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] dockerParameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param bool checkOutput: When True, this function returns docker's output.
    :param int defer: What action should be taken on the container upon job completion?
           FORGO (0) will leave the container untouched.
           STOP (1) will attempt to stop the container with `docker stop` (useful for debugging).
           RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`. This is the default behavior if defer is set to None.
    """
    if parameters is None:
        parameters = []
    if workDir is None:
        workDir = os.getcwd()

    # Setup the outgoing subprocess call for docker
    baseDockerCall = ['docker', 'run']
    if dockerParameters:
        baseDockerCall += dockerParameters
    else:
        baseDockerCall += ['--rm', '--log-driver', 'none', '-v',
                           os.path.abspath(workDir) + ':/data']

    # Ensure the user has passed a valid value for defer
    require(defer in (None, FORGO, STOP, RM),
            'Please provide a valid value for defer.')

    # Get container name which is needed for _dockerKill
    try:
        if any('--name' in x for x in baseDockerCall):
            if any('--name=' in x for x in baseDockerCall):
                containerName = [x.split('=')[1] for x in baseDockerCall if '--name' in x][0]
            else:
                containerName = baseDockerCall[baseDockerCall.index('--name') + 1]
        else:
            containerName = _getContainerName(job)
            baseDockerCall.extend(['--name', containerName])
    except ValueError:
        containerName = _getContainerName(job)
        baseDockerCall.extend(['--name', containerName])
    except IndexError:
        raise RuntimeError("Couldn't parse Docker's `--name=` option, check parameters: " + str(dockerParameters))

    # Defer the container on-exit action
    if '--rm' in baseDockerCall and defer is None:
        defer = RM
    if '--rm' in baseDockerCall and defer is not RM:
        _logger.warn('--rm being passed to docker call but defer not set to dockerCall.RM, defer set to: ' + str(defer))
    job.defer(_dockerKill, containerName, action=defer)
    # Defer the permission fixing function which will run after this job concludes.
    # We call this explicitly later on in this function, but we defer it as well to handle unexpected job failure.
    job.defer(_fixPermissions, tool, workDir)

    # Make subprocess call

    # If parameters is list of lists, treat each list as separate command and chain with pipes
    if len(parameters) > 0 and type(parameters[0]) is list:
        # When piping, all arguments now get merged into a single string to bash -c.
        # We try to support spaces in paths by wrapping them all in quotes first.
        chain_params = [' '.join(p) for p in [map(pipes.quote, q) for q in parameters]]
        # Use bash's set -eo pipefail to detect and abort on a failure in any command in the chain
        call = baseDockerCall + ['--entrypoint', '/bin/bash',  tool, '-c',
                                 'set -eo pipefail && {}'.format(' | '.join(chain_params))]
    else:
        call = baseDockerCall + [tool] + parameters
    _logger.info("Calling docker with " + repr(call))

    params = {}
    if outfile:
        params['stdout'] = outfile
    if checkOutput:
        callMethod = subprocess.check_output
    else:
        callMethod = subprocess.check_call

    for attempt in retry(predicate=dockerPredicate):
        with attempt:
            out = callMethod(call, **params)

    _fixPermissions(tool=tool, workDir=workDir)
    return out


def _dockerKill(containerName, action):
    """
    Kills the specified container.
    :param str containerName: The name of the container created by docker_call
    :param int action: What action should be taken on the container?  See `defer=` in
           :func:`docker_call`
    """
    running = _containerIsRunning(containerName)
    if running is None:
        # This means that the container doesn't exist.  We will see this if the container was run
        # with --rm and has already exited before this call.
        _logger.info('The container with name "%s" appears to have already been removed.  Nothing to '
                  'do.', containerName)
    else:
        if action in (None, FORGO):
            _logger.info('The container with name %s continues to exist as we were asked to forgo a '
                      'post-job action on it.', containerName)
        else:
            _logger.info('The container with name %s exists. Running user-specified defer functions.',
                         containerName)
            if running and action >= STOP:
                _logger.info('Stopping container "%s".', containerName)
                for attempt in retry(predicate=dockerPredicate):
                    with attempt:
                        subprocess.check_call(['docker', 'stop', containerName])
            else:
                _logger.info('The container "%s" was not found to be running.', containerName)
            if action >= RM:
                # If the container was run with --rm, then stop will most likely remove the
                # container.  We first check if it is running then remove it.
                running = _containerIsRunning(containerName)
                if running is not None:
                    _logger.info('Removing container "%s".', containerName)
                    for attempt in retry(predicate=dockerPredicate):
                        with attempt:
                            subprocess.check_call(['docker', 'rm', '-f', containerName])
                else:
                    _logger.info('The container "%s" was not found on the system.  Nothing to remove.',
                                 containerName)

def _fixPermissions(tool, workDir):
    """
    Fix permission of a mounted Docker directory by reusing the tool to change ownership.
    Docker natively runs as a root inside the container, and files written to the
    mounted directory are implicitly owned by root.

    :param list baseDockerCall: Docker run parameters
    :param str tool: Name of tool
    :param str workDir: Path of work directory to recursively chown
    """
    if os.geteuid() == 0:
        # we're running as root so this chown is redundant
        return

    baseDockerCall = ['docker', 'run', '--log-driver=none',
                      '-v', os.path.abspath(workDir) + ':/data', '--rm', '--entrypoint=chown']
    stat = os.stat(workDir)
    command = baseDockerCall + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
    for attempt in retry(predicate=dockerPredicate):
        with attempt:
            subprocess.check_call(command)


def _getContainerName(job):
    return '--'.join([str(job),
                      base64.b64encode(os.urandom(9), '-_')]).replace("'", '').replace('_', '')


def _containerIsRunning(container_name):
    """
    Checks whether the container is running or not.
    :param container_name: Name of the container being checked.
    :returns: True if running, False if not running, None if the container doesn't exist.
    :rtype: bool
    """
    try:
        for attempt in retry(predicate=dockerPredicate):
            with attempt:
                output = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Running}}',
                                                  container_name]).strip()
    except subprocess.CalledProcessError:
        # This will be raised if the container didn't exist.
        _logger.debug("'docker inspect' failed. Assuming container %s doesn't exist.", container_name,
                   exc_info=True)
        return None
    if output == 'true':
        return True
    elif output == 'false':
        return False
    else:
        raise RuntimeError("Got unexpected value for State.Running (%s)" % output)
