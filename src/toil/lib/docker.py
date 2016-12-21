import base64
import os
import subprocess
from toil_lib import require
import logging

_logger = logging.getLogger(__name__)


def dockerCall(job, tool, parameters=None, workDir=None, dockerParameters=None, outfile=None, defer=None):
    """
    Wrapper call to Docker. See docstring for Docker()

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed to the tool.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] dockerParameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param int defer: What action should be taken on the container upon job completion?
           docker_call.FORGO (0) will leave the container untouched.
           docker_call.STOP (1) will attempt to stop the container with `docker stop` (useful for
           debugging).
           docker_call.RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`.
           The default value is None and that shadows docker_call.FORGO, unless --rm is being passed in.
    """
    Docker(job, tool=tool, parameters=parameters, workDir=workDir, dockerParameters=dockerParameters,
           outfile=outfile, checkOutput=False, defer=defer)


def dockerCheckOutput(job, tool, parameters=None, workDir=None, dockerParameters=None, defer=None):
    """
    Wrapper call to Docker. See docstring for Docker()

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed to the tool.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] dockerParameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param int defer: What action should be taken on the container upon job completion?
           docker_call.FORGO (0) will leave the container untouched.
           docker_call.STOP (1) will attempt to stop the container with `docker stop` (useful for
           debugging).
           docker_call.RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`.
           The default value is None and that shadows docker_call.FORGO, unless --rm is being passed in.
    :returns: Stdout from the docker call
    :rtype: str
    """
    return Docker(job, tool=tool, parameters=parameters, workDir=workDir,
                  dockerParameters=dockerParameters, checkOutput=True, defer=defer)


def Docker(job,
           tool,
           parameters=None,
           workDir=None,
           dockerParameters=None,
           outfile=None,
           checkOutput=False,
           defer=None):
    """
    Calls Docker for a particular tool with the specified parameters. Assumes `docker` is on the PATH.

    Example of using docker_call in a Toil pipeline to index a FASTA file with SAMtools:
        def toil_job(job):
            work_dir = job.fileStore.getLocalTempDir()
            path = job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta')
            parameters = ['faidx', path]
            docker_call(job, tool='quay.io/ucgc_cgl/samtools', work_dir=work_dir, parameters=parameters)

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed to the tool.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] dockerParameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param bool checkOutput: When True, this function returns docker's output.
    :param int defer: What action should be taken on the container upon job completion?
           docker_call.FORGO (0) will leave the container untouched.
           docker_call.STOP (1) will attempt to stop the container with `docker stop` (useful for
           debugging).
           docker_call.RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`.
           The default value is None and that shadows docker_call.FORGO, unless --rm is being passed in.
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
    require(defer in (None, Docker.FORGO, Docker.STOP, Docker.RM),
            'Please provide a valid value for defer.')

    # Get container name which is needed for _dockerKill
    try:
        if any('--name=' in x for x in baseDockerCall):
            containerName = [x.split('=')[1] for x in baseDockerCall if '--name=' in x][0]
        else:
            containerName = baseDockerCall[baseDockerCall.index('--name') + 1]
    except ValueError:
        containerName = _getContainerName(job)
        baseDockerCall.extend(['--name', containerName])
    except IndexError:
        raise RuntimeError("Couldn't parse Docker's `--name=` option, check parameters: " + str(dockerParameters))

    # Defer the container on-exit action
    if '--rm' in baseDockerCall and defer is None:
        defer = Docker.RM
    if '--rm' in baseDockerCall and defer is not Docker.RM:
        _logger.warn('--rm being passed to docker call but defer not set to dockerCall.RM, defer set to: ' + str(defer))
    job.defer(_dockerKill, containerName, action=defer)
    # Defer the permission fixing function which will run after this job concludes.
    # We call this explicitly later on in this function, but we defer it as well to handle unexpected job failure.
    job.defer(_fixPermissions, baseDockerCall, tool, workDir)

    # Make subprocess call
    call = baseDockerCall + [tool] + parameters
    job.fileStore.logToMaster("Calling docker with " + repr(call))

    if outfile:
        subprocess.check_call(call, stdout=outfile)
    else:
        if checkOutput:
            return subprocess.check_output(call)
        else:
            subprocess.check_call(call)


Docker.FORGO = 0
Docker.STOP = 1
Docker.RM = 2


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
        if action in (None, Docker.FORGO):
            _logger.info('The container with name %s continues to exist as we were asked to forgo a '
                         'post-job action on it.', containerName)
        else:
            _logger.info('The container with name %s exists. Running user-specified defer functions.',
                         containerName)
            if running and (action == Docker.STOP or action == Docker.RM):
                _logger.info('Stopping container "%s".', containerName)
                subprocess.check_call(['docker', 'stop', containerName])
            else:
                _logger.info('The container "%s" was not found to be running.', containerName)
            if action == Docker.RM:
                # If the container was run with --rm, then stop will most likely remove the
                # container.  We first check if it is running then remove it.
                running = _containerIsRunning(containerName)
                if running is not None:
                    _logger.info('Removing container "%s".', containerName)
                    try:
                        subprocess.check_call(['docker', 'rm', '-f', containerName])
                    except subprocess.CalledProcessError:
                        _logger.exception("'docker rm' failed: " + str(containerName))
                else:
                    _logger.warn('The container "%s" was not found on the system.  Nothing to remove.',
                                 containerName)


def _fixPermissions(baseDockerCall, tool, workDir):
    """
    Fix permission of a mounted Docker directory by reusing the tool to change ownership.
    Docker natively runs as a root inside the container, and files written to the
    mounted directory are implicitly owned by root.

    :param list baseDockerCall: Docker run parameters
    :param str tool: Name of tool
    :param str workDir: Path of work directory to recursively chown
    """
    baseDockerCall.append('--entrypoint=chown')
    # We don't need the cleanup container to persist.
    baseDockerCall.append('--rm')
    stat = os.stat(workDir)
    command = baseDockerCall + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
    subprocess.check_call(command)


def _getContainerName(job):
    return '--'.join([str(job),
                      job.fileStore.jobID,
                      base64.b64encode(os.urandom(9), '-_')])


def _containerIsRunning(container_name):
    """
    Checks whether the container is running or not.

    :param container_name: Name of the container being checked.
    :returns: True if running, False if not running, None if the container doesn't exist.
    :rtype: bool
    """
    try:
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
        raise AssertionError("Got unexpected value for State.Running (%s)" % output)
