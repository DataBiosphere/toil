import os
import subprocess
from toil_lib import require
import logging

logger = logging.getLogger(__name__)


def docker_call(job,
                tool,
                parameters=None,
                work_dir='.',
                docker_parameters=None,
                outfile=None,
                check_output=False,
                defer=None):
    """
        Calls Docker for a particular tool with the specified parameters

        Example of using docker_call in a Toil pipeline:
            def toil_job(job):
                work_dir = job.fileStore.getLocalTempDir()
                path = job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta')
                parameters = ['faidx', path]
                docker_call(job, tool='quay.io/ucgc_cgl/samtools', work_dir=work_dir, parameters=parameters)

        :param toil.Job.job job: The Job instance for the calling function.
        :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
        :param list[str] parameters: Command line arguments to be passed to the tool.
        :param str work_dir: Directory to mount into the container via `-v`. Destination convention is /data
        :param list[str] docker_parameters: Parameters to pass to Docker. Default parameters are `--rm`,
                `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
                 These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
        :param file outfile: Pipe output of Docker call to file handle
        :param bool check_output: When True, this function returns docker's output.
        :param int defer: What action should be taken on the container upon job completion?
               docker_call.FORGO (0) will leave the container untouched.
               docker_call.STOP (1) will attempt to stop the container with `docker stop` (useful for
               debugging).
               docker_call.RM (2) will stop the container and then forcefully remove it from the system
               using `docker rm -f`.
               The default value is None and that shadows docker_call.FORGO, unless --rm is being passed in.
    """
    docker_call.FORGO = 0
    docker_call.STOP = 1
    docker_call.RM = 2

    if parameters is None:
        parameters = []

    # Setup the outgoing subprocess call for docker
    base_docker_call = ['docker', 'run']
    if docker_parameters:
        base_docker_call += docker_parameters
    else:
        base_docker_call += ['--rm', '--log-driver', 'none', '-v',
                             os.path.abspath(work_dir) + ':/data']

    # Ensure the user has passed a valid value for defer
    require(defer in (None, docker_call.FORGO, docker_call.STOP, docker_call.RM),
            'Please provide a valid value for defer.')

    # Get container name which is needed for _docker_kill
    try:
        if any('--name=' in x for x in base_docker_call):
            container_name = [x.split('=')[1] for x in base_docker_call if '--name=' in x][0]
        else:
            container_name = base_docker_call[base_docker_call.index('--name') + 1]
    except ValueError:
        container_name = _get_container_name(job)
        base_docker_call.extend(['--name', container_name])
    except IndexError:
        raise RuntimeError("Couldn't parse Docker's `--name=` option, check parameters: " + str(base_docker_call))

    # Defer the container on-exit action
    if '--rm' in base_docker_call and defer is None:
        defer = docker_call.RM
    job.defer(_docker_kill, container_name, action=defer)
    # Defer the permission fixing function.  We call this explicitly later on in this function, but
    # we defer it as well to handle unexpected job failure.
    job.defer(_fix_permissions, base_docker_call, tool, work_dir)

    # Make subprocess call
    call = base_docker_call + [tool] + parameters
    job.fileStore.logToMaster("Calling docker with {}.".format(' '.join(call)))

    if outfile:
        subprocess.check_call(call, stdout=outfile)
    else:
        if check_output:
            return subprocess.check_output(call)
        else:
            subprocess.check_call(call)

    # Fix root ownership of output files
    _fix_permissions(base_docker_call, tool, work_dir)
    """
    Calls Docker for a particular tool with the specified parameters

    Example of using docker_call in a Toil pipeline:
        def toil_job(job):
            work_dir = job.fileStore.getLocalTempDir()
            path = job.fileStore.readGlobalFile(ref_id, os.path.join(work_dir, 'ref.fasta')
            parameters = ['faidx', path]
            docker_call(job, tool='quay.io/ucgc_cgl/samtools', work_dir=work_dir, parameters=parameters)

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed to the tool.
    :param str work_dir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] docker_parameters: Parameters to pass to Docker. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if docker_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param bool check_output: When True, this function returns docker's output.
    :param int defer: What action should be taken on the container upon job completion?
           docker_call.FORGO (0) will leave the container untouched.
           docker_call.STOP (1) will attempt to stop the container with `docker stop` (useful for
           debugging).
           docker_call.RM (2) will stop the container and then forcefully remove it from the system
           using `docker rm -f`.
           The default value is None and that shadows docker_call.FORGO, unless --rm is being passed in.
    """
    docker_call.FORGO = 0
    docker_call.STOP = 1
    docker_call.RM = 2

    if parameters is None:
        parameters = []

    # Setup the outgoing subprocess call for docker
    base_docker_call = ['docker', 'run']
    if docker_parameters:
        base_docker_call += docker_parameters
    else:
        base_docker_call += ['--rm', '--log-driver', 'none', '-v',
                             os.path.abspath(work_dir) + ':/data']

    # Ensure the user has passed a valid value for defer
    require(defer in (None, docker_call.FORGO, docker_call.STOP, docker_call.RM),
            'Please provide a valid value for defer.')

    # Get container name which is needed for _docker_kill
    try:
        if any('--name=' in x for x in base_docker_call):
            container_name = [x.split('=')[1] for x in base_docker_call if '--name=' in x][0]
        else:
            container_name = base_docker_call[base_docker_call.index('--name') + 1]
    except ValueError:
        container_name = _get_container_name(job)
        base_docker_call.extend(['--name', container_name])
    except IndexError:
        raise RuntimeError("Couldn't parse Docker's `--name=` option, check parameters: " + str(base_docker_call))

    # Defer the container on-exit action
    if '--rm' in base_docker_call and defer is None:
        defer = docker_call.RM
    job.defer(_docker_kill, container_name, action=defer)
    # Defer the permission fixing function.  We call this explicitly later on in this function, but
    # we defer it as well to handle unexpected job failure.
    job.defer(_fix_permissions, base_docker_call, tool, work_dir)

    # Make subprocess call
    call = base_docker_call + [tool] + parameters
    job.fileStore.logToMaster("Calling docker with {}.".format(' '.join(call)))

    if outfile:
        subprocess.check_call(call, stdout=outfile)
    else:
        if check_output:
            return subprocess.check_output(call)
        else:
            subprocess.check_call(call)

    # Fix root ownership of output files
    _fix_permissions(base_docker_call, tool, work_dir)


def _docker_kill(container_name, action):
    """
    Kills the specified container.

    :param str container_name: The name of the container created by docker_call
    :param int action: What action should be taken on the container?  See `defer=` in
           :func:`docker_call`
    """
    docker_call.FORGO = 0
    docker_call.STOP = 1
    docker_call.RM = 2

    running = _container_is_running(container_name)
    if running is None:
        # This means that the container doesn't exist.  We will see this if the container was run
        # with --rm and has already exited before this call.
        logger.info('The container with name "%s" appears to have already been removed.  Nothing to '
                    'do.', container_name)
    else:
        if action in (None, docker_call.FORGO):
            logger.info('The container with name %s continues to exist as we were asked to forgo a '
                        'post-job action on it.', container_name)
            return
        else:
            logger.info('The container with name %s exists. Running user-specified defer functions.',
                        container_name)
            if running and action >= docker_call.STOP:
                logger.info('Stopping container "%s".', container_name)
                subprocess.check_call(['docker', 'stop', container_name])
            else:
                logger.info('The container "%s" was not found to be running.', container_name)
            if action >= docker_call.RM:
                # If the container was run with --rm, then stop will most likely remove the
                # container.  We first check if it is running then remove it.
                running = _container_is_running(container_name)
                if running is not None:
                    logger.info('Removing container "%s".', container_name)
                    try:
                        subprocess.check_call(['docker', 'rm', '-f', container_name])
                    except subprocess.CalledProcessError:
                        logger.exception("'docker rm' failed.")
                else:
                    logger.info('The container "%s" was not found on the system.  Nothing to remove.',
                                container_name)


def _fix_permissions(base_docker_call, tool, work_dir):
    """
    Fix permission of a mounted Docker directory by reusing the tool

    :param list base_docker_call: Docker run parameters
    :param str tool: Name of tool
    :param str work_dir: Path of work directory to recursively chown
    """
    base_docker_call.append('--entrypoint=chown')
    # We don't need the cleanup container to persist.
    base_docker_call.append('--rm')
    stat = os.stat(work_dir)
    command = base_docker_call + [tool] + ['-R', '{}:{}'.format(stat.st_uid, stat.st_gid), '/data']
    subprocess.check_call(command)


def _get_container_name(job):
    return '--'.join([job.fileStore.jobStore.config.workflowID,
                      job.fileStore.jobID,
                      base64.b64encode(os.urandom(9), '-_')])


def _container_is_running(container_name):
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
        logger.debug("'docker inspect' failed. Assuming container %s doesn't exist.", container_name,
                     exc_info=True)
        return None
    if output == 'true':
        return True
    elif output == 'false':
        return False
    else:
        raise AssertionError("Got unexpected value for State.Running (%s)" % output)
