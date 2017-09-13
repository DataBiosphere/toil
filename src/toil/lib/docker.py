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
from builtins import str
from builtins import map
import logging
import os
import pipes
import subprocess
import docker
from docker.utils.types import LogConfig

from bd2k.util.retry import retry

from toil.lib import dockerPredicate
from toil.lib import FORGO
from toil.lib import STOP
from toil.lib import RM

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
    _docker(job=job, tool=tool, parameters=parameters, workDir=workDir,
            dockerParameters=dockerParameters, outfile=outfile, checkOutput=False, defer=defer)

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
    return _docker(job=job, tool=tool, parameters=parameters, workDir=workDir,
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

    # default is 1000, which are standard user permissions
    # set this to 0 to become root
    # however all output files will have only root permissions
    defaultUser = 1000

    if parameters is None:
        parameters = []
    if workDir is None:
        workDir = os.getcwd()

    client = docker.from_env()

    # Ensure the user has passed a valid value for defer
    assert defer in (None, FORGO, STOP, RM), 'Please provide a valid value for defer.'

    if defer == FORGO:
        removeUponExit = False
    elif defer == STOP:
        removeUponExit = False
    else:
        removeUponExit = True

    if checkOutput:
        logDriver = {'type': LogConfig.types.SYSLOG, 'config': {}}
    else:
        logDriver = None

    # If parameters is list of lists, treat each list as separate command and chain with pipes
    if len(parameters) > 0 and type(parameters[0]) is list:
        chain_params = [' '.join(p) for p in [map(pipes.quote, q) for q in parameters]]
        command = ' | '.join(chain_params)
        _logger.info("Calling docker with: " + repr(command))
    else:
        command = [' '.join(p) for p in [map(pipes.quote, parameters)]]
        command = command[0]
        _logger.info("Calling docker with: " + repr(command))

    for attempt in retry(predicate=dockerPredicate):
        with attempt:
            try:
                client.containers.run(tool,
                                      command,
                                      os.path.abspath(workDir),
                                      volumes={os.path.abspath(workDir): {'bind': '/data/', 'mode': 'rw'}},
                                      remove=removeUponExit,
                                      log_config=logDriver,
                                      user=defaultUser)
                if defer == STOP:
                    client.containers.pause()
            # If the container exits with a non-zero exit code and detach is False.
            except docker.errors.ContainerError:
                _logger.error("Docker had non-zero exit.  Check your command: " + repr(command))
                return 1
            except docker.errors.ImageNotFound:
                _logger.error("Docker image not found.")
                return 2
            except docker.errors.APIError:
                _logger.error("The server returned an error.")
                return 3
    return 0

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