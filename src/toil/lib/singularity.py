"""
    Module for running Singularity (and Docker) images with Singularity.   
    Derived from https://github.com/BD2KGenomics/toil/blob/master/src/toil/lib/docker.py
    with which nearly all code is shared, save the command lines used to run the containers.

    Docker images can be run by prefacing the input tool with docker:// In this case, Singularity
    will download, convert, and cache the image on the fly.  This cache can be set with 
    SINGULARITY_CACHEDIR, and defaults to the user's home directory.  This cache can be a major
    bottleneck when repeatedly more different images than it can hold (not very many).  So for this
    type of usage pattern (concurrent or short consecutive calls to different images), it is 
    best to run Singularity images natively.
    
"""
import base64
import logging
import subprocess
import pipes
import os
from bd2k.util.exceptions import require

_logger = logging.getLogger(__name__)


def singularityCall(job,
               tool,
               parameters=None,
               workDir=None,
               singularityParameters=None,
               outfile=None):
    """
    Throws CalledProcessorError if the Singularity invocation returns a non-zero exit code
    This function blocks until the subprocess call to Singularity returns
    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Singularity image to be used (preface with docker:// to use a Docker image)
    :param list[str] parameters: Command line arguments to be passed to the tool.
           If list of lists: list[list[str]], then treat as successive commands chained with pipe.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] singularityParameters: Parameters to pass to Singularity. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if singularity_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Singularity call to file handle
    """
    _singularity(job, tool=tool, parameters=parameters, workDir=workDir, singularityParameters=singularityParameters,
            outfile=outfile, checkOutput=False)


def singularityCheckOutput(job,
                      tool,
                      parameters=None,
                      workDir=None,
                      singularityParameters=None):
    """
    Returns the stdout from the Singularity invocation (via subprocess.check_output)
    Throws CalledProcessorError if the Singularity invocation returns a non-zero exit code
    This function blocks until the subprocess call to Singularity returns
    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Singularity image to be used (preface with docker:// to use a Docker image)
    :param list[str] parameters: Command line arguments to be passed to the tool.
           If list of lists: list[list[str]], then treat as successive commands chained with pipe.
    :param str workDir: Directory to mount into the container via `-v`. Destination convention is /data
    :param list[str] singularityParameters: Parameters to pass to Singularity. Default parameters are `--rm`,
            `--log-driver none`, and the mountpoint `-v work_dir:/data` where /data is the destination convention.
             These defaults are removed if singularity_parmaters is passed, so be sure to pass them if they are desired.
    :returns: Stdout from the singularity call
    :rtype: str
    """
    return _singularity(job, tool=tool, parameters=parameters, workDir=workDir,
                   singularityParameters=singularityParameters, checkOutput=True)


def _singularity(job,
            tool,
            parameters=None,
            workDir=None,
            singularityParameters=None,
            outfile=None,
            checkOutput=False):
    """
    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Singularity image to be used (preface with docker:// to use a Docker image)
    :param list[str] parameters: Command line arguments to be passed to the tool.
           If list of lists: list[list[str]], then treat as successive commands chained with pipe.
    :param str workDir: Directory to mount into the container via `--bind`. Destination convention is /data
    :param list[str] singularityrParameters: Parameters to pass to Singularity. Default parameters are the mountpoint
             `--bind work_dir:/data` where /data is the destination convention.
             These defaults are removed if singularity_parmaters is passed, so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Singularity call to file handle
    :param bool checkOutput: When True, this function returns singularity's output.
    """
    if parameters is None:
        parameters = []
    if workDir is None:
        workDir = os.getcwd()

    # Setup the outgoing subprocess call for singularity
    baseSingularityCall = ['singularity', '-q', 'exec']
    if singularityParameters:
        baseSingularityCall += singularityParameters
    else:
        # We default to running on the container's home directory, because it is most likely to be mounted
        # in singularity.conf (this was an issue on Biowolf at NIH)
        baseSingularityCall += ['-H', '{}:{}'.format(os.path.abspath(workDir), os.environ.get('HOME')), '--pwd', os.environ.get('HOME')]

    # Make subprocess call

    # If parameters is list of lists, treat each list as separate command and chain with pipes
    if len(parameters) > 0 and type(parameters[0]) is list:
        # When piping, all arguments now get merged into a single string to bash -c.
        # We try to support spaces in paths by wrapping them all in quotes first.
        chain_params = [' '.join(p) for p in [map(pipes.quote, q) for q in parameters]]
        # Use bash's set -eo pipefail to detect and abort on a failure in any command in the chain
        call = baseSingularityCall + [tool, '/bin/bash', '-c',
                                 'set -eo pipefail && {}'.format(' | '.join(chain_params))]
    else:
        call = baseSingularityCall + [tool] + parameters
    _logger.info("Calling singularity with " + repr(call))

    params = {}
    if outfile:
        params['stdout'] = outfile
    if checkOutput:
        callMethod = subprocess.check_output
    else:
        callMethod = subprocess.check_call

    out = callMethod(call, **params)

    return out
