# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import

import logging
import os
import sys
import requests
from docker.errors import ImageNotFound
from toil.lib.memoize import memoize
from toil.version import currentCommit

# subprocess32 is a backport of python3's subprocess module for use on Python2,
# and includes many reliability bug fixes relevant on POSIX platforms.
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

log = logging.getLogger(__name__)


def toilPackageDirPath():
    """
    Returns the absolute path of the directory that corresponds to the top-level toil package.
    The return value is guaranteed to end in '/toil'.
    """
    result = os.path.dirname(os.path.realpath(__file__))
    assert result.endswith('/toil')
    return result


def inVirtualEnv():
    return hasattr(sys, 'real_prefix')


def resolveEntryPoint(entryPoint):
    """
    Returns the path to the given entry point (see setup.py) that *should* work on a worker. The
    return value may be an absolute or a relative path.
    """
    if inVirtualEnv():
        path = os.path.join(os.path.dirname(sys.executable), entryPoint)
        # Inside a virtualenv we try to use absolute paths to the entrypoints.
        if os.path.isfile(path):
            # If the entrypoint is present, Toil must have been installed into the virtualenv (as
            # opposed to being included via --system-site-packages). For clusters this means that
            # if Toil is installed in a virtualenv on the leader, it must be installed in
            # a virtualenv located at the same path on each worker as well.
            assert os.access(path, os.X_OK)
            return path
        else:
            # For virtualenv's that have the toil package directory on their sys.path but whose
            # bin directory lacks the Toil entrypoints, i.e. where Toil is included via
            # --system-site-packages, we rely on PATH just as if we weren't in a virtualenv.
            return entryPoint
    else:
        # Outside a virtualenv it is hard to predict where the entry points got installed. It is
        # the responsibility of the user to ensure that they are present on PATH and point to the
        # correct version of Toil. This is still better than an absolute path because it gives
        # the user control over Toil's location on both leader and workers.
        return entryPoint


@memoize
def physicalMemory():
    """
    >>> n = physicalMemory()
    >>> n > 0
    True
    >>> n == physicalMemory()
    True
    """
    try:
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    except ValueError:
        return int(subprocess.check_output(['sysctl', '-n', 'hw.memsize']).strip())


def physicalDisk(config, toilWorkflowDir=None):
    if toilWorkflowDir is None:
        from toil.common import Toil
        toilWorkflowDir = Toil.getWorkflowDir(config.workflowID, config.workDir)
    diskStats = os.statvfs(toilWorkflowDir)
    return diskStats.f_frsize * diskStats.f_bavail


def applianceSelf(forceDockerAppliance=False):
    """
    Returns the fully qualified name of the Docker image to start Toil appliance containers from.
    The result is determined by the current version of Toil and three environment variables:
    ``TOIL_DOCKER_REGISTRY``, ``TOIL_DOCKER_NAME`` and ``TOIL_APPLIANCE_SELF``.

    ``TOIL_DOCKER_REGISTRY`` specifies an account on a publicly hosted docker registry like Quay
    or Docker Hub. The default is UCSC's CGL account on Quay.io where the Toil team publishes the
    official appliance images. ``TOIL_DOCKER_NAME`` specifies the base name of the image. The
    default of `toil` will be adequate in most cases. ``TOIL_APPLIANCE_SELF`` fully qualifies the
    appliance image, complete with registry, image name and version tag, overriding both
    ``TOIL_DOCKER_NAME`` and `TOIL_DOCKER_REGISTRY`` as well as the version tag of the image.
    Setting TOIL_APPLIANCE_SELF will not be necessary in most cases.

    :rtype: str
    """
    import toil.version
    registry = lookupEnvVar(name='docker registry',
                            envName='TOIL_DOCKER_REGISTRY',
                            defaultValue=toil.version.dockerRegistry)
    name = lookupEnvVar(name='docker name',
                        envName='TOIL_DOCKER_NAME',
                        defaultValue=toil.version.dockerName)
    appliance = lookupEnvVar(name='docker appliance',
                             envName='TOIL_APPLIANCE_SELF',
                             defaultValue=registry + '/' + name + ':' + toil.version.dockerTag)

    checkDockerSchema(appliance)

    if forceDockerAppliance:
        return appliance
    else:
        return checkDockerImageExists(appliance=appliance)


def lookupEnvVar(name, envName, defaultValue):
    """
    Use this for looking up environment variables that control Toil and are important enough to
    log the result of that lookup.

    :param str name: the human readable name of the variable
    :param str envName: the name of the environment variable to lookup
    :param str defaultValue: the fall-back value
    :return: the value of the environment variable or the default value the variable is not set
    :rtype: str
    """
    try:
        value = os.environ[envName]
    except KeyError:
        log.info('Using default %s of %s as %s is not set.', name, defaultValue, envName)
        return defaultValue
    else:
        log.info('Overriding %s of %s with %s from %s.', name, defaultValue, value, envName)
        return value


def checkDockerImageExists(appliance):
    """
    Attempts to check a url registryName for the existence of a docker image with a given tag.

    :param str appliance: The url of a docker image's registry (with a tag) of the form:
                          'quay.io/<repo_path>:<tag>' or '<repo_path>:<tag>'.
                          Examples: 'quay.io/ucsc_cgl/toil:latest', 'ubuntu:latest', or
                          'broadinstitute/genomes-in-the-cloud:2.0.0'.
    :return: Raises an exception if the docker image cannot be found or is invalid.  Otherwise, it
             will return the appliance string.
    :rtype: str
    """
    if currentCommit in appliance:
        return appliance
    registryName, imageName, tag = parseDockerAppliance(appliance)

    if registryName == 'docker.io':
        return requestCheckDockerIo(origAppliance=appliance, imageName=imageName, tag=tag)
    else:
        return requestCheckRegularDocker(origAppliance=appliance, registryName=registryName, imageName=imageName, tag=tag)


def parseDockerAppliance(appliance):
    """
    Takes string describing a docker image and returns the parsed
    registry, image reference, and tag for that image.

    Example: "quay.io/ucsc_cgl/toil:latest"
    Should return: "quay.io", "ucsc_cgl/toil", "latest"

    If a registry is not defined, the default is: "docker.io"
    If a tag is not defined, the default is: "latest"

    :param appliance: The full url of the docker image originally
                      specified by the user (or the default).
                      e.g. "quay.io/ucsc_cgl/toil:latest"
    :return: registryName, imageName, tag
    """
    appliance = appliance.lower()

    # get the tag
    if ':' in appliance:
        tag = appliance.split(':')[-1]
        appliance = appliance[:-(len(':' + tag))] # remove only the tag
    else:
        # default to 'latest' if no tag is specified
        tag = 'latest'

    # get the registry and image
    registryName = 'docker.io' # default if not specified
    imageName = appliance # will be true if not specified
    if '/' in appliance and '.' in appliance.split('/')[0]:
        registryName = appliance.split('/')[0]
        imageName = appliance[len(registryName):]
    registryName = registryName.strip('/')
    imageName = imageName.strip('/')

    return registryName, imageName, tag


def checkDockerSchema(appliance):
    if not appliance:
        raise ImageNotFound("No docker image specified.")
    elif '://' in appliance:
        raise ImageNotFound("Docker images cannot contain a schema (such as '://'): %s"
                            "" % appliance)
    elif len(appliance) > 256:
        raise ImageNotFound("Docker image must be less than 256 chars: %s"
                            "" % appliance)


class ApplianceImageNotFound(ImageNotFound):
    """
    Compose an ApplianceImageNotFound error complaining that the given name and
    tag for TOIL_APPLIANCE_SELF specify an image manifest which could not be
    retrieved from the given URL, because it produced the given HTTP error
    code.

    :param str origAppliance: The full url of the docker image originally
                              specified by the user (or the default).
                              e.g. "quay.io/ucsc_cgl/toil:latest"
    :param str url: The URL at which the image's manifest is supposed to appear
    :param int statusCode: the failing HTTP status code returned by the URL
    """
    def __init__(self, origAppliance, url, statusCode):
        msg = ("The docker image that TOIL_APPLIANCE_SELF specifies (%s) produced "
               "a nonfunctional manifest URL (%s). The HTTP status returned was %s. "
               "The specifier is most likely unsupported or malformed.  "
               "Please supply a docker image with the format: "
               "'<websitehost>.io/<repo_path>:<tag>' or '<repo_path>:<tag>' "
               "(for official docker.io images).  Examples: "
               "'quay.io/ucsc_cgl/toil:latest', 'ubuntu:latest', or "
               "'broadinstitute/genomes-in-the-cloud:2.0.0'."
               "" % (origAppliance, url, str(statusCode)))
        super(ApplianceImageNotFound, self).__init__(msg)


def requestCheckRegularDocker(origAppliance, registryName, imageName, tag):
    """
    Checks to see if an image exists using the requests library.

    URL is based on the docker v2 schema described here:
    https://docs.docker.com/registry/spec/manifest-v2-2/

    This has the following format:
    https://{websitehostname}.io/v2/{repo}/manifests/{tag}

    Does not work with the official (docker.io) site, because they require an OAuth token, so a
    separate check is done for docker.io images.

    :param str origAppliance: The full url of the docker image originally
                              specified by the user (or the default).
                              e.g. "quay.io/ucsc_cgl/toil:latest"
    :param str registryName: The url of a docker image's registry.  e.g. "quay.io"
    :param str imageName: The image, including path and excluding the tag. e.g. "ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Return True if match found.  Raise otherwise.
    """
    ioURL = 'https://{webhost}/v2/{pathName}/manifests/{tag}' \
              ''.format(webhost=registryName, pathName=imageName, tag=tag)
    response = requests.head(ioURL)
    if not response.ok:
        raise ApplianceImageNotFound(origAppliance, ioURL, response.status_code)
    else:
        return origAppliance


def requestCheckDockerIo(origAppliance, imageName, tag):
    """
    Checks docker.io to see if an image exists using the requests library.

    URL is based on the docker v2 schema.  Requires that an access token be fetched first.

    :param str origAppliance: The full url of the docker image originally
                              specified by the user (or the default).  e.g. "ubuntu:latest"
    :param str imageName: The image, including path and excluding the tag. e.g. "ubuntu"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Return True if match found.  Raise otherwise.
    """
    # only official images like 'busybox' or 'ubuntu'
    if '/' not in imageName:
        imageName = 'library/' + imageName

    token_url = 'https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull'.format(repo=imageName)
    requests_url = 'https://registry-1.docker.io/v2/{repo}/manifests/{tag}'.format(repo=imageName, tag=tag)

    token = requests.get(token_url)
    jsonToken = token.json()
    bearer = jsonToken["token"]
    response = requests.head(requests_url, headers={'Authorization': 'Bearer {}'.format(bearer)})
    if not response.ok:
        raise ApplianceImageNotFound(origAppliance, requests_url, response.status_code)
    else:
        return origAppliance


def logProcessContext(config):
    # toil.version.version (string) cannot be imported at top level because it conflicts with
    # toil.version (module) and Sphinx doesn't like that.
    from toil.version import version
    log.info("Running Toil version %s.", version)
    log.debug("Configuration: %s", config.__dict__)
