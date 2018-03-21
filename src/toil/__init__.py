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
import docker
import time
from docker.errors import ImageNotFound
from docker.errors import APIError
from docker.errors import create_api_error_from_http_exception
from bd2k.util import memoize

# subprocess32 is a backport of python3's subprocess module for use on Python2,
# and includes many reliability bug fixes relevant on POSIX platforms.
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

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
        # the reponsibility of the user to ensure that they are present on PATH and point to the
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


def applianceSelf():
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

    checkDockerImageExists(appliance=appliance, dockerInstalled=isDockerInstalled())

    return appliance


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


def isDockerInstalled():
    """Returns True if docker is installed.  False otherwise."""
    try:
        subprocess.check_call(['which', 'docker'])
        return True
    except:
        return False


def checkDockerImageExists(appliance, dockerInstalled):
    """
    Attempts to check a url registry_name for the existence of a docker image with a given tag.

    :param str registry_name: The url of a docker image's registry.  e.g. "quay.io/ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Raises an exception if the docker image cannot be found.  Otherwise return True.
    May return True if the docker image does not exist but it cannot verify (docker is not
    installed).
    """
    appliance = appliance.lower()
    tag = appliance.split(':')[-1]
    registry_name = appliance[:-(len(':' + tag))] # remove only the tag

    if dockerInstalled:
        apiCheck(registry_name=registry_name, tag=tag)
    else:
        requestsCheck(registry_name=registry_name, tag=tag)
    return True


def requestsCheck(registry_name, tag):
    """
    Attempts to check a url registry_name for the existence of a docker image with a given tag.

    Does not require a docker installation, but only checks quay.io and docker.com repositories.

    :param str registry_name: The url of a docker image's registry.  e.g. "quay.io/ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Nothing, but raises an exception if the docker image cannot be found.
    """
    if 'quay.io/' in registry_name:
        requestsCheckQuay(registry_name=registry_name, tag=tag)
    else:
        # assume official docker.com repo and attempt a check
        requestsCheckOfficialDocker(registry_name=registry_name, tag=tag)


def requestsCheckQuay(registry_name, tag):
    """
    Checks quay to see if an image exists using the requests library.

    :param str registry_name: The url of a docker image's registry.  e.g. "quay.io/ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Return True if match found.  Raise otherwise.
    """
    # find initial position of 'quay.io/' so that '//', 'http://', 'https://', etc. are removed
    clip_position = registry_name.find('quay.io/')
    # cut off 'quay.io/' itself too to get the path_name
    path_name = registry_name[clip_position + len('quay.io/'):]
    if path_name.endswith('/'):
        path_name = path_name[:-1]
    quay_url = 'https://quay.io/v2/{path_name}/manifests/{tag}'.format(path_name=path_name, tag=tag)
    response = requests.head(quay_url)
    if not response.ok:
        raise ImageNotFound("TOIL_APPLIANCE_SELF not found at Quay.io: %s"
                            "" % registry_name + ':' + tag)
    else:
        return True


def requestsCheckOfficialDocker(registry_name, tag):
    """
    Checks the official docker repo.  Return True if match found.  Return False if unsure.

    :param str registry_name: The url of a docker image's registry.  e.g. "quay.io/ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Return True if match found.  Return False if unsure.
    """
    if registry_name.endswith('/'):
        registry_name = registry_name[:-1]
    # only official docker.com images such as 'ubuntu:latest' or 'busybox:latest'
    if '/' not in registry_name:
        docker_url = 'https://hub.docker.com/r/library/{path_name}/tags'.format(path_name=registry_name)
    # third-party docker.com images such as 'broadinstitute/genomes-in-the-cloud:2.1.1'
    else:
        docker_url = 'https://hub.docker.com/r/{path_name}/tags'.format(path_name=registry_name)
    response = requests.get(docker_url)
    if response.ok:
        # fetch the html and parse with beautifulsoup4 to look for tags
        http_text = str(response.content).decode("utf-8")
        soup = BeautifulSoup(http_text, "html5lib")
        divs = soup.find_all('div')
        for div in divs:
            if div.text.startswith('404 Page Not Found'):
                return False
            if div.get('class'):
                if 'FlexTable__flexItemGrow2___3I1KN' in div.get('class'):
                    if tag == div.text:
                        return True
        # # if no matching tags found
        # raise ImageNotFound("Tag for docker image (TOIL_APPLIANCE_SELF) not found in "
        #                     "registry: %s." % (registry_name + ':' + tag))
    # might not be a docker.com repository but we made a best effort
    log.warning('Cannot verify that TOIL_APPLIANCE_SELF exists.  If starting a cluster, and it'
                'takes too long to launch (more than 5-10 minutes), stop and check '
                'TOIL_APPLIANCE_SELF.  Docker is not installed on this system.  Please '
                'install docker to properly check.')
    return False


def apiCheck(registry_name, tag):
    """
    Attempts to check a url registry_name for the existence of a docker image with a given tag.

    More robust than checking with the requestsCheck(appliance) metadata fetch, but requires
    docker to be installed on the machine to work.

    :param str registry_name: The url of a docker image's registry.  e.g. "quay.io/ucsc_cgl/toil"
    :param str tag: The tag used at that docker image's registry.  e.g. "latest"
    :return: Nothing, but raises an exception if the docker image cannot be found.
    """
    client = docker.APIClient(base_url='unix://var/run/docker.sock', version='auto')
    try:
        # raises an error if the image does not exist
        for line in client.pull(registry_name + ':' + tag, stream=True):
            time.sleep(10)
            break
    except ImageNotFound:
        log.error("Docker image (TOIL_APPLIANCE_SELF) not found: %s" % registry_name)
        raise
    except APIError:
        log.error("Docker image (TOIL_APPLIANCE_SELF) called with APIError: %s" % registry_name)
        raise
    except requests.exceptions.HTTPError as e:
        log.error("The server returned an error.")
        raise create_api_error_from_http_exception(e)
    except:
        raise


def logProcessContext(config):
    # toil.version.version (string) cannot be imported at top level because it conflicts with
    # toil.version (module) and Sphinx doesn't like that.
    from toil.version import version
    log.info("Running Toil version %s.", version)
    log.debug("Configuration: %s", config.__dict__)
