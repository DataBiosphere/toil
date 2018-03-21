# Copyright (C) 2015-2016 Regents of the University of California
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

from __future__ import absolute_import, print_function
from toil.test import ToilTest, slow, needs_appliance
from toil import checkDockerImageExists
from docker.errors import ImageNotFound, APIError


class dockerCheckTests(ToilTest):
    """
    Tests initial checking of whether a docker image exists in the specified repository or not.

    If docker is installed, this will throw an exception if an image does not exist.
    """
    def setUp(self):
        pass

    # Cases where docker is not installed and we check with the requests package.
    def testOfficialUbuntuRepo_NoDockerInstall(self, dockerInstalled=False):
        """Image exists.  Should pass."""
        ubuntu_repo = 'ubuntu:latest'
        assert checkDockerImageExists(ubuntu_repo, dockerInstalled)

    def testOfficialBusyBoxRepo_NoDockerInstall(self, dockerInstalled=False):
        """Image exists.  Should pass."""
        ubuntu_repo = 'busybox:latest'
        assert checkDockerImageExists(ubuntu_repo, dockerInstalled)

    def testToilQuayRepo_NoDockerInstall(self, dockerInstalled=False):
        """Image exists.  Should pass."""
        toil_repo = 'quay.io/ucsc_cgl/toil:latest'
        assert checkDockerImageExists(toil_repo, dockerInstalled)

    def testBroadDockerRepo_NoDockerInstall(self, dockerInstalled=False):
        """Image exists.  Should pass."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo, dockerInstalled)

    def testBadBroadDockerRepo_NoDockerInstall(self, dockerInstalled=False):
        """Bad image, but no docker installed and not quay.  Should warn user but pass."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:----------'
        checkDockerImageExists(broad_repo, dockerInstalled)

    def testNonexistentRepo_NoDockerInstall(self, dockerInstalled=False):
        """Bad image, but no docker installed and not quay.  Should warn user but pass."""
        nonexistent_repo = '------:-----'
        checkDockerImageExists(nonexistent_repo, dockerInstalled)

    def testNonexistentQuayRepo_NoDockerInstall(self, dockerInstalled=False):
        """Bad quay tag.  Even without docker installed, this should raise."""
        nonexistent_quay_repo = 'quay.io/--------:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo, dockerInstalled)

    # Cases where docker is installed and we check with the API.
    def testOfficialUbuntuRepo(self, dockerInstalled=True):
        """Image exists.  Should pass."""
        ubuntu_repo = 'ubuntu:latest'
        assert checkDockerImageExists(ubuntu_repo, dockerInstalled)

    def testOfficialBusyBoxRepo(self, dockerInstalled=True):
        """Image exists.  Should pass."""
        ubuntu_repo = 'busybox:latest'
        assert checkDockerImageExists(ubuntu_repo, dockerInstalled)

    def testToilQuayRepo(self, dockerInstalled=True):
        """Image exists.  Should pass."""
        toil_repo = 'quay.io/ucsc_cgl/toil:latest'
        assert checkDockerImageExists(toil_repo, dockerInstalled)

    def testBroadDockerRepo(self, dockerInstalled=True):
        """Image exists.  Should pass."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo, dockerInstalled)

    def testBadBroadDockerRepo(self, dockerInstalled=True):
        """Tag is bad.  Should raise."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:----------'
        with self.assertRaises(APIError):
            checkDockerImageExists(broad_repo, dockerInstalled)

    def testNonexistentRepo(self, dockerInstalled=True):
        """Repo is bad.  Should raise."""
        nonexistent_repo = '------:-----'
        with self.assertRaises(APIError):
            checkDockerImageExists(nonexistent_repo, dockerInstalled)
