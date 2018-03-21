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
    def testOfficialUbuntuRepo_NoDockerInstall(self):
        """Image exists, but is not quay.  This should raise."""
        ubuntu_repo = 'ubuntu:latest'
        with self.assertRaises(NameError):
            checkDockerImageExists(ubuntu_repo)

    def testBroadDockerRepo(self):
        """Image exists, but is not quay.  This should raise."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:2.0.0'
        with self.assertRaises(NameError):
            checkDockerImageExists(broad_repo)

    def testNonexistentRepo(self):
        """Bad image and not quay.  This should raise."""
        nonexistent_repo = '------:-----'
        with self.assertRaises(NameError):
            checkDockerImageExists(nonexistent_repo)

    def testBroadDockerRepo(self):
        """Image exists, and overrides the quay requirement.  Should pass."""
        broad_repo = '[override]broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo)

    def testToilQuayRepo(self):
        """Image exists.  Should pass."""
        toil_repo = 'quay.io/ucsc_cgl/toil:latest'
        assert checkDockerImageExists(toil_repo)

    def testBadQuayRepoNTag(self):
        """Bad quay repo and tag.  This should raise."""
        nonexistent_quay_repo = 'quay.io/--------:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)

    def testBadQuayRepo(self):
        """Bad quay repo.  This should raise."""
        nonexistent_quay_repo = 'quay.io/--------:latest'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)

    def testBadQuayTag(self):
        """Bad quay tag.  This should raise."""
        nonexistent_quay_repo = 'quay.io/ucsc_cgl/toil:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)
