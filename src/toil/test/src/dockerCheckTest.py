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
from toil.test import ToilTest
from toil import checkDockerImageExists
from docker.errors import ImageNotFound


class dockerCheckTests(ToilTest):
    """
    Tests initial checking of whether a docker image exists in the specified repository or not.
    """
    def setUp(self):
        pass

    def testOfficialUbuntuRepo(self):
        """Image exists.  This should pass."""
        ubuntu_repo = 'ubuntu:latest'
        assert checkDockerImageExists(ubuntu_repo)

    def testBroadDockerRepo(self):
        """Image exists.  This should pass."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo)

    def testBroadDockerRepo(self):
        """Bad tag.  This should raise."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:-----'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(broad_repo)

    def testNonexistentRepo(self):
        """Bad image.  This should raise."""
        nonexistent_repo = '------:-----'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_repo)

    def testNonexistentRepoOverride(self):
        """Bad image, and overrides.  Should pass."""
        nonexistent_repo = '[override]------:-----'
        assert checkDockerImageExists(nonexistent_repo)

    def testBroadDockerRepoOverride(self):
        """Image exists, and overrides.  Should pass."""
        broad_repo = '[override]broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo)

    def testToilQuayRepo(self):
        """Image exists.  Should pass."""
        toil_repo = 'quay.io/ucsc_cgl/toil:latest'
        assert checkDockerImageExists(toil_repo)

    def testBadQuayRepoNTag(self):
        """Bad repo and tag.  This should raise."""
        nonexistent_quay_repo = 'quay.io/--------:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)

    def testBadQuayRepo(self):
        """Bad repo.  This should raise."""
        nonexistent_quay_repo = 'quay.io/--------:latest'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)

    def testBadQuayTag(self):
        """Bad tag.  This should raise."""
        nonexistent_quay_repo = 'quay.io/ucsc_cgl/toil:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_quay_repo)
