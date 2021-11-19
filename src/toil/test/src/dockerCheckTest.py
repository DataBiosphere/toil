# Copyright (C) 2015-2021 Regents of the University of California
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
import unittest

from docker.errors import ImageNotFound
from toil import checkDockerImageExists, parseDockerAppliance
from toil.test import ToilTest, needs_docker


@needs_docker
class DockerCheckTest(ToilTest):
    """Tests checking whether a docker image exists or not."""

    @unittest.skip('Consumes unauthenticated Docker Hub pulls if run')
    def testOfficialUbuntuRepo(self):
        """Image exists.  This should pass."""
        ubuntu_repo = 'ubuntu:latest'
        assert checkDockerImageExists(ubuntu_repo)

    @unittest.skip('Consumes unauthenticated Docker Hub pulls if run')
    def testBroadDockerRepo(self):
        """Image exists.  This should pass."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:2.0.0'
        assert checkDockerImageExists(broad_repo)

    @unittest.skip('Consumes unauthenticated Docker Hub pulls if run')
    def testBroadDockerRepoBadTag(self):
        """Bad tag.  This should raise."""
        broad_repo = 'broadinstitute/genomes-in-the-cloud:-----'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(broad_repo)

    @unittest.skip('Consumes unauthenticated Docker Hub pulls if run')
    def testNonexistentRepo(self):
        """Bad image.  This should raise."""
        nonexistent_repo = '------:-----'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_repo)

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

    def testGoogleRepo(self):
        """Image exists.  Should pass."""
        google_repo = 'gcr.io/google-containers/busybox:latest'
        assert checkDockerImageExists(google_repo)

    def testBadGoogleRepo(self):
        """Bad repo and tag.  This should raise."""
        nonexistent_google_repo = 'gcr.io/google-containers/--------:---'
        with self.assertRaises(ImageNotFound):
            checkDockerImageExists(nonexistent_google_repo)

    def testApplianceParser(self):
        """Test that a specified appliance is parsed correctly."""
        docker_list = ['ubuntu:latest',
                       'ubuntu',
                       'broadinstitute/genomes-in-the-cloud:2.0.0',
                       'quay.io/ucsc_cgl/toil:latest',
                       'gcr.io/google-containers/busybox:latest']
        parsings = []
        for image in docker_list:
            registryName, imageName, tag = parseDockerAppliance(image)
            parsings.append([registryName, imageName, tag])
        expected_parsings = [['docker.io', 'ubuntu', 'latest'],
                             ['docker.io', 'ubuntu', 'latest'],
                             ['docker.io', 'broadinstitute/genomes-in-the-cloud', '2.0.0'],
                             ['quay.io', 'ucsc_cgl/toil', 'latest'],
                             ['gcr.io', 'google-containers/busybox', 'latest']]
        assert parsings == expected_parsings
