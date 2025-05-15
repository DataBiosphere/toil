# Copyright (C) 2015-2022 Regents of the University of California
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
import getpass
import logging

from toil.lib.misc import get_user_name
from toil.lib.url import URLAccess

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class TestURLAccess():
    """
    Test URLAccess class handling read, list,
    and checking the size/existence of resources at given URL
    """

    @needs_online
    def test_get_url_access(self):
        url = URLAccess()
        self.assertTrue(url.url_exists("https://httpstat.us/200"))

    @needs_aws_s3
    def test_get_size(self):
        url = URLAccess()
        size = url.get_size("s3://toil-datasets/hello.txt")
        self.assertIsInstance(size, int)
        self.assertGreater(size, 0)

    @needs_aws_s3
    def test_get_is_directory(self):
        url = URLAccess()
        self.assertFalse(url.get_is_directory("s3://toil-datasets/hello.txt"))

    @needs_aws_s3
    def test_list_url(self):
        url = URLAccess()
        test_dir = url.list_url("s3://1000genomes/")
        self.assertIsInstance(test_dir, list)
        self.assertGreater(len(test_dir), 0)

    @needs_aws_s3
    def test_read_from_url(self):
        import io
        url = URLAccess()
        output = io.BytesIO()
        size, executable = url.read_from_url("s3://toil-datasets/hello.txt", output)
        self.assertIsInstance(size, int)
        self.assertGreater(size, 0)
        self.assertFalse(executable)
        self.assertGreater(len(output.getvalue()), 0)

    @needs_aws_s3
    def test_open_url(self):
        url = URLAccess()
        with url.open_url("s3://toil-datasets/hello.txt") as readable:
            content = readable.read()
            self.assertIsInstance(content, bytes)
            self.assertGreater(len(content), 0)
