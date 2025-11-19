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
import logging

from pytest_httpserver import HTTPServer

from toil.lib.url import URLAccess
from toil.test import needs_aws_s3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class TestURLAccess:
    """
    Test URLAccess class handling read, list,
    and checking the size/existence of resources at given URL
    """

    def test_get_url_access(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/some_url").respond_with_data("Yep that's a URL")
        file_url = httpserver.url_for("/some_url")
        assert URLAccess.url_exists(file_url)

    @needs_aws_s3
    def test_get_size(self) -> None:
        size = URLAccess.get_size("s3://toil-datasets/hello.txt")
        assert isinstance(size, int)
        assert size > 0

    @needs_aws_s3
    def test_get_is_directory(self) -> None:
        assert not URLAccess.get_is_directory("s3://toil-datasets/hello.txt")

    @needs_aws_s3
    def test_list_url(self) -> None:
        test_dir = URLAccess.list_url("s3://1000genomes/")
        assert isinstance(test_dir, list)
        assert len(test_dir) > 0

    @needs_aws_s3
    def test_read_from_url(self) -> None:
        import io

        output = io.BytesIO()
        size, executable = URLAccess.read_from_url(
            "s3://toil-datasets/hello.txt", output
        )
        assert isinstance(size, int)
        assert size > 0
        assert not executable
        assert len(output.getvalue()) > 0

    @needs_aws_s3
    def test_open_url(self) -> None:
        with URLAccess.open_url("s3://toil-datasets/hello.txt") as readable:
            content = readable.read()
            assert isinstance(content, bytes)
            assert len(content) > 0
