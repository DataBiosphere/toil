# Copyright (C) 2015-2025 Regents of the University of California
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
from typing import IO, Optional, Union

from configargparse import ArgParser, ArgumentParser

from toil.batchSystems.abstractBatchSystem import (
    AbstractBatchSystem,
    UpdatedBatchJobInfo,
)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.batchSystems.options import OptionSetter
from toil.batchSystems.registry import add_batch_system_factory
from toil.common import Toil, addOptions
from toil.job import JobDescription
from toil.test import ToilTest
from toil.lib.plugins import remove_plugin


import io
from urllib.parse import ParseResult
from toil.test import ToilTest
from toil.lib.url import URLAccess
from toil.lib.plugins import register_plugin, remove_plugin

logger = logging.getLogger(__name__)

class FakeURLPlugin(URLAccess):
    @classmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        return url.scheme == "fake"

    @classmethod
    def _url_exists(cls, url: ParseResult) -> bool:
        return url.netloc == "exists"

    @classmethod
    def _get_size(cls, url: ParseResult) -> int:
        return 1234

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        return url.path.endswith("/")

    @classmethod
    def _list_url(cls, url: ParseResult) -> list[str]:
        return ["file1.txt", "subdir/"]

    @classmethod
    def _read_from_url(cls, url: ParseResult, writable: IO[bytes]) -> tuple[int, bool]:
        content = b"hello world"
        writable.write(content)
        return len(content), False

    @classmethod
    def _open_url(cls, url: ParseResult) -> IO[bytes]:
        return io.BytesIO(b"hello world")

    @classmethod
    def _write_to_url(cls, readable: Union[IO[bytes], IO[str]], url: ParseResult, executable: bool = False) -> None:
        pass


class TestURLAccess(ToilTest):
    def setUp(self) -> None:
        super().setUp()
        register_plugin("url_access", "fake", lambda: FakeURLPlugin)

    def tearDown(self) -> None:
        remove_plugin("url_access", "fake")
        super().tearDown()

    def test_url_exists(self) -> None:
        assert URLAccess.url_exists("fake://exists/resource") == True
        assert URLAccess.url_exists("fake://missing/resource") == False

    def test_get_size(self) -> None:
        assert URLAccess.get_size("fake://any/resource") == 1234

    def test_get_is_directory(self) -> None:
        assert URLAccess.get_is_directory("fake://any/folder/") == True
        assert URLAccess.get_is_directory("fake://any/file.txt") == False

    def test_list_url(self) -> None:
        assert URLAccess.list_url("fake://any/folder/") == ["file1.txt", "subdir/"]

    def test_read_from_url(self) -> None:
        output = io.BytesIO()
        size, _ = URLAccess.read_from_url("fake://any/resource", output)
        assert output.getvalue() == b"hello world"
        assert size == len("hello world")

    def test_open_url(self) -> None:
        with URLAccess.open_url("fake://any/resource") as stream:
            content = stream.read()
            assert content == b"hello world"
