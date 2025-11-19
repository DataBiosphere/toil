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
import hashlib
import logging
from io import BytesIO
from typing import TYPE_CHECKING, BinaryIO

from toil.lib.aws.config import S3_PART_SIZE

if TYPE_CHECKING:
    # mypy complaint: https://github.com/python/typeshed/issues/2928
    from hashlib import _Hash


logger = logging.getLogger(__name__)


class ChecksumError(Exception):
    """Raised when a download does not contain the correct data."""


class Etag:
    """A hasher for s3 etags."""

    def __init__(self, chunk_size: int) -> None:
        self.etag_bytes: int = 0
        self.etag_parts: list[bytes] = []
        self.etag_hasher: "_Hash" = hashlib.md5()
        self.chunk_size: int = chunk_size

    def update(self, chunk: bytes) -> None:
        if self.etag_bytes + len(chunk) > self.chunk_size:
            chunk_head = chunk[: self.chunk_size - self.etag_bytes]
            chunk_tail = chunk[self.chunk_size - self.etag_bytes :]
            self.etag_hasher.update(chunk_head)
            self.etag_parts.append(self.etag_hasher.digest())
            self.etag_hasher = hashlib.md5()
            self.etag_hasher.update(chunk_tail)
            self.etag_bytes = len(chunk_tail)
        else:
            self.etag_hasher.update(chunk)
            self.etag_bytes += len(chunk)

    def hexdigest(self) -> str:
        if self.etag_bytes:
            self.etag_parts.append(self.etag_hasher.digest())
            self.etag_bytes = 0
        if len(self.etag_parts) > 1:
            etag = hashlib.md5(b"".join(self.etag_parts)).hexdigest()
            return f"{etag}-{len(self.etag_parts)}"
        else:
            return self.etag_hasher.hexdigest()


hashers = {
    "sha1": hashlib.sha1(),
    "sha256": hashlib.sha256(),
    "etag": Etag(chunk_size=S3_PART_SIZE),
}


def compute_checksum_for_file(local_file_path: str, algorithm: str = "sha1") -> str:
    with open(local_file_path, "rb") as fh:
        checksum_result = compute_checksum_for_content(fh, algorithm=algorithm)
    return checksum_result


def compute_checksum_for_content(
    fh: BinaryIO | BytesIO, algorithm: str = "sha1"
) -> str:
    """
    Note: Chunk size matters for s3 etags, and must be the same to get the same hash from the same object.
    Therefore this buffer is not modifiable throughout Toil.
    """
    hasher: "_Hash" = hashers[algorithm]  # type: ignore
    contents = fh.read(S3_PART_SIZE)
    while contents != b"":
        hasher.update(contents)
        contents = fh.read(S3_PART_SIZE)

    return f"{algorithm}${hasher.hexdigest()}"
