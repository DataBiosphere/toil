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
from typing import BinaryIO, Union

logger = logging.getLogger(__name__)


class ChecksumError(Exception):
    """Raised when a download does not contain the correct data."""


def compute_checksum_for_file(local_file_path: str, algorithm: str = 'sha1') -> str:
    with open(local_file_path, 'rb') as fh:
        checksum_result = compute_checksum_for_content(fh, algorithm=algorithm)
    return checksum_result


def compute_checksum_for_content(fh: Union[BinaryIO, BytesIO], algorithm: str = 'sha1') -> str:
    hash_object = getattr(hashlib, algorithm)()
    contents = fh.read(1024 * 1024)
    while contents != b'':
        hash_object.update(contents)
        contents = fh.read(1024 * 1024)

    return f'{algorithm}${hash_object.hexdigest()}'
