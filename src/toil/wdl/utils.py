# Copyright (C) 2020-2021 UCSC Computational Genomics Lab
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
from collections.abc import Iterable


def get_version(iterable: Iterable[str]) -> str:
    """
    Get the version of the WDL document.

    :param iterable: An iterable that contains the lines of a WDL document.
    :return: The WDL version used in the workflow.
    """
    if isinstance(iterable, str):
        iterable = iterable.split("\n")

    for line in iterable:
        line = line.strip()
        # check if the first non-empty, non-comment line is the version statement
        if line and not line.startswith("#"):
            if line.startswith("version "):
                return line[8:].strip()
            break
    # only draft-2 doesn't contain the version declaration
    return "draft-2"
