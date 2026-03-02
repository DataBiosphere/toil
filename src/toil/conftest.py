# Copyright (C) 2015-2026 Regents of the University of California
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

import pytest
# TODO: Pytest doesn't expose the types we need to use to type this publicly
import _pytest.nodes
# TODO: Pytest also doesn't expose the class we need to sniff for.
import _pytest.doctest

# We want to impose a timeout on doctest test cases, because we had some hang
# for hours.
DOCTEST_TIMEOUT_SECONDS = 20

# Google Gemini put me up to the strategy of hooking in with
# pytest_collection_modifyitems(), sniffing each test case to see if it is a
# doctest, and if so adding a timeout to it. I haven't yet found a better
# approach.
def pytest_collection_modifyitems(items: list[_pytest.nodes.Item]) -> None:
    """
    Apply a timeout to all test items that extend _pytest.doctest.DoctestItem.
    """
    timeout_mark = pytest.mark.timeout(DOCTEST_TIMEOUT_SECONDS)
    for item in items:
        if isinstance(item, _pytest.doctest.DoctestItem):
            item.add_marker(timeout_mark)


