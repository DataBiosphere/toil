# Copyright (C) 2015-2024 Regents of the University of California
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

import io
import logging
import pytest
from typing import IO
import urllib.request
from urllib.error import URLError

from toil.lib.retry import retry
from toil.lib.integration import get_workflow_root_from_dockstore
from toil.test import ToilTest, needs_online

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

@pytest.mark.integrative
@needs_online
class DockstoreLookupTest(ToilTest):
    """
    Make sure we can look up workflows on Dockstore.
    """

    @retry(errors=[URLError, RuntimeError])
    def read_result(self, url_or_path: str) -> IO[bytes]:
        """
        Read a file or URL.

        Binary mode to allow testing for binary file support.

        This lets us test that we have the right workflow contents and not care
        how we are being shown them.
        """
        if url_or_path.startswith("http://") or url_or_path.startswith("https://"):
            response = urllib.request.urlopen(url_or_path)
            if response.status != 200:
                raise RuntimeError(f"HTTP error response: {response}")
            return response
        else:
            return open(url_or_path, "rb")

    # TODO: Tests that definitely test a clear cache

    def test_lookup_from_page_url(self) -> None:
        PAGE_URL = "https://dockstore.org/workflows/github.com/dockstore/bcc2020-training/HelloWorld:master?tab=info"
        # If we go in through the website we get an extra refs/heads/ on the branch name.
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore/bcc2020-training/master/wdl-training/exercise1/HelloWorld.wdl"
        looked_up = get_workflow_root_from_dockstore(PAGE_URL)

        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_lookup_from_trs(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        # Despite "-checker" in the ID, this actually refers to the base md5sum
        # workflow that just happens to have a checker *available*, not to the
        # checker workflow itself.
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/master/md5sum/md5sum-workflow.cwl"
        looked_up = get_workflow_root_from_dockstore(TRS_ID)
        
        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_lookup_from_trs_cached(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/master/md5sum/md5sum-workflow.cwl"
        # This lookup may or may not be cached
        get_workflow_root_from_dockstore(TRS_ID)
        # This lookup is definitely cached
        looked_up = get_workflow_root_from_dockstore(TRS_ID)
        
        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_lookup_from_trs_with_version(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker:workflowWithHTTPImport"
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/workflowWithHTTPImport/md5sum/md5sum-workflow.cwl"
        looked_up = get_workflow_root_from_dockstore(TRS_ID)
        
        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_lookup_from_trs_nonexistent_version(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker:notARealVersion"
        with self.assertRaises(RuntimeError):
            looked_up = get_workflow_root_from_dockstore(TRS_ID)


