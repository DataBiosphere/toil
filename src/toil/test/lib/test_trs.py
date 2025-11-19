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

import logging
import urllib.request
from typing import IO
from urllib.error import URLError

import pytest

from toil.lib.retry import retry
from toil.lib.trs import fetch_workflow, find_workflow
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
        trs_id, trs_version, language = find_workflow(PAGE_URL)

        self.assertEqual(
            trs_id, "#workflow/github.com/dockstore/bcc2020-training/HelloWorld"
        )
        self.assertEqual(trs_version, "master")
        self.assertEqual(language, "WDL")

    def test_lookup_from_trs_with_version(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        TRS_VERSION = "master"
        trs_id, trs_version, language = find_workflow(f"{TRS_ID}:{TRS_VERSION}")

        self.assertEqual(trs_id, TRS_ID)
        self.assertEqual(trs_version, TRS_VERSION)
        self.assertEqual(language, "CWL")

    def test_lookup_from_trs_no_version(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        with pytest.raises(ValueError):
            # We don't yet have a way to read Dockstore's default version info,
            # so it's not safe to apply any default version when multiple
            # versions exist.
            trs_id, trs_version, language = find_workflow(TRS_ID)

    # TODO: Add a test with a workflow that we know has and will only ever
    # have one version, to test version auto-detection in that case.

    def test_get(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        TRS_VERSION = "master"
        LANGUAGE = "CWL"
        # Despite "-checker" in the ID, this actually refers to the base md5sum
        # workflow that just happens to have a checker *available*, not to the
        # checker workflow itself.
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/master/md5sum/md5sum-workflow.cwl"
        looked_up = fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)

        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_get_from_trs_cached(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        TRS_VERSION = "master"
        LANGUAGE = "CWL"
        WORKFLOW_URL = "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/master/md5sum/md5sum-workflow.cwl"
        # This lookup may or may not be cached
        fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)
        # This lookup is definitely cached
        looked_up = fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)

        data_from_lookup = self.read_result(looked_up).read()
        data_from_source = self.read_result(WORKFLOW_URL).read()
        self.assertEqual(data_from_lookup, data_from_source)

    def test_lookup_from_trs_with_version(self) -> None:
        TRS_VERSIONED_ID = "#workflow/github.com/dockstore-testing/md5sum-checker:workflowWithHTTPImport"
        trs_id, trs_version, language = find_workflow(TRS_VERSIONED_ID)

        parts = TRS_VERSIONED_ID.split(":")

        self.assertEqual(trs_id, parts[0])
        self.assertEqual(trs_version, parts[1])
        self.assertEqual(language, "CWL")

    def test_lookup_from_trs_nonexistent_workflow(self) -> None:
        TRS_VERSIONED_ID = "#workflow/github.com/adamnovak/veryfakerepo:notARealVersion"
        with self.assertRaises(FileNotFoundError):
            looked_up = find_workflow(TRS_VERSIONED_ID)

    def test_lookup_from_trs_nonexistent_workflow_bad_format(self) -> None:
        TRS_VERSIONED_ID = "#workflow/AbsoluteGarbage:notARealVersion"
        with self.assertRaises(FileNotFoundError):
            looked_up = find_workflow(TRS_VERSIONED_ID)

    def test_lookup_from_trs_nonexistent_version(self) -> None:
        TRS_VERSIONED_ID = (
            "#workflow/github.com/dockstore-testing/md5sum-checker:notARealVersion"
        )
        with self.assertRaises(FileNotFoundError):
            looked_up = find_workflow(TRS_VERSIONED_ID)

    def test_get_nonexistent_workflow(self) -> None:
        TRS_ID = "#workflow/github.com/adamnovak/veryfakerepo"
        TRS_VERSION = "notARealVersion"
        LANGUAGE = "CWL"
        with self.assertRaises(FileNotFoundError):
            looked_up = fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)

    def test_get_nonexistent_version(self) -> None:
        TRS_ID = "#workflow/github.com/dockstore-testing/md5sum-checker"
        TRS_VERSION = "notARealVersion"
        LANGUAGE = "CWL"
        with self.assertRaises(FileNotFoundError):
            looked_up = fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)

    def test_get_nonexistent_workflow_bad_format(self) -> None:
        # Dockstore enforces an ID pattern and blames your request if you ask
        # about something that doesn't follow it. So don't follow it.
        TRS_ID = "#workflow/AbsoluteGarbage"
        TRS_VERSION = "notARealVersion"
        LANGUAGE = "CWL"
        with self.assertRaises(FileNotFoundError):
            looked_up = fetch_workflow(TRS_ID, TRS_VERSION, LANGUAGE)
