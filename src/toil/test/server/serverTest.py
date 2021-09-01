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
import json
import logging
import os
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flask import Flask

from toil.server.api.utils import DefaultOptions
from toil.server.app import create_app, parser_with_server_options
from toil.test import ToilTest


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilServerUtilsTest(ToilTest):
    """
    Tests for the utility functions / classes used by the Toil server.
    """

    def test_default_options(self):
        """ Tests the DefaultOptions object and its methods."""
        options = DefaultOptions([
            "--logLevel=CRITICAL",
            "--workDir=/path/to/directory",
            "--tag=Name=default",
            "--tag=Owner=shared",
        ])
        print(options.pairs)

        self.assertEqual(options.get_option("--logLevel"), "CRITICAL")
        self.assertEqual(options.get_option("--logLevel", "INFO"), "CRITICAL")
        self.assertEqual(options.get_option("jobStore"), None)
        self.assertEqual(options.get_option("jobStore", "default"), "default")

        # when there are multiple options, get_option() returns the first value
        self.assertEqual(options.get_option("--tag"), "Name=default")

        # get_options() should return every option that matches the key
        self.assertEqual(options.get_options("--tag"), ["Name=default", "Owner=shared"])


class ToilWESServerTest(ToilTest):
    """
    Tests for Toil's Workflow Execution Service API support.
    """

    def setUp(self) -> None:
        super(ToilWESServerTest, self).setUp()
        self.temp_dir = self._createTempDir()
        # self.temp_dir = "/Users/wlgao/Desktop"

        parser = parser_with_server_options()
        args = parser.parse_args(["--work_dir", os.path.join(self.temp_dir, "workflows")])

        self.app: "Flask" = create_app(args).app
        self.app.testing = True

    def tearDown(self):
        pass

    def test_get_service_info(self) -> None:
        """ Test the GET /service-info endpoint."""
        with self.app.test_client() as client:
            rv = client.get("/ga4gh/wes/v1/service-info")
        self.assertEqual(rv.status_code, 200)
        service_info = json.loads(rv.data)

        self.assertIn("version", service_info)
        self.assertIn("workflow_type_versions", service_info)
        self.assertIn("supported_wes_versions", service_info)
        self.assertIn("supported_filesystem_protocols", service_info)
        self.assertIn("workflow_engine_versions", service_info)
        self.assertIn("default_workflow_engine_parameters", service_info)
        self.assertIn("system_state_counts", service_info)
        self.assertIn("tags", service_info)

    def test_run_workflow(self) -> None:
        """ Test submitting a workflow to the WES server."""

        # relative workflow URL but no attachments
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "example.cwl",
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": "{}"
            })
            self.assertEqual(rv.status_code, 400)
            self.assertTrue(rv.is_json)
            self.assertEqual(rv.json.get("msg"), "Relative 'workflow_url' but missing 'workflow_attachment'")

        #
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "example.cwl",
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": json.dumps({"message": "Hello, world!"}),
                "workflow_attachment": [
                    # (open("/Users/wlgao/code/GI/toil_server/src/example.cwl", "rb"), "example.cwl"),
                ],
            })
            self.assertEqual(rv.status_code, 200)
            self.assertTrue(rv.is_json)
            run_id = rv.json.get("run_id")
            self.assertIsNotNone(run_id)


if __name__ == "__main__":
    unittest.main()
